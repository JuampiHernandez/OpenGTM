"""
Living workbooks (Pillar 3).

A workbook with a `refresh_policy` becomes a standing subscription to a slice of
the market. Recurrence is built on the existing DB-polling queue_service: the
`refresh_workbook` handler does its work then re-enqueues itself at
`next_run_at = now + interval`. No new scheduler infra (APScheduler/Celery).

Refresh does two things:
  1. Re-run every `source` column → append only genuinely-new entities (the
     entity graph dedups, so refresh never duplicates).
  2. Re-enrich only STALE fields (per `staleness_ttl_days`), row-scoped.

Signals become source triggers: `signal_scan` runs the existing monitor, applies
score boosts, and refreshes workbooks whose policy lists the fired signal type.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from apps.api.database import SessionLocal
from apps.api.services.workbook.models import Workbook, WorkbookRow, WorkbookEnrichment
from apps.api.services.workbook.activity_models import WorkbookActivity

logger = logging.getLogger("workbook.refresh")

DEFAULT_STALENESS_DAYS = 30
INTERVAL_MINUTES = {"hourly": 60, "daily": 1440, "weekly": 10080}


def _now():
    return datetime.now(timezone.utc)


def log_activity(db, workbook_id: str, kind: str, message: str):
    # Denormalized tenant from the active workspace scope (callers run inside
    # workspace_scope) so the row matches the RLS GUC / WITH CHECK.
    from apps.api.core.tenancy import current_workspace_var
    db.add(WorkbookActivity(
        workbook_id=workbook_id, workspace_id=current_workspace_var.get(),
        kind=kind, message=message,
    ))


def _interval_minutes(policy: dict) -> Optional[int]:
    interval = (policy or {}).get("interval")
    if not interval:
        return None
    if isinstance(interval, (int, float)):
        return int(interval)
    return INTERVAL_MINUTES.get(str(interval).lower())


def _stale_row_columns(db, workbook_id: str, enrichment_cols: List[dict], ttl_map: dict,
                       batch_size: Optional[int] = None) -> dict[str, list[str]]:
    """Exact enrichment cells missing, failed or older than their field's TTL.

    Selected in SQL, keyset-paginated over ``workbook_rows.id`` so a refresh
    never loads the whole workbook (or its overlays) into Python. Each page
    cross-joins the page's rows with the enrichment columns and LEFT JOINs the
    overlay on the execution key (linked lead id, else row id); only stale
    (row, column) pairs come back. A cell is stale when its overlay is missing,
    its status is anything but ``complete`` (including NULL), its
    ``updated_at`` is NULL, or ``updated_at`` is older than
    ``now - staleness_ttl_days[target_field or id]`` (default 30 days).

    Every page is scanned, so the result is independent of ``batch_size``.
    Rows are returned in id order with columns in config order. Tenancy: the
    explicit ``workbook_id`` filter plus RLS on both tables, as before.
    """
    from sqlalchemy import DateTime, Integer, String, and_, func, literal, or_, select, true, union_all

    from apps.api.core.config import settings

    # Column ids are NOT NULL on overlays; de-dup preserves config order (the
    # caller de-dups the per-row lists anyway).
    cutoffs: dict[str, datetime] = {}
    now = _now()
    for col in enrichment_cols:
        cid = col.get("id")
        if not cid or cid in cutoffs:
            continue
        ttl_days = ttl_map.get(col.get("target_field") or cid, DEFAULT_STALENESS_DAYS)
        # Overlay timestamps are naive UTC (DateTime without tz on both
        # dialects); compare naive UTC to avoid session-timezone coercion on
        # PostgreSQL and offset-suffixed strings on SQLite.
        cutoffs[cid] = (now - timedelta(days=ttl_days)).astimezone(timezone.utc).replace(tzinfo=None)
    if not cutoffs:
        return {}

    columns = union_all(*(
        select(literal(cid, String).label("cid"),
               literal(ordinal, Integer).label("ord"),
               literal(cutoff, DateTime()).label("cutoff"))
        for ordinal, (cid, cutoff) in enumerate(cutoffs.items())
    )).subquery("refresh_cols")

    size = max(1, int(batch_size or settings.WORKBOOK_REFRESH_SCAN_BATCH_SIZE))
    R, E = WorkbookRow, WorkbookEnrichment
    stale: dict[str, list[str]] = {}
    after = None
    while True:
        # Keyset page bounds: the next `size` row ids after the previous page.
        page_q = select(R.id).where(R.workbook_id == workbook_id)
        if after is not None:
            page_q = page_q.where(R.id > after)
        page = page_q.order_by(R.id).limit(size).subquery("refresh_page")
        hi, count = db.execute(select(func.max(page.c.id), func.count())).one()
        if not count:
            break

        in_page = [R.workbook_id == workbook_id, R.id <= hi]
        if after is not None:
            in_page.append(R.id > after)
        overlay_on = and_(
            E.workbook_id == workbook_id,
            E.lead_id == func.coalesce(R.lead_id, R.id),
            E.column_id == columns.c.cid,
        )
        rows = db.execute(
            select(R.id, columns.c.cid)
            .select_from(R)
            .join(columns, true())
            .outerjoin(E, overlay_on)
            .where(*in_page)
            .where(or_(
                E.id.is_(None),
                E.status.is_(None),
                E.status != "complete",
                E.updated_at.is_(None),
                E.updated_at < columns.c.cutoff,
            ))
            .order_by(R.id, columns.c.ord)
        ).all()
        for row_id, cid in rows:
            stale.setdefault(str(row_id), []).append(cid)

        if count < size:
            break
        after = hi
    return stale


async def refresh_workbook(
    workbook_id: str, reason: str = "scheduled", workspace_id: str = None
) -> dict:
    """One refresh cycle: source new rows + re-enrich stale fields.

    ``workspace_id`` is the tenant this refresh belongs to; it arrives out-of-band
    (job payload / caller), never read off the row. Enter ``workspace_scope``
    FIRST (fail-loud on empty) so the wb/row reads and the chained source/enrich
    work are tenant-scoped for RLS.
    """
    from apps.api.core.tenancy import workspace_scope

    with workspace_scope(workspace_id):
        return await _refresh_workbook_impl(workbook_id, reason, workspace_id)


async def _refresh_workbook_impl(workbook_id: str, reason: str, workspace_id: str) -> dict:
    from apps.api.services.workbook.source_engine import materialize_source
    from apps.api.services.workbook.enrichment import ENRICHMENT_COL_TYPES, run_workbook_enrichment
    from apps.api.services.workbook.column_deps import downstream_columns

    with SessionLocal() as db:
        wb = db.query(Workbook).filter(Workbook.id == workbook_id).first()
        if not wb:
            return {"error": "workbook_not_found"}
        cols = wb.columns_config or []
        policy = wb.refresh_policy or {}
        source_cols = [c for c in cols if c.get("type") == "source"]
        enrichment_cols = [c for c in cols if c.get("type") in ("enrichment", "waterfall", "ai_formula")]
        ttl_map = policy.get("staleness_ttl_days") or {}

    # 1) Re-source (append-only via entity dedup). Track exactly the rows our
    # own sourcing inserted: rows added concurrently by other writers (CSV
    # import, API insert, another job) must not get every column run here.
    sourced = 0
    added_row_ids: list = []
    for col in source_cols:
        res = await materialize_source(workbook_id, col["id"], workspace_id)
        sourced += res.get("added", 0)
        added_row_ids.extend(res.get("added_row_ids") or [])

    # 2) Refresh stale cells and their reactive downstream dependencies.
    def is_reactive(column: dict) -> bool:
        return (
            column.get("type") in ENRICHMENT_COL_TYPES
            and column.get("reactive", column.get("type") != "output")
        )

    # Sourcing initializes every runnable cell, without dispatching outputs.
    new_row_columns = [c["id"] for c in cols
                       if c.get("id") and c.get("type") in ENRICHMENT_COL_TYPES
                       and c.get("type") != "output"]
    reenriched = 0
    with SessionLocal() as db:
        stale = _stale_row_columns(db, workbook_id, enrichment_cols, ttl_map) if enrichment_cols else {}
    for row_id, column_ids in stale.items():
        dependents = downstream_columns(cols, set(column_ids), eligible=is_reactive)
        stale[row_id] = list(dict.fromkeys([*column_ids, *(c["id"] for c in dependents if c.get("id"))]))
    if new_row_columns:
        for row_id in dict.fromkeys(added_row_ids):
            stale[str(row_id)] = list(new_row_columns)
    if stale:
        result = await run_workbook_enrichment(
            workbook_id, row_ids=[int(row_id) for row_id in stale],
            column_ids=list(dict.fromkeys(cid for ids in stale.values() for cid in ids)),
            row_columns=stale, workspace_id=workspace_id,
        )
        reenriched = result.get("completed", 0)

    with SessionLocal() as db:
        log_activity(db, workbook_id, "refresh",
                     f"{reason}: +{sourced} new rows, re-enriched {reenriched} stale cells")
        db.commit()

    logger.info(f"Refresh {workbook_id} ({reason}): +{sourced} rows, {reenriched} re-enriched")
    return {"sourced": sourced, "reenriched": reenriched, "stale_rows": len(stale)}


def _enqueue_next(db, workbook_id: str, minutes: int, workspace_id: str):
    """Self-re-enqueue the next refresh via queue_service.next_run_at.

    Stamp ``workspace_id`` into the payload (OD-4) so the handler that picks this
    job up can enter the correct tenant scope.
    """
    from apps.api.models import Job
    job = Job(
        type="refresh_workbook",
        payload={"workbook_id": workbook_id, "workspace_id": workspace_id},
        status="pending",
        priority=1,
        next_run_at=_now() + timedelta(minutes=minutes),
        max_retries=3,
    )
    db.add(job)
    db.commit()


async def handle_refresh_workbook(job_id: int, payload: dict):
    """queue_service handler — runs a refresh, then schedules the next if enabled.

    Worker tenant signal comes from the payload (OD-4): enter ``workspace_scope``
    first and fail loud if ``workspace_id`` is absent.
    """
    from apps.api.core.tenancy import workspace_scope

    workbook_id = payload["workbook_id"]
    workspace_id = payload.get("workspace_id")
    with workspace_scope(workspace_id):
        await refresh_workbook(
            workbook_id, reason=payload.get("reason", "scheduled"), workspace_id=workspace_id
        )
        with SessionLocal() as db:
            wb = db.query(Workbook).filter(Workbook.id == workbook_id).first()
            if not wb:
                return
            policy = wb.refresh_policy or {}
            minutes = _interval_minutes(policy)
            if policy.get("enabled") and minutes:
                _enqueue_next(db, workbook_id, minutes, workspace_id)
                logger.info(f"[job {job_id}] next refresh for {workbook_id} in {minutes}m")


def set_refresh_policy(db, workbook_id: str, policy: dict) -> dict:
    """Persist a refresh policy and (if enabled+interval) kick off the recurring chain."""
    wb = db.query(Workbook).filter(Workbook.id == workbook_id).first()
    if not wb:
        return {"error": "workbook_not_found"}
    wb.refresh_policy = policy
    db.commit()
    minutes = _interval_minutes(policy)
    if policy.get("enabled") and minutes:
        # Called from request/copilotkit paths with the workbook loaded; the
        # workbook's own workspace is the tenant for the recurring chain.
        from apps.api.models import Job
        active = db.query(Job.id).filter(
            Job.type == "refresh_workbook",
            Job.status.in_(("pending", "processing")),
            Job.payload["workbook_id"].as_string() == workbook_id,
        ).first()
        # Pending and claimed refreshes already continue the enabled policy.
        # Saving it again must not fork another self-reenqueuing chain.
        if active is None:
            _enqueue_next(db, workbook_id, minutes, wb.workspace_id)
    return {"refresh_policy": policy, "next_in_minutes": minutes if policy.get("enabled") else None}


# ── Signals → score → action ─────────────────────────────────────────────

async def handle_signal_scan(job_id: int, payload: dict):
    """Run the signal monitor, then trigger workbooks subscribed to fired signals.

    The signal_scan job itself is GLOBAL (it touches only the non-RLS ``jobs``
    table and runs the per-workspace ``run_signal_scan`` from #89). The workbook
    trigger pass must NOT enumerate workbooks globally (``Workbook.all()`` is
    structurally incompatible with per-tenant RLS — it returns zero rows once
    FORCE RLS lands). Instead enumerate workspaces and, per workspace, scope to
    that tenant and trigger only its workbooks (mirrors run_signal_scan's
    per-workspace loop). One workspace failing must not abort the rest.
    """
    from apps.api.core.tenancy import workspace_scope
    from apps.api.services.workspace import manager as ws_manager

    try:
        from apps.api.services.signals.monitor import run_signal_scan
        scan = await run_signal_scan()
    except Exception as e:
        logger.warning(f"signal scan failed: {e}")
        scan = {}

    fired_types = payload.get("signal_types") or ["hiring", "funding", "tech_change", "news"]
    triggered = 0
    try:
        workspaces = ws_manager.list_workspaces()
    except Exception as e:
        logger.warning(f"signal_scan: could not list workspaces: {e}")
        workspaces = []

    for ws in workspaces:
        try:
            with workspace_scope(ws.id):
                with SessionLocal() as db:
                    # Belt (explicit workspace filter) + suspenders (RLS once on).
                    # With RLS off this filter is what keeps the per-workspace
                    # loop from re-triggering every tenant's workbooks N times.
                    wbs = (
                        db.query(Workbook)
                        .filter(Workbook.workspace_id == ws.id)
                        .all()
                    )
                    for wb in wbs:
                        policy = wb.refresh_policy or {}
                        on_signal = set(policy.get("on_signal") or [])
                        if policy.get("enabled") and on_signal & set(fired_types):
                            _enqueue_next_now(db, wb.id, reason="signal", workspace_id=ws.id)
                            log_activity(
                                db, wb.id, "signal",
                                f"signal trigger → refresh ({', '.join(on_signal & set(fired_types))})",
                            )
                            triggered += 1
                    db.commit()
        except Exception as e:
            logger.warning(f"signal_scan: workspace {ws.id} trigger pass failed: {e}")

    # Self-re-enqueue the next periodic scan (default daily).
    interval_min = int(payload.get("interval_minutes", INTERVAL_MINUTES["daily"]))
    with SessionLocal() as db:
        from apps.api.services.job_scheduling import enqueue_job_once

        next_run_at = _now() + timedelta(minutes=interval_min)
        fire_key = f"signal_scan:{next_run_at.replace(microsecond=0).isoformat()}"
        enqueue_job_once(
            db,
            job_type="signal_scan",
            payload={"interval_minutes": interval_min},
            fire_key=fire_key,
            next_run_at=next_run_at,
        )
        db.commit()
    logger.info(f"[job {job_id}] signal_scan: {scan}, triggered {triggered} workbooks, next in {interval_min}m")


def bootstrap_signal_scan(interval_minutes: int = None):
    """Enqueue the recurring signal scan once, if no scan job is already pending."""
    interval_minutes = interval_minutes or INTERVAL_MINUTES["daily"]
    with SessionLocal() as db:
        from apps.api.models import Job
        from apps.api.services.job_scheduling import enqueue_job_once
        pending = db.query(Job).filter(
            Job.type == "signal_scan", Job.status.in_(["pending", "processing"])
        ).count()
        if pending:
            return False
        next_run_at = _now() + timedelta(minutes=interval_minutes)
        fire_key = f"signal_scan:{next_run_at.replace(microsecond=0).isoformat()}"
        job = enqueue_job_once(
            db,
            job_type="signal_scan",
            payload={"interval_minutes": interval_minutes},
            fire_key=fire_key,
            next_run_at=next_run_at,
        )
        if job is None:
            return False
        db.commit()
    logger.info(f"bootstrapped recurring signal_scan (every {interval_minutes}m)")
    return True


def _enqueue_next_now(db, workbook_id: str, reason: str, workspace_id: str):
    from apps.api.models import Job
    db.add(Job(
        type="refresh_workbook",
        payload={"workbook_id": workbook_id, "reason": reason, "workspace_id": workspace_id},
        status="pending", priority=2, next_run_at=_now(), max_retries=3,
    ))
