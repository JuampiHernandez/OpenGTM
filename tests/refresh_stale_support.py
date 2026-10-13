"""Shared fixtures for the SQL stale-cell selection parity tests (SQLite + PG).

``legacy_stale_row_columns`` is the pre-#33 Python implementation, kept
verbatim as the oracle: the SQL selection must return exactly what it did.
"""

import random
from datetime import timedelta, timezone

from apps.api.services.workbook.models import Workbook, WorkbookEnrichment, WorkbookRow
from apps.api.services.workbook.refresh import DEFAULT_STALENESS_DAYS

COLUMNS = [
    {"id": "email", "type": "input"},
    {"id": "summary", "type": "ai_formula", "prompt": "s {email}"},       # ttl 7
    {"id": "pitch", "type": "ai_formula", "prompt": "p {email}"},         # ttl 30
    {"id": "domain_col", "type": "enrichment", "target_field": "domain"},  # ttl via target_field: 2
    {"id": "no_ttl", "type": "waterfall"},                                 # default 30
    {"id": "quick", "type": "ai_formula", "prompt": "q {email}"},         # ttl 0
]
# "domain_col" also has its own entry: target_field must take precedence.
TTL_MAP = {"summary": 7, "pitch": 30, "domain": 2, "quick": 0, "domain_col": 999}
ENRICHMENT_COLS = [c for c in COLUMNS if c["type"] in ("enrichment", "waterfall", "ai_formula")]


def legacy_stale_row_columns(db, workbook_id, enrichment_cols, ttl_map, now):
    rows = db.query(WorkbookRow).filter(WorkbookRow.workbook_id == workbook_id).all()
    if not rows:
        return {}
    overlays = db.query(WorkbookEnrichment).filter(
        WorkbookEnrichment.workbook_id == workbook_id
    ).all()
    seen = {(o.lead_id, o.column_id): o for o in overlays}
    stale = {}
    for row in rows:
        row_id = row.id
        lead_id = row.lead_id or row_id
        for col in enrichment_cols:
            cid = col.get("id")
            ttl_days = ttl_map.get(col.get("target_field") or cid, DEFAULT_STALENESS_DAYS)
            o = seen.get((lead_id, cid))
            if o is None or o.status != "complete":
                stale.setdefault(str(row_id), []).append(cid)
                continue
            upd = o.updated_at
            if upd is None:
                stale.setdefault(str(row_id), []).append(cid)
                continue
            if upd.tzinfo is None:
                upd = upd.replace(tzinfo=timezone.utc)
            if upd < now - timedelta(days=ttl_days):
                stale.setdefault(str(row_id), []).append(cid)
    return stale


def _age(now, days, hours=0):
    return (now - timedelta(days=days, hours=hours)).astimezone(timezone.utc).replace(tzinfo=None)


STATES = ["fresh", "stale", "missing", "error", "pending", "null_status",
          "null_updated", "edge_fresh", "edge_stale"]


def populate_random(db, ws, now, *, n_rows=40, seed=33, name="Stale SQL parity"):
    """A workbook with a randomized mix of fresh/stale/missing/failed cells.

    Also writes a sibling workbook whose complete, fresh overlays share the
    same execution keys; they must never leak into the first workbook's
    selection. Returns (workbook_id, states_seen).
    """
    rng = random.Random(seed)
    wb = Workbook(name=name, workspace_id=ws, columns_config=COLUMNS,
                  refresh_policy={"staleness_ttl_days": TTL_MAP})
    other = Workbook(name=f"{name} sibling", workspace_id=ws, columns_config=COLUMNS)
    db.add_all([wb, other])
    db.flush()
    seen_states = set()
    for i in range(n_rows):
        row = WorkbookRow(workbook_id=wb.id, workspace_id=ws, position=i,
                          lead_id=(100000 + i) if rng.random() < 0.5 else None,
                          data={"email": f"r{i}@example.test"}, enrichments={})
        db.add(row)
        db.flush()
        key = row.lead_id or row.id
        for col in ENRICHMENT_COLS:
            db.add(WorkbookEnrichment(workbook_id=other.id, workspace_id=ws, lead_id=key,
                                      column_id=col["id"], status="complete",
                                      updated_at=_age(now, 0)))
            ttl = TTL_MAP.get(col.get("target_field") or col["id"], DEFAULT_STALENESS_DAYS)
            state = rng.choice(STATES)
            seen_states.add(state)
            if state == "missing":
                continue
            status, upd = "complete", _age(now, 0)
            if state == "stale":
                upd = _age(now, ttl + 5)
            elif state == "error":
                status = "error"
            elif state == "pending":
                status = "pending"
            elif state == "edge_fresh":
                upd = _age(now, ttl, hours=-1)
            elif state == "edge_stale":
                upd = _age(now, ttl, hours=1)
            cell = WorkbookEnrichment(workbook_id=wb.id, workspace_id=ws, lead_id=key,
                                      column_id=col["id"], status=status, updated_at=upd)
            db.add(cell)
            db.flush()
            if state in ("null_status", "null_updated"):
                # Bypass ORM/server defaults to persist genuine NULLs.
                db.query(WorkbookEnrichment).filter(WorkbookEnrichment.id == cell.id).update(
                    {"status": None} if state == "null_status" else {"updated_at": None},
                    synchronize_session=False)
    db.commit()
    return wb.id, seen_states
