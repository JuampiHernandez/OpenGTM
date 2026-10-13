#!/usr/bin/env python3
"""Enrichment completion harness (M0 baseline, issue #33).

Measures how fast the *current* Python stack completes workbook enrichments
end to end, against the deterministic provider simulator:

1. starts ``provider_simulator.py`` on an ephemeral loopback port;
2. writes a declarative provider manifest pointing at it (the real
   ``DeclarativeProvider`` compile/register path, run in the killable pebble
   provider pool);
3. creates N workbook rows with one ``enrichment`` column using that provider;
4. enqueues ``run_workbook`` jobs on the durable SQL queue and runs a real
   ``QueueService`` worker (claim -> job child process -> provider workers ->
   result commit);
5. waits for every job to finish and reports committed completions/sec,
   per-cell latency percentiles, errors, provider attempts/429s, CPU/RSS, and
   the identity block (commit, hardware, Python, DB, worker config, workload).

    uv run python scripts/bench/enrichment_harness.py --rows 500 --latency-ms 50 \
        --output artifacts/bench/enrichment-500.json

SQLite in a temporary directory is the default. Set ``--database-url`` (or
``BENCH_DATABASE_URL``) to a *disposable*, migrated PostgreSQL database to
measure Postgres; the harness only claims and deletes rows it created.

Benchmark-only tooling. The SSRF guard is exempted for exactly the simulator's
loopback origin inside benchmark processes (see ``_shim/sitecustomize.py``).
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import json
import math
import os
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from collections import Counter
from datetime import UTC, datetime, timezone
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parents[1]
SHIM_DIR = BENCH_DIR / "_shim"
SIMULATOR = BENCH_DIR / "provider_simulator.py"
PROVIDER_NAME = "bench_simulator"
COLUMN_ID = "email"
SCHEMA_VERSION = 1


# ── small helpers ─────────────────────────────────────────────────────────

def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile (same definition as the queue load gate)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(pct * len(ordered)) - 1))
    return ordered[index]


def _ms_summary(seconds: list[float]) -> dict:
    return {
        "count": len(seconds),
        "p50": round(percentile(seconds, 0.50) * 1000, 1),
        "p95": round(percentile(seconds, 0.95) * 1000, 1),
        "p99": round(percentile(seconds, 0.99) * 1000, 1),
        "max": round(max(seconds) * 1000, 1) if seconds else 0.0,
    }


def _run(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=False,
                              cwd=REPO_ROOT).stdout.strip()
    except Exception:
        return ""


def _hardware() -> dict:
    system = platform.system()
    cpu_model, ram_bytes, physical = "", 0, None
    if system == "Darwin":
        cpu_model = _run(["sysctl", "-n", "machdep.cpu.brand_string"])
        ram_bytes = int(_run(["sysctl", "-n", "hw.memsize"]) or 0)
        physical = int(_run(["sysctl", "-n", "hw.physicalcpu"]) or 0) or None
    elif system == "Linux":
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    cpu_model = line.split(":", 1)[1].strip()
                    break
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemTotal"):
                    ram_bytes = int(line.split()[1]) * 1024
                    break
        except OSError:
            pass
    return {
        "cpu_model": cpu_model or platform.processor() or "unknown",
        "physical_cores": physical,
        "logical_cores": os.cpu_count(),
        "ram_gb": round(ram_bytes / 2**30, 1) if ram_bytes else None,
        "os": platform.platform(),
        "machine": platform.machine(),
    }


def _git() -> dict:
    return {
        "commit": _run(["git", "rev-parse", "HEAD"]) or os.getenv("OPENGTM_BUILD_SHA", ""),
        "branch": _run(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "dirty": bool(_run(["git", "status", "--porcelain", "--untracked-files=no"])),
    }


def _redis_reachable(url: str) -> bool:
    try:
        import redis

        return bool(redis.Redis.from_url(url, socket_connect_timeout=0.5).ping())
    except Exception:
        return False


def _http_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read())


# ── simulator lifecycle ───────────────────────────────────────────────────

def start_simulator(args: argparse.Namespace) -> tuple[subprocess.Popen, str]:
    cmd = [
        sys.executable, str(SIMULATOR), "--host", "127.0.0.1", "--port", "0",
        "--seed", str(args.seed), "--latency-ms", str(args.latency_ms),
        "--latency-dist", args.latency_dist, "--jitter-ms", str(args.jitter_ms),
        "--sigma", str(args.sigma), "--error-rate", str(args.error_rate),
        "--rate-429", str(args.rate_429),
    ]
    env = {k: v for k, v in os.environ.items() if not k.startswith("OPENGTM_BENCH_")}
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, env=env)
    deadline = time.monotonic() + 20
    assert proc.stdout is not None
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if line.startswith("READY "):
            return proc, line.split(" ", 1)[1].strip()
        if not line and proc.poll() is not None:
            break
    proc.kill()
    raise RuntimeError("provider simulator failed to start")


def write_manifest(path: Path, origin: str, args: argparse.Namespace) -> None:
    """A normal declarative manifest; only its URL points at the simulator."""
    path.write_text(
        f"""manifest_version: "1"
name: {PROVIDER_NAME}
display_name: Benchmark provider simulator
author: OpenGTM bench
capability: email
default_confidence: 0.9
cost_per_lookup: {args.cost_per_lookup}
auth:
  type: none
request:
  method: POST
  url: {origin}/v1/enrich
  headers:
    Content-Type: application/json
  body:
    domain: "{{{{input.domain}}}}"
    company: "{{{{input.company}}}}"
  timeout: {args.provider_http_timeout}
response:
  mappings:
    email: "$.data.email"
""",
        encoding="utf-8",
    )


# ── the measured run ──────────────────────────────────────────────────────

def _columns_config() -> list[dict]:
    return [
        {"id": "company", "name": "Company", "type": "lead_field", "lead_field": "company"},
        {"id": "website", "name": "Website", "type": "lead_field", "lead_field": "website"},
        {
            "id": COLUMN_ID, "name": "Email", "type": "enrichment",
            "provider": PROVIDER_NAME, "target_field": "email",
            # Keep the run to one provider call per cell: the email verify
            # cascade does live DNS/SMTP and would measure the network instead.
            "verify": False,
        },
    ]


def _payload(args, workbook_id: str, workspace_id: str) -> dict:
    """Mirror POST /workbooks/{id}/run's run_workbook payload."""
    return {
        "workbook_id": workbook_id,
        "workspace_id": workspace_id,
        "run_id": str(uuid.uuid4()),
        "column_ids": [COLUMN_ID],
        "row_columns": None,
        "row_ids": None,
        "lead_ids": None,
        "concurrency": args.row_concurrency,
        "max_providers": args.max_providers,
        "retry_passes": args.retry_passes,
        "provider_workers": args.provider_workers,
        "provider_timeout": args.provider_timeout,
        "fill_missing": False,
        "force": False,
    }


async def run_benchmark(args: argparse.Namespace, timings_path: Path) -> dict:
    from apps.api.database import SessionLocal, engine
    from apps.api.db_init import init_db
    from apps.api.models import Job
    from apps.api.services.job_registry import register_job_handlers
    from apps.api.services.queue_service import JOB_TIMEOUTS, QueueService
    from apps.api.services.workbook.models import (
        Workbook,
        WorkbookEnrichment,
        WorkbookRow,
    )

    init_db()
    tag = f"bench-enrich:{uuid.uuid4().hex[:12]}:"
    with SessionLocal() as db:
        if not args.allow_busy_queue and db.query(Job).filter(
            Job.status.in_(["pending", "processing"])
        ).count():
            raise RuntimeError("queue has pending/processing jobs; use a disposable database")

    workbook_ids: list[str] = []
    try:
        # ── fixtures: deterministic rows, stable simulator keys ──
        with SessionLocal() as db:
            for w in range(args.workbooks):
                workbook_id = f"{tag}wb{w}".replace(":", "-")
                workbook_ids.append(workbook_id)
                db.add(Workbook(
                    id=workbook_id, workspace_id=f"bench-ws-{w % args.workspaces}",
                    name=f"Enrichment bench {w}", source_type="empty",
                    columns_config=_columns_config(), status="draft",
                ))
            db.flush()
            rows = []
            for i in range(args.rows):
                w = i % args.workbooks
                rows.append({
                    "workspace_id": f"bench-ws-{w % args.workspaces}",
                    "workbook_id": workbook_ids[w], "position": i // args.workbooks,
                    "data": {"company": f"Bench Co {i:07d}",
                             "website": f"https://bench-{i:07d}.example"},
                    "enrichments": {},
                })
            db.bulk_insert_mappings(WorkbookRow, rows)
            db.commit()

        # ── queue + worker (the production QueueService, scoped to our jobs) ──
        queue = QueueService(concurrency=args.worker_concurrency)
        queue.max_active_per_workspace = args.max_active_per_workspace
        register_job_handlers(queue)
        queue.claim_next_job = functools.partial(queue.claim_next_job, fire_key_prefix=tag)

        enqueued_at = time.time()
        with SessionLocal() as db:
            for w, workbook_id in enumerate(workbook_ids):
                wb = db.query(Workbook).filter(Workbook.id == workbook_id).one()
                wb.status, wb.completed_rows = "running", 0
                wb.total_rows = sum(1 for i in range(args.rows) if i % args.workbooks == w)
                db.commit()
                queue.add_job(db, "run_workbook", _payload(args, workbook_id, wb.workspace_id),
                              fire_key=f"{tag}{w}")

        await queue.start_worker()
        deadline = time.monotonic() + args.timeout
        timed_out = False
        try:
            while True:
                with SessionLocal() as db:
                    active = db.query(Job).filter(
                        Job.fire_key.like(f"{tag}%"),
                        Job.status.in_(["pending", "processing"]),
                    ).count()
                if not active:
                    break
                if time.monotonic() > deadline:
                    timed_out = True
                    break
                await asyncio.sleep(0.2)
        finally:
            await queue.stop_worker()
        finished_at = time.time()

        # ── collect results ──
        with SessionLocal() as db:
            jobs = db.query(Job).filter(Job.fire_key.like(f"{tag}%")).all()
            job_statuses = Counter(job.status for job in jobs)
            job_errors = [job.error for job in jobs if job.error][:5]
            job_attempts = sum((job.retry_count or 0) for job in jobs)
            cell_status: Counter[str] = Counter()
            cell_errors: Counter[str] = Counter()
            for (enrichments,) in db.query(WorkbookRow.enrichments).filter(
                WorkbookRow.workbook_id.in_(workbook_ids)
            ):
                cell = (enrichments or {}).get(COLUMN_ID)
                status = cell.get("status") if isinstance(cell, dict) else "missing"
                cell_status[status] += 1
                if status != "complete":
                    cell_errors[str((cell or {}).get("error") or status)[:80]] += 1
            overlay_rows = db.query(WorkbookEnrichment).filter(
                WorkbookEnrichment.workbook_id.in_(workbook_ids)
            ).count()
            dialect = engine.dialect.name
            if dialect == "sqlite":
                import sqlite3

                db_version = sqlite3.sqlite_version
            else:
                from sqlalchemy import text

                db_version = db.execute(text("SHOW server_version")).scalar()

        records = []
        if timings_path.exists():
            records = [json.loads(line) for line in timings_path.read_text().splitlines() if line]
        final_by_cell: dict[tuple, dict] = {}
        for record in sorted(records, key=lambda r: r["end"]):
            final_by_cell[(record["workbook_id"], record["row_id"])] = record
        finals = list(final_by_cell.values())
        completed = cell_status.get("complete", 0)
        last_end = max((r["end"] for r in finals), default=finished_at)
        first_start = min((r["start"] for r in records), default=enqueued_at)
        busy = max(1e-9, last_end - first_start)
        to_last = max(1e-9, last_end - enqueued_at)

        return {
            "timed_out": timed_out,
            "dialect": dialect,
            "db_version": db_version,
            "jobs": {"count": len(jobs), "statuses": dict(job_statuses),
                     "retries": job_attempts, "errors": job_errors},
            "cells": {
                "total": args.rows, "completed": completed,
                "statuses": dict(cell_status), "errors": dict(cell_errors.most_common(10)),
                "overlay_rows": overlay_rows, "attempts_recorded": len(records),
            },
            "throughput": {
                "completed_per_second": round(completed / to_last, 2),
                "completed_per_second_excluding_startup": round(completed / busy, 2),
            },
            "latency_ms": {
                "end_to_end": _ms_summary([r["end"] - enqueued_at for r in finals]),
                "cell_execution": _ms_summary([r["end"] - r["start"] for r in records]),
                "startup_to_first_cell": round((first_start - enqueued_at) * 1000, 1),
            },
            "wall_seconds": {
                "enqueue_to_last_cell": round(to_last, 3),
                "enqueue_to_jobs_done": round(finished_at - enqueued_at, 3),
            },
            "job_timeout_seconds": JOB_TIMEOUTS.get("run_workbook"),
        }
    finally:
        if not args.keep_data and workbook_ids:
            with SessionLocal() as db:
                db.query(WorkbookEnrichment).filter(
                    WorkbookEnrichment.workbook_id.in_(workbook_ids)).delete(synchronize_session=False)
                db.query(WorkbookRow).filter(
                    WorkbookRow.workbook_id.in_(workbook_ids)).delete(synchronize_session=False)
                db.query(Workbook).filter(
                    Workbook.id.in_(workbook_ids)).delete(synchronize_session=False)
                db.query(Job).filter(Job.fire_key.like(f"{tag}%")).delete(synchronize_session=False)
                db.commit()


def _resources() -> dict:
    own = resource.getrusage(resource.RUSAGE_SELF)
    kids = resource.getrusage(resource.RUSAGE_CHILDREN)
    # ru_maxrss is bytes on macOS, KiB on Linux.
    scale = 1 if platform.system() == "Darwin" else 1024
    return {
        "harness_cpu_seconds": round(own.ru_utime + own.ru_stime, 2),
        "children_cpu_seconds": round(kids.ru_utime + kids.ru_stime, 2),
        "harness_peak_rss_mb": round(own.ru_maxrss * scale / 2**20, 1),
        "largest_child_peak_rss_mb": round(kids.ru_maxrss * scale / 2**20, 1),
        "note": "children = job child + provider workers + simulator (reaped processes)",
    }


def summary_text(report: dict) -> str:
    r, w, s = report["results"], report["identity"]["workload"], report["identity"]["worker_config"]
    lat = r["latency_ms"]
    sim = report.get("simulator_stats") or {}
    lines = [
        f"OpenGTM enrichment baseline  ok={report['ok']}  commit={report['identity']['git']['commit'][:12]}",
        f"  workload : {w['rows']} rows x 1 declarative HTTP column, {w['workbooks']} workbook(s), "
        f"simulator {w['simulator']['latency_dist']} {w['simulator']['latency_ms']}ms "
        f"err={w['simulator']['error_rate']} 429={w['simulator']['rate_429']} seed={w['simulator']['seed']}",
        f"  workers  : slots={s['worker_concurrency']} ws_cap={s['max_active_per_workspace']} "
        f"rows||={s['row_concurrency']} provider_workers={s['provider_workers']} "
        f"retry_passes={s['retry_passes']}  db={r['dialect']}",
        f"  result   : {r['cells']['completed']}/{r['cells']['total']} completed, "
        f"{r['throughput']['completed_per_second']} completions/s "
        f"({r['throughput']['completed_per_second_excluding_startup']}/s excl. startup)",
        f"  latency  : e2e p50/p95/p99 = {lat['end_to_end']['p50']}/{lat['end_to_end']['p95']}/"
        f"{lat['end_to_end']['p99']} ms; cell p50/p95/p99 = {lat['cell_execution']['p50']}/"
        f"{lat['cell_execution']['p95']}/{lat['cell_execution']['p99']} ms; "
        f"startup {lat['startup_to_first_cell']} ms",
        f"  errors   : {r['cells']['errors'] or 'none'}; provider requests={sim.get('requests')} "
        f"statuses={sim.get('status_counts')}",
        f"  wall     : {r['wall_seconds']['enqueue_to_jobs_done']} s",
    ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrichment completion harness (M0 baseline)")
    p.add_argument("--rows", type=int, default=500)
    p.add_argument("--workbooks", type=int, default=1, help="rows are split round-robin; one run_workbook job each")
    p.add_argument("--workspaces", type=int, default=1, help="workbooks are assigned round-robin")
    # simulator
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--latency-ms", type=float, default=50.0)
    p.add_argument("--latency-dist", choices=("fixed", "uniform", "lognormal"), default="fixed")
    p.add_argument("--jitter-ms", type=float, default=0.0)
    p.add_argument("--sigma", type=float, default=0.25)
    p.add_argument("--error-rate", type=float, default=0.0)
    p.add_argument("--rate-429", type=float, default=0.0)
    p.add_argument("--cost-per-lookup", type=float, default=0.0,
                   help=">0 routes calls through the spend-reservation path")
    p.add_argument("--provider-http-timeout", type=float, default=20.0)
    # worker config (defaults = shipped defaults; see docs/operations/worker-concurrency.md)
    p.add_argument("--worker-concurrency", type=int, default=None, help="default: WORKER_CONCURRENCY")
    p.add_argument("--max-active-per-workspace", type=int, default=None,
                   help="default: WORKER_MAX_ACTIVE_PER_WORKSPACE")
    p.add_argument("--row-concurrency", type=int, default=None, help="default: Settings row_concurrency")
    p.add_argument("--provider-workers", type=int, default=None, help="default: Settings provider_workers")
    p.add_argument("--provider-timeout", type=float, default=None, help="default: Settings provider_timeout")
    p.add_argument("--max-providers", type=int, default=None, help="default: Settings max_providers")
    p.add_argument("--retry-passes", type=int, default=None, help="default: Settings retry_passes")
    # environment
    p.add_argument("--database-url", default=os.getenv("BENCH_DATABASE_URL", ""),
                   help="disposable DB; default is a temporary SQLite file")
    p.add_argument("--allow-busy-queue", action="store_true")
    p.add_argument("--timeout", type=float, default=1800.0, help="overall wait for jobs (s)")
    p.add_argument("--keep-data", action="store_true", help="keep DB rows and temp dir")
    p.add_argument("--output", help="write the JSON report here")
    p.add_argument("--json", action="store_true", help="print the JSON report to stdout")
    p.add_argument("--label", default="", help="free-form label stored in the report")
    args = p.parse_args(argv)
    if not 1 <= args.rows <= 200_000 or not 1 <= args.workbooks <= min(args.rows, 64):
        p.error("rows must be 1..200000 and workbooks 1..min(rows, 64)")
    if not 1 <= args.workspaces <= args.workbooks:
        p.error("workspaces must be 1..workbooks")
    return args


def _resolve_worker_defaults(args: argparse.Namespace) -> None:
    """Fill unset knobs from the same sources production uses."""
    from apps.api.core.config import settings
    from apps.api.routers.settings import _ENRICHMENT_DEFAULTS

    if args.worker_concurrency is None:
        args.worker_concurrency = settings.WORKER_CONCURRENCY
    if args.max_active_per_workspace is None:
        args.max_active_per_workspace = settings.WORKER_MAX_ACTIVE_PER_WORKSPACE
    for key in ("row_concurrency", "provider_workers", "provider_timeout",
                "max_providers", "retry_passes"):
        if getattr(args, key) is None:
            setattr(args, key, _ENRICHMENT_DEFAULTS[key])


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    workdir = Path(tempfile.mkdtemp(prefix="opengtm-bench-"))
    database_url = args.database_url or f"sqlite:///{workdir}/bench.db"
    os.environ["DATABASE_URL"] = database_url
    os.environ.setdefault("LOG_LEVEL", "WARNING")
    sys.path.insert(0, str(REPO_ROOT))

    simulator, origin = start_simulator(args)
    manifest_path = workdir / f"{PROVIDER_NAME}.yaml"
    timings_path = workdir / "cell_timings.jsonl"
    write_manifest(manifest_path, origin, args)
    os.environ.update({
        "OPENGTM_BENCH_SIMULATOR_ORIGIN": origin,
        "OPENGTM_BENCH_MANIFEST": str(manifest_path),
        "OPENGTM_BENCH_TIMINGS": str(timings_path),
        "PYTHONPATH": os.pathsep.join(
            [str(SHIM_DIR), str(REPO_ROOT)] + [p for p in [os.getenv("PYTHONPATH", "")] if p]),
    })

    started = time.time()
    try:
        _resolve_worker_defaults(args)
        results = asyncio.run(run_benchmark(args, timings_path))
        try:
            simulator_stats = _http_json(f"{origin}/stats")
        except Exception as exc:  # pragma: no cover - diagnostic only
            simulator_stats = {"error": str(exc)}
    finally:
        simulator.terminate()
        try:
            simulator.wait(timeout=5)
        except subprocess.TimeoutExpired:
            simulator.kill()
        if not args.keep_data:
            shutil.rmtree(workdir, ignore_errors=True)

    from apps.api.services.queue_service import QueueService

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    report = {
        "schema_version": SCHEMA_VERSION,
        "benchmark": "enrichment_completion",
        "label": args.label,
        "ok": not results["timed_out"] and results["cells"]["completed"] > 0,
        "started_at": datetime.fromtimestamp(started, UTC).isoformat(),
        "identity": {
            "git": _git(),
            "hardware": _hardware(),
            "python": {"version": platform.python_version(),
                       "implementation": platform.python_implementation()},
            "database": {"dialect": results["dialect"], "version": results["db_version"],
                         "location": "temporary file" if not args.database_url else "caller-supplied"},
            "redis": {"reachable": _redis_reachable(redis_url)},
            "worker_config": {
                "worker_concurrency": args.worker_concurrency,
                "max_active_per_workspace": args.max_active_per_workspace,
                "worker_processes": 1,
                "heartbeat_interval_seconds": QueueService(concurrency=1).heartbeat_interval,
                "run_workbook_timeout_seconds": results["job_timeout_seconds"],
                "row_concurrency": args.row_concurrency,
                "provider_workers": args.provider_workers,
                "provider_timeout_seconds": args.provider_timeout,
                "max_providers": args.max_providers,
                "retry_passes": args.retry_passes,
            },
            "workload": {
                "rows": args.rows, "workbooks": args.workbooks, "workspaces": args.workspaces,
                "columns": [{"type": "enrichment", "provider": PROVIDER_NAME,
                             "path": "declarative manifest -> pebble provider worker -> httpx"}],
                "cost_per_lookup": args.cost_per_lookup,
                "simulator": {"seed": args.seed, "latency_ms": args.latency_ms,
                              "latency_dist": args.latency_dist, "jitter_ms": args.jitter_ms,
                              "sigma": args.sigma, "error_rate": args.error_rate,
                              "rate_429": args.rate_429},
            },
        },
        "results": {k: v for k, v in results.items() if k not in ("dialect", "db_version", "job_timeout_seconds")},
        "simulator_stats": {k: v for k, v in simulator_stats.items() if k != "config"},
        "resources": _resources(),
    }
    report["results"]["dialect"] = results["dialect"]

    encoded = json.dumps(report, indent=2, sort_keys=False)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = out.with_suffix(out.suffix + ".tmp")
        tmp.write_text(encoded + "\n", encoding="utf-8")
        tmp.replace(out)
    if args.json:
        print(encoded)
    print(summary_text(report), file=sys.stderr if args.json else sys.stdout)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
