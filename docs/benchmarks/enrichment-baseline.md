# Enrichment completion baseline (M0)

**Developer-laptop baseline.** These numbers come from one development machine
with simulated providers. Use them as a regression reference only. They are
not capacity claims and do not meet the M0 exit gate in
[the backend plan](../plans/go-python-backend-rewrite.md), which needs a
controlled environment, PostgreSQL, steady-state duration, and API
measurements. Raw reports:
[enrichment-baseline-2026-10-14.json](enrichment-baseline-2026-10-14.json).
For how to run the harness and what each knob does, see
[operations/worker-concurrency.md](../operations/worker-concurrency.md).

## Identity

| Field | Value |
| --- | --- |
| Commit | `9188fc4d040c186f3495902fe77e0d041deba3bd` (application code; harness files added on top, uncommitted at run time) |
| Hardware | Apple M2, 8 cores (8 logical), 16 GB RAM |
| OS | macOS 26.4.1 (arm64) |
| Python | CPython 3.14.5 (uv-managed venv) |
| Database | SQLite 3.50.4 (temporary file, WAL); PostgreSQL 18.4 (Homebrew, local socket, superuser role, so RLS policies were bypassed) |
| Redis | Reachable on localhost (cell broadcasts enabled) |
| Worker processes | 1 harness worker process (`QueueService`), 1 job child per slot |
| Defaults used unless noted | `WORKER_CONCURRENCY=1`, `WORKER_MAX_ACTIVE_PER_WORKSPACE=2`, `row_concurrency=12`, `provider_workers=8`, `provider_timeout=10`, `max_providers=8`, `retry_passes=1`, heartbeat 30 s, `run_workbook` timeout 1800 s |
| Workload | N rows x 1 `enrichment` column on a declarative HTTP provider (manifest -> pebble provider worker -> httpx -> simulator), `verify: false`, `cost_per_lookup: 0`, one provider call per cell |
| Simulator | Seed 42; latency, distribution, and error rates per row below |
| Date | 2026-10-14 |

## Results

Throughput is committed completions/sec from enqueue to the last result
commit; the number in parentheses excludes the job-start overhead. "E2E"
means enqueue to the cell's result commit (one batch enqueue, so it includes
queueing). "Cell" is one `enrich_cell` execution: provider call plus commit.

| Run | DB | Rows | Simulator | Config change | Done | Completions/s | E2E p50 / p95 / p99 (ms) | Cell p50 / p95 / p99 (ms) | Wall (s) |
| --- | --- | ---: | --- | --- | ---: | ---: | --- | --- | ---: |
| A | SQLite | 500 | fixed 50 ms | defaults | 500 | 70.1 (77.2) | 4227 / 6852 / 7121 | 87 / 132 / 702 | 7.5 |
| B | SQLite | 2000 | fixed 50 ms | defaults | 2000 | 80.6 (82.9) | 13248 / 23697 / 24587 | 85 / 130 / 139 | 25.2 |
| C | SQLite | 2000 | fixed 0 ms | defaults | 2000 | 214.6 (230.5) | 5347 / 8950 / 9243 | 40 / 47 / 94 | 9.7 |
| D | SQLite | 2000 | lognormal 50 ms, sigma 0.5, 1% 5xx, 2% 429 | defaults | 1999 | 61.7 (63.0) | 16487 / 30490 / 32078 | 93 / 170 / 234 | 32.9 |
| E | SQLite | 2000 | fixed 50 ms | `provider_workers=12` | 2000 | 112.9 (117.3) | 9826 / 16948 / 17532 | 84 / 94 / 133 | 18.0 |
| F | SQLite | 2000 | fixed 50 ms | 4 workbooks / 2 workspaces, `WORKER_CONCURRENCY=4` | 2000 | 189.2 (203.7) | 6882 / 10063 / 10389 | 122 / 195 / 2482 | 11.0 |
| G | PostgreSQL | 500 | fixed 50 ms | defaults | 500 | 45.1 (48.1) | 6204 / 10672 / 11099 | 183 / 219 / 662 | 11.4 |
| H | PostgreSQL | 2000 | fixed 50 ms | defaults | 2000 | 37.8 (38.3) | 20794 / 49865 / 52167 | 214 / 411 / 469 | 53.3 |

Provider attempts equal rows in every run except D, which made 2061 requests:
1999 returned 200, 38 returned 429, and 24 returned 5xx. The retry pass
recovered all but one cell. Peak simulator in-flight requests matched
`provider_workers` per job (8, 12, or 32 = 4 jobs x 8). Job startup (claim,
child process, provider registry, pool start) took 0.65-0.75 s to the first
cell. Peak RSS per process was about 160-170 MB.

## Observations to carry into M0/M3

1. **Provider pool and batching set throughput.** With the defaults, 12 rows
   per batch share 8 provider processes, and each batch waits for its slowest
   row. Raising `provider_workers` to match `row_concurrency` gave +40%
   (B -> E). The zero-latency ceiling (C) is about 215/s per job on SQLite.
2. **Row concurrency above the DB pool deadlocks a run.** With
   `row_concurrency=32` (allowed by Settings, up to 64), the job child blocked
   its event loop on a SQLAlchemy `QueuePool` checkout ("limit of size 5
   overflow 10 reached"). A 300-row run finished 0 cells in 240 s, and an
   earlier 2000-row attempt finished 14 cells in about 10 minutes. This
   should be a bug fix (bound or decouple sessions from in-flight provider
   calls), not a tuning note.
3. **PostgreSQL is slower than SQLite here and degrades with N.** Cell p95 went
   from 219 ms (G, 500 rows) to 411 ms (H, 2000 rows) at the same provider
   latency. Per-cell database work appears to grow with workbook size. Profile
   this before M3 ("bounded workbook refresh queries").
4. **Failure detail is lost.** The cell that exhausted retries in D recorded
   `no_data`. The provider's `http_429`/`http_500` error does not reach the
   cell, so 429s are not visible per cell and do not count as rate-limit
   signals.
5. **Slots scale across workbooks, not within one.** Four slots and four
   workbooks reached 189/s (F). A single large workbook uses one slot no
   matter what `WORKER_CONCURRENCY` is set to.

## Reproduce

```bash
uv run python scripts/bench/enrichment_harness.py --rows 500  --latency-ms 50 --output a.json
uv run python scripts/bench/enrichment_harness.py --rows 2000 --latency-ms 50 --output b.json
uv run python scripts/bench/enrichment_harness.py --rows 2000 --latency-ms 0  --output c.json
uv run python scripts/bench/enrichment_harness.py --rows 2000 --latency-ms 50 --latency-dist lognormal \
  --sigma 0.5 --error-rate 0.01 --rate-429 0.02 --output d.json
uv run python scripts/bench/enrichment_harness.py --rows 2000 --latency-ms 50 --provider-workers 12 --output e.json
uv run python scripts/bench/enrichment_harness.py --rows 2000 --latency-ms 50 --workbooks 4 --workspaces 2 \
  --worker-concurrency 4 --output f.json
BENCH_DATABASE_URL='postgresql+psycopg://USER@/opengtm_bench?host=/tmp' \
  uv run python scripts/bench/enrichment_harness.py --rows 2000 --latency-ms 50 --output h.json
```
