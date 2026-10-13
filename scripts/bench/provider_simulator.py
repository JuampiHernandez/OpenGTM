#!/usr/bin/env python3
"""Deterministic enrichment-provider simulator for benchmarks.

A tiny local HTTP server that behaves like a paid enrichment API with a
configurable, *seeded* latency distribution, 5xx error rate, and 429 rate. It
returns fixed enrichment payloads so throughput runs never touch a real vendor.

Determinism: every response is a pure function of ``(seed, request key,
attempt number for that key)``. The key is the request's ``domain`` field, so
outcomes do not depend on the order in which concurrent requests arrive. A retry
of the same key draws a fresh (but still reproducible) outcome.

    uv run python scripts/bench/provider_simulator.py --port 8099 \
        --latency-ms 50 --latency-dist lognormal --sigma 0.25 \
        --error-rate 0.01 --rate-429 0.02 --seed 42

Endpoints:
    POST /v1/enrich   {"domain": "...", "company": "..."} -> {"data": {...}}
    GET  /stats       request/status counters (for harness reports)
    GET  /healthz     liveness

On start it prints ``READY http://HOST:PORT`` (one line) to stdout so a parent
process can discover an ephemeral port (``--port 0``).

Benchmark-only tooling: this is not part of the application package.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import math
import random
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass

from aiohttp import web

LATENCY_DISTRIBUTIONS = ("fixed", "uniform", "lognormal")


@dataclass(frozen=True)
class SimulatorConfig:
    seed: int = 42
    latency_ms: float = 0.0
    latency_dist: str = "fixed"
    # uniform: +/- jitter_ms around latency_ms. lognormal: sigma of ln(latency).
    jitter_ms: float = 0.0
    sigma: float = 0.25
    error_rate: float = 0.0
    rate_429: float = 0.0

    def validate(self) -> SimulatorConfig:
        if self.latency_dist not in LATENCY_DISTRIBUTIONS:
            raise ValueError(f"latency_dist must be one of {LATENCY_DISTRIBUTIONS}")
        if self.latency_ms < 0 or self.jitter_ms < 0 or self.sigma < 0:
            raise ValueError("latency parameters must be non-negative")
        if not (0 <= self.error_rate <= 1 and 0 <= self.rate_429 <= 1):
            raise ValueError("error_rate and rate_429 must be within [0, 1]")
        if self.error_rate + self.rate_429 > 1:
            raise ValueError("error_rate + rate_429 must not exceed 1")
        return self


def decide(config: SimulatorConfig, key: str, attempt: int) -> tuple[float, int]:
    """Return ``(latency_seconds, http_status)`` for one request.

    Pure and order-independent: the RNG is seeded from a stable digest of the
    seed, the request key, and the per-key attempt number.
    """
    digest = hashlib.sha256(f"{config.seed}:{key}:{attempt}".encode()).digest()
    rng = random.Random(int.from_bytes(digest[:8], "big"))
    base = config.latency_ms
    if config.latency_dist == "uniform":
        latency_ms = rng.uniform(base - config.jitter_ms, base + config.jitter_ms)
    elif config.latency_dist == "lognormal" and base > 0:
        latency_ms = rng.lognormvariate(math.log(base), config.sigma)
    else:
        latency_ms = base
    roll = rng.random()
    if roll < config.rate_429:
        status = 429
    elif roll < config.rate_429 + config.error_rate:
        status = 500
    else:
        status = 200
    return max(0.0, latency_ms) / 1000.0, status


def payload_for(domain: str, company: str) -> dict:
    """The fixed enrichment payload returned on success."""
    slug = domain or "unknown.invalid"
    return {
        "data": {
            "email": f"contact@{slug}",
            "company": company or slug,
            "company_size": "51-200",
            "industry": "Software",
        }
    }


def build_app(config: SimulatorConfig) -> web.Application:
    config.validate()
    attempts: Counter[str] = Counter()
    statuses: Counter[int] = Counter()
    state = {"requests": 0, "started_at": time.time(), "in_flight": 0, "peak_in_flight": 0}

    async def enrich(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:
            body = {}
        domain = str((body or {}).get("domain") or request.query.get("domain") or "")
        company = str((body or {}).get("company") or "")
        attempt = attempts[domain]
        attempts[domain] += 1
        state["requests"] += 1
        state["in_flight"] += 1
        state["peak_in_flight"] = max(state["peak_in_flight"], state["in_flight"])
        try:
            latency, status = decide(config, domain, attempt)
            if latency:
                await asyncio.sleep(latency)
        finally:
            state["in_flight"] -= 1
        statuses[status] += 1
        if status == 429:
            return web.json_response({"error": "rate_limited"}, status=429,
                                     headers={"Retry-After": "1"})
        if status >= 500:
            return web.json_response({"error": "simulated_failure"}, status=status)
        return web.json_response(payload_for(domain, company))

    async def stats(_request: web.Request) -> web.Response:
        return web.json_response({
            "config": asdict(config),
            "requests": state["requests"],
            "unique_keys": len(attempts),
            "retried_keys": sum(1 for count in attempts.values() if count > 1),
            "status_counts": {str(code): count for code, count in sorted(statuses.items())},
            "peak_in_flight": state["peak_in_flight"],
            "uptime_seconds": round(time.time() - state["started_at"], 3),
        })

    async def healthz(_request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    app = web.Application()
    app.router.add_post("/v1/enrich", enrich)
    app.router.add_get("/v1/enrich", enrich)
    app.router.add_get("/stats", stats)
    app.router.add_get("/healthz", healthz)
    return app


async def _serve(config: SimulatorConfig, host: str, port: int) -> None:
    runner = web.AppRunner(build_app(config), access_log=None)
    await runner.setup()
    # Large backlog: benchmark clients open many short-lived connections.
    site = web.TCPSite(runner, host, port, backlog=4096)
    await site.start()
    bound = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    print(f"READY http://{host}:{bound}", flush=True)
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0, help="0 = ephemeral")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--latency-ms", type=float, default=0.0)
    parser.add_argument("--latency-dist", choices=LATENCY_DISTRIBUTIONS, default="fixed")
    parser.add_argument("--jitter-ms", type=float, default=0.0)
    parser.add_argument("--sigma", type=float, default=0.25)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument("--rate-429", type=float, default=0.0)
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> SimulatorConfig:
    return SimulatorConfig(
        seed=args.seed, latency_ms=args.latency_ms, latency_dist=args.latency_dist,
        jitter_ms=args.jitter_ms, sigma=args.sigma, error_rate=args.error_rate,
        rate_429=args.rate_429,
    ).validate()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        asyncio.run(_serve(config_from_args(args), args.host, args.port))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
