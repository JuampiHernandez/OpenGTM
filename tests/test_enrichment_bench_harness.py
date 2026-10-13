"""Keep the M0 enrichment benchmark harness and provider simulator working."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BENCH_DIR = REPO_ROOT / "scripts" / "bench"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module  # dataclasses resolve their module by name
    spec.loader.exec_module(module)
    return module


def test_simulator_outcomes_are_deterministic_and_order_independent():
    sim = _load("bench_provider_simulator", BENCH_DIR / "provider_simulator.py")
    config = sim.SimulatorConfig(seed=7, latency_ms=50, latency_dist="lognormal",
                                 sigma=0.5, error_rate=0.1, rate_429=0.2).validate()
    keys = [f"bench-{i:07d}.example" for i in range(2000)]
    forward = [sim.decide(config, key, 0) for key in keys]
    backward = [sim.decide(config, key, 0) for key in reversed(keys)][::-1]
    assert forward == backward
    statuses = [status for _, status in forward]
    assert 0.15 < statuses.count(429) / len(keys) < 0.25
    assert 0.06 < statuses.count(500) / len(keys) < 0.14
    # A retry of the same key draws a fresh, reproducible outcome.
    assert sim.decide(config, keys[0], 1) == sim.decide(config, keys[0], 1)
    assert sim.decide(sim.SimulatorConfig(latency_ms=50), "k", 0) == (0.05, 200)


def test_shim_only_exempts_loopback_origins():
    shim = _load("bench_sitecustomize", BENCH_DIR / "_shim" / "sitecustomize.py")
    assert shim._is_loopback_origin("http://127.0.0.1:8099")
    assert not shim._is_loopback_origin("http://10.0.0.5:8099")
    assert not shim._is_loopback_origin("http://169.254.169.254:80")
    assert not shim._is_loopback_origin("http://localhost:8099")
    assert not shim._is_loopback_origin("https://127.0.0.1")


def test_harness_smoke_completes_tiny_run(tmp_path):
    """End to end: queue claim -> job child -> declarative provider -> commit."""
    output = tmp_path / "report.json"
    env = {k: v for k, v in os.environ.items()
           if k not in {"DATABASE_URL", "BENCH_DATABASE_URL", "PYTHONPATH"}
           and not k.startswith("OPENGTM_BENCH_")}
    result = subprocess.run(
        [sys.executable, str(BENCH_DIR / "enrichment_harness.py"), "--rows", "5",
         "--latency-ms", "0", "--timeout", "60", "--output", str(output)],
        cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=120, check=False,
    )
    assert result.returncode == 0, result.stderr[-4000:]
    report = json.loads(output.read_text())
    assert report["ok"] is True
    assert report["results"]["cells"]["completed"] == 5
    assert report["results"]["jobs"]["statuses"] == {"completed": 1}
    assert report["simulator_stats"]["requests"] == 5
    assert report["simulator_stats"]["status_counts"] == {"200": 5}
    assert report["results"]["latency_ms"]["end_to_end"]["count"] == 5
    identity = report["identity"]
    assert identity["database"]["dialect"] == "sqlite"
    assert identity["workload"]["rows"] == 5
    assert {"worker_concurrency", "max_active_per_workspace", "row_concurrency",
            "provider_workers"} <= set(identity["worker_config"])
    assert identity["hardware"]["logical_cores"]
