"""Benchmark-only process hooks for the enrichment completion harness.

The harness puts this directory on ``PYTHONPATH`` so every Python process it
starts (the durable job child and the pebble provider workers forked from the
forkserver) imports this module at interpreter startup. It does nothing unless
``OPENGTM_BENCH_SIMULATOR_ORIGIN`` is set, and it never ships with the app.

When active it installs post-import patches (applied as each target module is
first executed, so ``from x import y`` callers see the patched object):

* ``apps.api.core.url_guard.check_url`` allows exactly one loopback origin, the
  provider simulator (scheme + host + port must match). Every other URL still
  goes through the unmodified SSRF guard.
* ``register_declarative_manifests`` also registers the harness-generated
  manifest at ``OPENGTM_BENCH_MANIFEST`` through the normal compile/register
  path, so the run uses the real ``DeclarativeProvider``.
* ``enrich_cell`` is wrapped to append one JSON line per cell attempt (wall-clock
  start/end, outcome) to ``OPENGTM_BENCH_TIMINGS`` for latency percentiles.
"""

from __future__ import annotations

import importlib.abc
import json
import os
import sys
import time
from urllib.parse import urlparse

_ORIGIN = os.environ.get("OPENGTM_BENCH_SIMULATOR_ORIGIN", "").rstrip("/")


def _origin_key(url: str) -> tuple[str, str, int | None]:
    parsed = urlparse(url)
    port = parsed.port or {"http": 80, "https": 443}.get(parsed.scheme)
    return parsed.scheme, (parsed.hostname or "").lower(), port


def _is_loopback_origin(origin: str) -> bool:
    import ipaddress

    scheme, host, port = _origin_key(origin)
    try:
        return scheme == "http" and port is not None and ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _patch_url_guard(module) -> None:
    original = module.check_url
    allowed = _origin_key(_ORIGIN)
    if not _is_loopback_origin(_ORIGIN):
        # Only a loopback simulator may be exempted; never widen egress to a
        # private-network or metadata host, even in benchmark processes.
        raise RuntimeError("OPENGTM_BENCH_SIMULATOR_ORIGIN must be http://<loopback-ip>:<port>")

    def check_url(url, *args, **kwargs):
        if isinstance(url, str) and _origin_key(url.strip()) == allowed:
            return url
        return original(url, *args, **kwargs)

    check_url.__wrapped__ = original  # type: ignore[attr-defined]
    module.check_url = check_url


def _patch_declarative_registry(module) -> None:
    original = module.register_declarative_manifests
    manifest_path = os.environ.get("OPENGTM_BENCH_MANIFEST", "")

    def register_declarative_manifests() -> int:
        count = original()
        if manifest_path:
            from pathlib import Path

            from apps.api.services.leadgen.enrichment.declarative.compiler import (
                compile_manifest,
            )
            from apps.api.services.leadgen.enrichment.declarative.manifest import (
                load_manifest,
            )
            from apps.api.services.workbook.providers import register_provider

            register_provider(compile_manifest(load_manifest(Path(manifest_path))))
            count += 1
        return count

    module.register_declarative_manifests = register_declarative_manifests


def _patch_enrichment(module) -> None:
    timings_path = os.environ.get("OPENGTM_BENCH_TIMINGS", "")
    if not timings_path:
        return
    original = module.enrich_cell

    async def enrich_cell(*args, **kwargs):
        started = time.time()
        result = None
        try:
            result = await original(*args, **kwargs)
            return result
        finally:
            lead_data = kwargs.get("lead_data") or {}
            record = {
                "row_id": lead_data.get("__row_id"),
                "workbook_id": kwargs.get("workbook_id"),
                "column_id": kwargs.get("col_id"),
                "start": started,
                "end": time.time(),
                "success": bool(isinstance(result, dict) and result.get("success")),
                "error": (result or {}).get("error") if isinstance(result, dict) else "exception",
            }
            # One short O_APPEND write per line; safe across the job children.
            with open(timings_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")

    module.enrich_cell = enrich_cell


_PATCHES = {
    "apps.api.core.url_guard": _patch_url_guard,
    "apps.api.services.leadgen.enrichment.declarative.registry": _patch_declarative_registry,
    "apps.api.services.workbook.enrichment": _patch_enrichment,
}


class _PatchingLoader(importlib.abc.Loader):
    def __init__(self, inner, patch):
        self._inner = inner
        self._patch = patch

    def create_module(self, spec):
        return self._inner.create_module(spec)

    def exec_module(self, module):
        self._inner.exec_module(module)
        self._patch(module)


class _PatchingFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        patch = _PATCHES.get(fullname)
        if patch is None:
            return None
        for finder in sys.meta_path:
            if finder is self or not hasattr(finder, "find_spec"):
                continue
            spec = finder.find_spec(fullname, path, target)
            if spec is not None and spec.loader is not None:
                spec.loader = _PatchingLoader(spec.loader, patch)
                return spec
        return None


if _ORIGIN and not any(isinstance(f, _PatchingFinder) for f in sys.meta_path):
    sys.meta_path.insert(0, _PatchingFinder())
    # `kill -USR1 <pid>` dumps every thread's stack to stderr: diagnoses stalls
    # without root (py-spy needs root on macOS).
    import faulthandler
    import signal

    if hasattr(signal, "SIGUSR1"):
        faulthandler.register(signal.SIGUSR1, all_threads=True)
