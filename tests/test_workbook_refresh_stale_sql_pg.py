"""Stale refresh-cell selection on PostgreSQL with FORCE RLS.

The SQL selection compares ``timestamp without time zone`` overlays against
bound cutoffs, cross-joins a literal column set and LEFT JOINs overlays on
``COALESCE(lead_id, id)``. Prove it matches the legacy Python oracle on PG
under the app role, and that another tenant's rows/overlays are invisible.
Set TEST_DATABASE_URL to a disposable PostgreSQL database (see pg_rls_support).
"""

import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL not set"),
]


@pytest.fixture(scope="module")
def app_session():
    from tests.pg_rls_support import rls_app_session

    factory, dispose = rls_app_session(TEST_DATABASE_URL, pool_size=5)
    try:
        yield factory
    finally:
        dispose()


@pytest.mark.parametrize("batch_size", [1, 4, 1000])
def test_pg_sql_selection_matches_legacy_python(app_session, monkeypatch, batch_size):
    from apps.api.core.tenancy import workspace_scope
    from apps.api.services.workbook import refresh
    from tests.refresh_stale_support import (
        ENRICHMENT_COLS, STATES, TTL_MAP, legacy_stale_row_columns, populate_random,
    )

    now = datetime.now(timezone.utc)
    monkeypatch.setattr(refresh, "_now", lambda: now)
    ws = f"stale_{uuid4().hex}"
    with workspace_scope(ws):
        with app_session() as db:
            wid, seen_states = populate_random(db, ws, now, n_rows=30, seed=batch_size)
            expected = legacy_stale_row_columns(db, wid, ENRICHMENT_COLS, TTL_MAP, now)
            got = refresh._stale_row_columns(db, wid, ENRICHMENT_COLS, TTL_MAP, batch_size=batch_size)
    assert seen_states == set(STATES)
    assert got == expected and list(got) == list(expected)
    assert 0 < sum(map(len, got.values())) < 30 * len(ENRICHMENT_COLS)

    # Another tenant sees neither the rows nor the overlays (RLS), even when
    # it names the workbook id directly.
    with workspace_scope(f"other_{uuid4().hex}"):
        with app_session() as db:
            assert refresh._stale_row_columns(db, wid, ENRICHMENT_COLS, TTL_MAP, batch_size=batch_size) == {}
