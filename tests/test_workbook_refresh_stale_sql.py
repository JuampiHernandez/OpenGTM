"""Refresh selects stale cells in bounded SQL with the legacy Python semantics."""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.api.core.config import settings
from apps.api.database import Base
from apps.api.services.workbook import refresh
from apps.api.services.workbook.models import Workbook, WorkbookEnrichment, WorkbookRow
from tests.refresh_stale_support import (
    ENRICHMENT_COLS, STATES, TTL_MAP, legacy_stale_row_columns, populate_random,
)

WS = "stale-sql"


@pytest.fixture
def db_now(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'stale-sql.db'}")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, autoflush=False)
    now = datetime.now(timezone.utc)
    monkeypatch.setattr(refresh, "_now", lambda: now)
    with sessions() as db:
        yield db, now
    engine.dispose()


@pytest.mark.parametrize("batch_size", [1, 3, 7, 40, 1000])
@pytest.mark.parametrize("seed", [33, 34, 35])
def test_sql_selection_matches_legacy_python(db_now, batch_size, seed):
    db, now = db_now
    wid, seen_states = populate_random(db, WS, now, seed=seed)
    assert seen_states == set(STATES)  # every cell state is exercised
    expected = legacy_stale_row_columns(db, wid, ENRICHMENT_COLS, TTL_MAP, now)
    got = refresh._stale_row_columns(db, wid, ENRICHMENT_COLS, TTL_MAP, batch_size=batch_size)
    assert got == expected
    assert list(got) == list(expected)  # row order (id) and per-row column order
    assert 0 < sum(map(len, got.values())) < 40 * len(ENRICHMENT_COLS)  # mixed fresh/stale


def test_setting_bounds_each_page(db_now, monkeypatch):
    db, now = db_now
    wid, _ = populate_random(db, WS, now)
    expected = legacy_stale_row_columns(db, wid, ENRICHMENT_COLS, TTL_MAP, now)
    statements = []
    from sqlalchemy import event

    @event.listens_for(db.get_bind(), "before_cursor_execute")
    def _count(conn, cursor, statement, params, context, executemany):  # noqa: ANN001
        statements.append(statement)

    monkeypatch.setattr(settings, "WORKBOOK_REFRESH_SCAN_BATCH_SIZE", 5)
    assert refresh._stale_row_columns(db, wid, ENRICHMENT_COLS, TTL_MAP) == expected
    # 40 rows / 5 per page: 8 LIMIT-bounded page probes with one id-range
    # selection each, plus a final empty probe.
    assert sum("LIMIT" in s for s in statements) == 9
    assert len(statements) == 9 + 8


def test_fresh_stale_missing_and_default_ttl_cells(db_now):
    db, now = db_now
    wb = Workbook(name="cases", workspace_id=WS, refresh_policy={})
    db.add(wb)
    db.flush()
    cols = [{"id": "a", "type": "ai_formula"}, {"id": "b", "type": "enrichment", "target_field": "f"},
            {"id": "c", "type": "waterfall"}]
    ttl = {"a": 1, "f": 10}  # c has no entry -> 30 days
    cells = {  # row_id -> {col: days old or None for missing}
        1: {"a": 0, "b": 0, "c": 0},          # all fresh
        2: {"a": 2, "b": 9, "c": 29},         # a stale only
        3: {"a": 0, "b": 11, "c": 31},        # b and c stale
        4: {},                                # never enriched
    }
    for row_id, ages in cells.items():
        db.add(WorkbookRow(id=row_id, workbook_id=wb.id, workspace_id=WS, data={}, enrichments={}))
        for cid, days in ages.items():
            db.add(WorkbookEnrichment(workbook_id=wb.id, workspace_id=WS, lead_id=row_id, column_id=cid,
                                      status="complete",
                                      updated_at=(now - timedelta(days=days)).replace(tzinfo=None)))
    db.commit()
    expected = {"2": ["a"], "3": ["b", "c"], "4": ["a", "b", "c"]}
    assert legacy_stale_row_columns(db, wb.id, cols, ttl, now) == expected
    for size in (1, 2, 100):
        assert refresh._stale_row_columns(db, wb.id, cols, ttl, batch_size=size) == expected


def test_empty_workbook_and_no_columns(db_now):
    db, now = db_now
    wb = Workbook(name="empty", workspace_id=WS)
    db.add(wb)
    db.commit()
    assert refresh._stale_row_columns(db, wb.id, ENRICHMENT_COLS, TTL_MAP, batch_size=1) == {}
    wid, _ = populate_random(db, WS, now, n_rows=3)
    assert refresh._stale_row_columns(db, wid, [], TTL_MAP) == {}
