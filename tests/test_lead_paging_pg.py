"""Score-ordered lead paging on PostgreSQL with FORCE RLS.

SQLite happens to return tied rows in rowid order, which masks a missing
tiebreaker. PostgreSQL gives no such guarantee, so OFFSET pages over tied
scores can overlap and skip rows unless the ORDER BY is total. Set
TEST_DATABASE_URL to a disposable PostgreSQL database (see pg_rls_support).
"""

import asyncio
import os
from uuid import uuid4

import pytest

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL not set"),
]

COUNT = 1001


@pytest.fixture(scope="module")
def app_session():
    from tests.pg_rls_support import rls_app_session

    factory, dispose = rls_app_session(TEST_DATABASE_URL, pool_size=5)
    try:
        yield factory
    finally:
        dispose()


@pytest.fixture
def tied_store(app_session, monkeypatch):
    """A PgLeadStore holding COUNT leads that all share one score."""
    from apps.api.core.tenancy import workspace_scope
    from apps.api.services.leadgen import store as store_module
    from apps.api.services.leadgen.models import Lead

    monkeypatch.setattr(store_module, "SessionLocal", app_session)
    ws = f"tied_{uuid4().hex}"
    store = store_module.PgLeadStore(ws)
    with workspace_scope(ws):
        ids = {store.upsert_lead(Lead(company=f"Tied {i}", city="Tie", score=7))
               for i in range(COUNT)}
    assert len(ids) == COUNT
    return ws, store, ids


def test_query_leads_page_tied_scores_returns_each_id_once(tied_store):
    from apps.api.core.tenancy import workspace_scope

    ws, store, ids = tied_store
    seen = []
    with workspace_scope(ws):
        for page in range(1, COUNT // 100 + 3):
            rows, total = store.query_leads_page({"city": "Tie"}, page=page, page_size=100)
            assert total == COUNT
            seen.extend(row["id"] for row in rows)
        listed = [lead.id for offset in range(0, COUNT, 100)
                  for lead in store.get_leads(limit=100, offset=offset)]
    assert len(seen) == len(set(seen)) == COUNT and set(seen) == ids
    assert len(listed) == len(set(listed)) == COUNT and set(listed) == ids


def test_workbook_migration_tied_scores_migrates_each_lead_once(tied_store, monkeypatch):
    """Drive the legacy-lead migration's 500-row pages over the PG store."""
    from apps.api.core.tenancy import WorkspaceCtx, workspace_scope
    from apps.api.database import SessionLocal
    from apps.api.routers.workbooks import migrate_workbook_to_v2
    from apps.api.services.workbook.models import Workbook, WorkbookRow

    ws, store, ids = tied_store
    ctx = WorkspaceCtx(user=None, workspace_id=ws, slug=ws)
    monkeypatch.setattr(WorkspaceCtx, "lead_db", lambda self: store)
    with workspace_scope(ws):
        with SessionLocal() as db:
            wb = Workbook(name="Tied PG migration", workspace_id=ws, source_type="leads_filter",
                          filter_criteria={"city": "Tie"}, columns_config=[])
            db.add(wb)
            db.commit()
            wid = wb.id
            result = asyncio.run(migrate_workbook_to_v2(wid, db=db, ctx=ctx))
        assert result["rows"] == COUNT
        with SessionLocal() as db:
            lead_ids = [row.lead_id for row in db.query(WorkbookRow).filter_by(workbook_id=wid)]
    assert len(lead_ids) == len(set(lead_ids)) == COUNT
    assert set(lead_ids) == ids
