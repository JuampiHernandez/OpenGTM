"""Standing refresh spends only on stale cells, preserving fresh results."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.api.database import Base
from apps.api.services.workbook import enrichment, refresh
from apps.api.services.workbook.models import Workbook, WorkbookEnrichment, WorkbookRow


@pytest.fixture
def refresh_runner(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'refresh-cells.db'}")
    Base.metadata.create_all(engine)
    sessions = sessionmaker(bind=engine, autoflush=False)
    now = datetime.now(timezone.utc)
    calls, sent = [], []

    async def ai(**kwargs):
        email = kwargs["row_cells"]["email"]["value"]
        calls.append((kwargs["prompt_template"], email))
        return {"value": "UPDATED", "error": None}

    async def append_row(spreadsheet, values, range_name, workspace_id):
        sent.append(values)
        return {"success": True, "range": range_name}

    monkeypatch.setattr(refresh, "SessionLocal", sessions)
    monkeypatch.setattr(refresh, "_now", lambda: now)
    monkeypatch.setattr(enrichment, "SessionLocal", sessions)
    monkeypatch.setattr(enrichment, "execute_ai_column", ai)
    monkeypatch.setattr(enrichment, "_make_redis", lambda: None)
    monkeypatch.setattr(enrichment, "flush_row_change_emits", lambda *args: None)
    monkeypatch.setattr(enrichment, "BATCH_ENABLED", False)
    monkeypatch.setattr("apps.api.services.integrations.sheets.append_row", append_row)
    yield sessions, now, calls, sent
    engine.dispose()


def populate(sessions, now, linked, *, all_fresh=False, stale_status="complete"):
    columns = [
        {"id": "email", "type": "input"},
        {"id": "summary", "type": "ai_formula", "prompt": "summary {email}"},
        {"id": "pitch", "type": "ai_formula", "prompt": "pitch {email}"},
        {"id": "send", "type": "output", "destination": "sheets",
         "destination_config": {"columns": ["email"]}},
    ]
    with sessions() as db:
        wb = Workbook(name="Refresh scope", workspace_id="refresh-scope", columns_config=columns,
                      refresh_policy={"staleness_ttl_days": {"summary": 7, "pitch": 30}})
        db.add(wb)
        db.flush()
        wid = wb.id
        for row_id, stale_col in [(101, "summary"), (202, "pitch")]:
            lead_id = row_id + 800 if linked else None
            row = WorkbookRow(id=row_id, workbook_id=wid, workspace_id="refresh-scope",
                              position=row_id, lead_id=lead_id,
                              data={"email": f"row{row_id}@example.test"}, enrichments={})
            db.add(row)
            db.flush()
            for col_id in ["summary", "pitch"]:
                stale = col_id == stale_col and not all_fresh
                cell = {"value": f"KEEP-{row_id}-{col_id}", "status": stale_status if stale else "complete"}
                row.enrichments = {**row.enrichments, col_id: cell}
                db.add(WorkbookEnrichment(
                    workbook_id=wid, workspace_id="refresh-scope", lead_id=lead_id or row_id,
                    column_id=col_id, value=cell["value"], status=cell["status"],
                    updated_at=now - timedelta(days=60) if stale else now))
        db.commit()
        return wid


@pytest.mark.parametrize("linked", [False, True])
@pytest.mark.parametrize("stale_status", ["complete", "error"])
def test_refresh_only_runs_each_rows_stale_cell(refresh_runner, linked, stale_status):
    sessions, now, calls, sent = refresh_runner
    wid = populate(sessions, now, linked, stale_status=stale_status)
    result = asyncio.run(refresh.refresh_workbook(wid, workspace_id="refresh-scope"))
    assert sorted(calls) == [("pitch {email}", "row202@example.test"),
                             ("summary {email}", "row101@example.test")]
    assert sent == []  # A schedule is not permission to dispatch output columns.
    assert result == {"sourced": 0, "reenriched": 2, "stale_rows": 2}
    with sessions() as db:
        for row_id, stale_col in [(101, "summary"), (202, "pitch")]:
            row = db.get(WorkbookRow, row_id)
            assert row.enrichments[stale_col]["value"] == "UPDATED"
            fresh_col = "pitch" if stale_col == "summary" else "summary"
            assert row.enrichments[fresh_col] == {"value": f"KEEP-{row_id}-{fresh_col}", "status": "complete"}
            assert "send" not in row.enrichments
    assert asyncio.run(refresh.refresh_workbook(wid, workspace_id="refresh-scope"))["stale_rows"] == 0
    assert len(calls) == 2 and sent == []


@pytest.mark.parametrize("linked", [False, True])
def test_all_fresh_cells_make_refresh_a_noop(refresh_runner, linked):
    sessions, now, calls, sent = refresh_runner
    wid = populate(sessions, now, linked, all_fresh=True)
    result = asyncio.run(refresh.refresh_workbook(wid, workspace_id="refresh-scope"))
    assert result == {"sourced": 0, "reenriched": 0, "stale_rows": 0}
    assert calls == sent == []


@pytest.mark.parametrize("linked", [False, True])
def test_refresh_scope_is_independent_of_scan_page_size(refresh_runner, monkeypatch, linked):
    from apps.api.core.config import settings

    sessions, now, calls, sent = refresh_runner
    monkeypatch.setattr(settings, "WORKBOOK_REFRESH_SCAN_BATCH_SIZE", 1)
    wid = populate(sessions, now, linked)
    result = asyncio.run(refresh.refresh_workbook(wid, workspace_id="refresh-scope"))
    assert sorted(calls) == [("pitch {email}", "row202@example.test"),
                             ("summary {email}", "row101@example.test")]
    assert result == {"sourced": 0, "reenriched": 2, "stale_rows": 2}


def test_refresh_recomputes_transitive_dependents_without_unrelated_spend(refresh_runner):
    sessions, now, calls, sent = refresh_runner
    columns = [
        {"id": "email", "type": "input"},
        {"id": "summary", "type": "ai_formula", "prompt": "summary {email}"},
        {"id": "derived", "type": "formula", "formula": "{summary}"},
        {"id": "final", "type": "ai_formula", "prompt": "final {derived}"},
        {"id": "fresh", "type": "ai_formula", "prompt": "fresh {email}"},
        {"id": "send", "type": "output", "destination": "sheets",
         "destination_config": {"columns": ["summary"]}},
    ]
    with sessions() as db:
        wb = Workbook(name="Refresh dependencies", workspace_id="refresh-scope", columns_config=columns)
        db.add(wb)
        db.flush()
        wid = wb.id
        row = WorkbookRow(id=101, workbook_id=wid, workspace_id="refresh-scope", position=0,
                          data={"email": "dependent@example.test"})
        db.add(row)
        db.flush()
        cells = {}
        for cid in ("summary", "derived", "final", "fresh"):
            cells[cid] = {"value": "OLD" if cid != "fresh" else "KEEP", "status": "complete"}
            db.add(WorkbookEnrichment(workbook_id=wid, workspace_id="refresh-scope", lead_id=101,
                                     column_id=cid, value=cells[cid]["value"], status="complete",
                                     updated_at=now-timedelta(days=60) if cid == "summary" else now))
        row.enrichments = cells
        db.commit()
    result = asyncio.run(refresh.refresh_workbook(wid, workspace_id="refresh-scope"))
    with sessions() as db:
        row = db.get(WorkbookRow, 101)
        assert row.enrichments["derived"]["value"] == "UPDATED"
        assert row.enrichments["final"]["value"] == "UPDATED"
        assert row.enrichments["fresh"] == {"value": "KEEP", "status": "complete"}
        assert "send" not in row.enrichments
    assert [prompt for prompt, _email in calls] == ["summary {email}", "final {derived}"]
    assert result == {"sourced": 0, "reenriched": 3, "stale_rows": 1}
    assert sent == []


def _sourced_workbook(sessions, monkeypatch, *, concurrent_row: bool):
    """Workbook whose sourcing adds row 202; optionally another writer adds 303 meanwhile."""
    from apps.api.services.workbook import source_engine, http_column, research_column, agent_column
    columns = [
        {"id": "source", "type": "source"},
        {"id": "email", "type": "input"},
        {"id": "formula", "type": "formula", "formula": "{email}"},
        {"id": "http", "type": "http", "url": "https://owned.example.test"},
        {"id": "agent", "type": "agent"},
        {"id": "research", "type": "research", "prompt": "research {email}"},
        {"id": "send", "type": "output", "destination": "sheets",
         "destination_config": {"columns": ["email"]}},
    ]
    with sessions() as db:
        wb = Workbook(name="Freshly sourced", workspace_id="refresh-scope", columns_config=columns)
        db.add(wb)
        db.flush()
        wid = wb.id
        # Existing rows must not acquire unrelated missing runner cells.
        db.add(WorkbookRow(id=101, workbook_id=wid, workspace_id="refresh-scope", position=0,
                           data={"email": "existing@example.test"}))
        db.commit()
    async def source(_workbook_id, _column_id, _workspace_id):
        with sessions() as db:
            db.add(WorkbookRow(id=202, workbook_id=wid, workspace_id="refresh-scope", position=1,
                               data={"email": "new@example.test"}))
            if concurrent_row:
                # A CSV import / API insert landing while sourcing is in flight.
                db.add(WorkbookRow(id=303, workbook_id=wid, workspace_id="refresh-scope", position=2,
                                   data={"email": "imported@example.test"}))
            db.commit()
        return {"added": 1, "added_row_ids": [202]}
    async def http(*args, **kwargs):
        return {"value": "HTTP", "error": None}
    async def research(*args, **kwargs):
        return {"value": "RESEARCH", "error": None}
    async def agent(*args, **kwargs):
        return {"value": "AGENT", "error": None}
    monkeypatch.setattr(source_engine, "materialize_source", source)
    monkeypatch.setattr(http_column, "execute_http_column", http)
    monkeypatch.setattr(research_column, "execute_research_column", research)
    monkeypatch.setattr(agent_column, "run_agent_cell", agent)
    return wid


def test_new_source_row_runs_every_runnable_column_without_dispatch(refresh_runner, monkeypatch):
    sessions, now, calls, sent = refresh_runner
    wid = _sourced_workbook(sessions, monkeypatch, concurrent_row=False)
    result = asyncio.run(refresh.refresh_workbook(wid, workspace_id="refresh-scope"))
    with sessions() as db:
        added = db.get(WorkbookRow, 202)
        assert {cid: added.enrichments[cid]["value"] for cid in ("formula", "http", "agent", "research")} == {
            "formula": "new@example.test", "http": "HTTP", "agent": "AGENT", "research": "RESEARCH"}
        assert db.get(WorkbookRow, 101).enrichments == {}
        assert "send" not in added.enrichments
    assert result == {"sourced": 1, "reenriched": 4, "stale_rows": 1}
    assert sent == calls == []


def test_concurrently_inserted_row_is_not_treated_as_sourced(refresh_runner, monkeypatch):
    """Only rows the refresh's own sourcing created get every column run.

    A row another writer inserts while sourcing is in flight is not ours: the
    scheduled refresh must not spend paid enrichments on it.
    """
    sessions, now, calls, sent = refresh_runner
    wid = _sourced_workbook(sessions, monkeypatch, concurrent_row=True)
    result = asyncio.run(refresh.refresh_workbook(wid, workspace_id="refresh-scope"))
    with sessions() as db:
        added = db.get(WorkbookRow, 202)
        assert {cid: added.enrichments[cid]["value"] for cid in ("formula", "http", "agent", "research")} == {
            "formula": "new@example.test", "http": "HTTP", "agent": "AGENT", "research": "RESEARCH"}
        assert not db.get(WorkbookRow, 303).enrichments
        assert not db.get(WorkbookRow, 101).enrichments
    assert result == {"sourced": 1, "reenriched": 4, "stale_rows": 1}
    assert sent == calls == []
