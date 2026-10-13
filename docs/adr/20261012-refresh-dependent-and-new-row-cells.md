# Refresh dependent cells and initialize sourced rows

## Context

PR #74 restricts standing refresh to stale enrichment cells per row. The strict
allowlist preserved unrelated fresh values but also excluded fresh downstream
cells whose inputs changed, and left newly sourced rows' formula, HTTP, agent
and research cells uninitialized.

## Decision

Expand each existing row's stale-cell scope through the existing transitive
`downstream_columns` helper, using the row-edit path's reactive eligibility
policy. The source engine reports the IDs of the rows each run inserted
(`added_row_ids` in the `materialize_source` summary); those rows, and only
those, initialize every runnable non-output column. Pass the resulting exact
per-row scopes through the existing enrichment runner.

## Alternatives

The original version of this decision snapshotted persisted row IDs before
sourcing and diffed afterward, avoiding a source-engine contract change. That
misclassified rows inserted concurrently by other writers (CSV import, API
insert, another job) as sourced, running paid enrichments on them; it was
replaced by the additive `added_row_ids` key. Running all columns on every stale row would restore dependent
values but discard this PR's cost-saving behavior for unrelated fresh cells.
Running outputs during sourcing would introduce unrequested external dispatch.

## Consequences

Changed inputs propagate through derived columns while unrelated fresh cells
remain intact. Explicitly non-reactive dependents stay excluded; outputs remain
non-reactive by default and new-row initialization excludes outputs.

Resolved limitation: the snapshot diff was not concurrent-writer safe. Refresh
now uses the IDs the source engine itself inserted, so rows added by another
writer during sourcing are left alone (covered by
`test_concurrently_inserted_row_is_not_treated_as_sourced`). No extra row-ID
queries are issued.

## Validation

Python 3.12.3 and the repository's frozen uv dependencies: both new regressions
failed on the reviewed head (2 failed, 6 passed). After repair, 49 tests passed
across refresh-cell scope, refresh-row identities and cell-scope suites. Tests
execute the real enrichment orchestration, including formula-to-AI transitive
recomputation, preservation of unrelated fresh values, and new-row formula,
HTTP, agent and research initialization. External provider/source boundaries use
owned stubs; no paid provider or real output destination was called. Existing
linked/unlinked row, failed/complete overlay, no-op and no-dispatch controls pass.
`git diff --check` passed. Raw logs remain local.
