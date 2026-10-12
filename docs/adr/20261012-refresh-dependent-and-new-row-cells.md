# Refresh dependent cells and initialize sourced rows

## Context

PR #74 restricts standing refresh to stale enrichment cells per row. The strict
allowlist preserved unrelated fresh values but also excluded fresh downstream
cells whose inputs changed, and left newly sourced rows' formula, HTTP, agent
and research cells uninitialized.

## Decision

Expand each existing row's stale-cell scope through the existing transitive
`downstream_columns` helper, using the row-edit path's reactive eligibility
policy. Snapshot persisted row IDs before sourcing and compare them afterward.
New rows initialize every runnable non-output column. Pass the resulting exact
per-row scopes through the existing enrichment runner.

## Alternatives

Returning added IDs from the source engine requires changing its return contract
and each caller. Running all columns on every stale row would restore dependent
values but discard this PR's cost-saving behavior for unrelated fresh cells.
Running outputs during sourcing would introduce unrequested external dispatch.

## Consequences

Changed inputs propagate through derived columns while unrelated fresh cells
remain intact. Explicitly non-reactive dependents stay excluded; outputs remain
non-reactive by default and new-row initialization excludes outputs. Row-ID
comparison adds two scoped ID reads and identifies rows added during the cycle.
It is not a concurrent-writer snapshot isolation guarantee.

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
