# Changelog

## v0.3.0 — ReadIndex & Partition Safety

### Added

- ReadIndex-based linearizable `Get` as the default read path.
- Explicit stale reads via `ReadOptions{ReadMode::kStale}`; the response includes `applied_index`.
- Test/ops partition hook (`cairnctl partition <ids>` / `unpartition`) that drops Raft traffic in both directions.
- `scripts/partition-smoke.sh` for minority/majority and rejoin validation.

### Correctness

- A leader serves ReadIndex only after committing a current-term no-op.
- Reads wait for `lastApplied >= readIndex`; pending reads fail with a leader hint on step-down.
- An isolated old leader rejects linearizable reads; stale reads remain local and explicitly best-effort.
- A majority partition elects a new leader and continues; after rejoin, log and apply indexes converge.

### Validation

- Build: 51/51.
- `ctest`: 7/7.
- Cluster smoke: kill leader, re-election, committed data retained, follower redirect works.
- Partition smoke: isolated leader stale read succeeds, linearizable read times out, new leader serves, rejoin converges.

## v0.2.0 — Raft Core

### Added

- Leader election, AppendEntries, current-term commit rule, and leader no-op.
- Per-peer transport, a single Raft core thread, and a request pool off the I/O threads.
- Leader redirect with client reconnect/retry; cluster scripts and Raft tests.

### Validation

- Build: 51/51.
- `ctest`: 7/7.
- Cluster smoke: kill leader, re-election, committed data retained.
- Follower redirect: put/get through a follower reaches the leader.
