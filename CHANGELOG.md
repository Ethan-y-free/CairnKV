# Changelog

## v0.4.1 — Benchmark Baseline & Raft Metrics

### Added

- Closed-loop benchmark client (`tools/cairnkv-bench`) with warmup/measurement barriers, p50–p999 latency, error classification, and server snapshots.
- `scripts/bench.sh` single/cluster matrix; raw JSON and per-node logs are archived under `docs/benchmarks/results/`.
- Per-second Raft metrics: proposals, fsync count/latency, apply count/latency, AppendEntries sends/responses/rejections, and per-peer `next`/`match`/queue depth.
- `docs/benchmarks/v0.4-baseline.md` with the measured baseline and bottleneck analysis.

### Fixed

- Heartbeat ping-pong: an empty AppendEntries response no longer triggers another AppendEntries when the follower is caught up.
- `LogStore::Append` now checks the `ftruncate` result when rolling back a partial record.

### Validation

- Build: 54/54 (Debug and Release).
- `ctest`: 7/7.
- Cluster smoke and partition smoke: passed.
- Baseline (2 vCPU AMD Ryzen 9 8945HX VM, ext4, RocksDB 8.9.1):
  - single-node Put 1176 / 1532 / 1796 ops/s at 1 / 8 / 32 threads; `sync_calls : proposals` = 1:1, mean fsync 500 µs.
  - cluster Put 513 ops/s at 1 thread; 8/32 threads collapse to 114/125 ops/s, traced to ~100× redundant AppendEntries from per-proposal broadcasts and a lossy bounded send queue.

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
