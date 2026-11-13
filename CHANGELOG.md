# Changelog

## v1.0.0 — API Freeze

Milestone release: `libcairnkv` public API, wire protocol, and operational
behavior are frozen so CairnShop can pin this tag.

### Changed

- No wire-protocol or Raft behavior changes relative to v0.4.2. This release
  is the v1.0 API/quality milestone, not a feature drop.
- README rewritten around the v1.0 feature set, validation matrix, and the
  corrected multi-node smoke order.
- Added `docs/releases/v1.0.0.md` and `docs/case-study-w5.md` (fault model,
  benchmark methodology, and rejected optimizations).

### Validation

- Build: 56/56 (Debug; v0.4.2 tree plus documentation-only changes).
- `ctest`: 7/7.
- cluster-smoke + partition-smoke: passed on a fresh cluster.
- Performance: v0.4.2 measurements remain the reference (cluster Put
  1894/1901 ops/s at 8/32 threads on the original 2 vCPU VM). Absolute
  numbers drift with host/disk state; use same-session A/B for comparisons.

### Evaluated and not shipped

- Follower asynchronous group-commit acknowledgments (W5-1): correct and
  fully tested, but same-machine A/B showed -10%/-15% cluster write
  throughput on the current VM (fsync ~1120 µs, closed-loop load leaves the
  replication window under-filled). Patch archived at
  `cairnkv-w5-1-follower-async-ack.patch`; revisit on faster storage or with
  open-loop load.
- Snapshot / InstallSnapshot: still the designated stretch item; deferred to
  post-v1.0. The design and on-disk formats remain compatible.

### Known limits

- Single Raft group; no sharding / multi-Raft.
- No snapshot / InstallSnapshot; the WAL is not compacted.
- Reads: ReadIndex (default) and stale; no lease read / follower read.
- No dynamic membership, TLS, or multi-machine deployment tooling.

## v0.4.2 — Replication Pipelining

### Added

- Per-peer in-flight replication windows: `next_index` advances optimistically, `match_index` only on confirmed responses, up to 4 batches in flight.
- `PeerClient` sender/receiver threads; `Send()` reports a full queue instead of silently dropping the oldest message.
- 1s in-flight retry and explicit rollback when a follower rejects a batch.
- `examples/basic.cc` (libcairnkv walkthrough) and `docs/case-study-w4.md`.

### Changed

- WAL `fdatasync` moved to a dedicated group-commit thread; `commit_index` is bounded by `last_synced_index`.
- Heartbeats are sent only when the replication window has room.

### Fixed

- Data race on `EventLoop::thread_id_` (found by TSan under cluster-smoke).

### Validation

- Build: 54/54; `ctest`: 7/7.
- TSan (`halt_on_error=1`): ctest + cluster-smoke + 5s benchmark, zero reports.
- ASan+UBSan: ctest + 5s benchmark, zero reports.
- Cluster Put 8t/32t: 114/125 -> 1894/1901 ops/s; p99 132/452 ms -> 6.5/20.9 ms.
- Single-node Put 8t/32t: 1532/1796 -> 3574/3475 ops/s.

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
