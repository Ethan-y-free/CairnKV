# CairnKV W5 Case Study — Shipping v1.0

W5 was the stabilization week: no new Raft behavior, only hardening,
documentation, and the release decision. This note records what was measured,
what was rejected, and why v1.0 is safe to pin.

## 1. What v1.0 contains

- epoll multi-reactor network layer with a single-threaded Raft core.
- Election, AppendEntries replication, conflict truncation, WAL group commit,
  per-peer in-flight replication windows.
- ReadIndex linearizable reads (default) and stale reads with `applied_index`.
- Leader redirect in `libcairnkv`; `cairnctl` partition hooks.
- Per-second Raft metrics and a closed-loop benchmark harness.
- Fault tests: leader kill, isolated leader, minority partition, rejoin.

Snapshot / InstallSnapshot was deliberately left out: the design marks it as a
stretch item, and v1.0 needed a frozen API more than a larger feature surface.

## 2. Fault model and test matrix

| Failure | Expected behavior | Test |
|---|---|---|
| Leader killed | New leader in 1–2 s; committed data survives | `cluster-smoke.sh` |
| Leader isolated | No linearizable reads; stale read returns local value | `partition-smoke.sh` |
| Minority partition | No commit; majority continues | `partition-smoke.sh` |
| Rejoin | `commit_index` / `last_applied` converge | `partition-smoke.sh` |
| Restart | WAL replay restores state | `raft_node_test`, TSan/ASan runs |
| torn tail | Truncated on recovery; committed data intact | `wal_test` |

The acceptance rule is "committed means durable on a majority", not "the
client saw a response": a follower only acknowledges after its WAL fsync, and
the leader only commits after a durable majority.

## 3. Performance: what changed and what was rejected

v0.4.2 delivered the last shipped optimization: WAL group commit plus a
per-peer replication pipeline. Reference numbers on the original 2 vCPU VM:

- cluster Put 8t/32t: 114/125 → **1894/1901 ops/s**, p99 132/452 ms → 6.5/20.9 ms
- single-node Put 8t/32t: 1532/1796 → 3574/3475 ops/s

W5 explored follower-side asynchronous group-commit acknowledgments (W5-1).
The implementation was correct (unit tests for "no ack before fsync",
heartbeat `match_index` capping, retransmission-triggered sync, and fsync
failure) and passed ctest + cluster/partition smoke. But a same-session A/B
against v0.4.2 showed:

| Scenario | v0.4.2 | W5-1 | Delta |
|---|---:|---:|---:|
| cluster put-8t | 578.5 | 518.2 | -10.4% |
| cluster put-32t | 589.6 | 502.5 | -14.8% |
| single put-8t | 1164.5 | 1132.1 | -2.8% |
| cluster stale-get | 2793 | 2840 | +1.7% |

The environment had drifted: `fdatasync` was ~1120 µs on that VM state, versus
~500 µs when the v0.4.2 baseline was recorded. Under closed-loop load the
leader produces roughly one proposal every 5 ms, so the four in-flight slots
never fill; each AppendEntries carries ~1 entry and there is no batch to
coalesce. The extra core → WAL → core → I/O hops therefore cost more than the
group commit could save. The change was reverted; the patch is archived.

**Lesson:** performance claims require a same-session A/B. Historical absolute
numbers are not a baseline when the host or disk state can change.

## 4. Deliberately not built

- **Snapshot / InstallSnapshot**: would add log compaction, snapshot transport,
  and a new failure mode (lagging follower below the log base) in the release
  week. Stretch item, deferred.
- **Lease read / follower read**: correctness surface not justified for v1.0.
- **Multi-Raft / sharding**: out of scope; the API is single-group by design.
- **Dynamic membership**: operational complexity without a consumer yet.

## 5. v1.0 API contract

CairnShop can rely on:

- `Client::Put/Get/Scan/Delete/GetStatus` signatures and semantics.
- `ReadOptions{mode = kLinearizable | kStale}` and `applied_index`.
- leader redirect behavior: `kNotLeader` carries the leader address.
- on-disk formats: `raft.log`, `hard_state`, RocksDB state directory.

Breaking any of these requires a major version bump.
