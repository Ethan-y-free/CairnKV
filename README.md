# CairnKV

Raft-backed、强一致的 KV 中间件。

- 线性一致写；读支持 `kLinearizable`（ReadIndex）与 `kStale`
- epoll 多 reactor + 单 Raft core；WAL 持久化 + RocksDB 状态机
- 可复用客户端 SDK：`libcairnkv`

Current release: `v0.3.0`

## Scope

- 单 Raft group：不做 sharding / multi-Raft。横向写扩展需要 multi-Raft，本项目专注单 group 的共识正确性与可验证性。
- Snapshot、lease read、动态成员变更属于 future work。
- 验证环境为本地多进程集群 + 故障注入，不依赖多机部署。

## 当前状态

**W1 完整实现（单节点）**：

- epoll 多 reactor 网络层（Acceptor + N IO loops + non-blocking + ET）
- 二进制 RPC 帧（magic / version / request_id / length / CRC32）
- WAL：append / fsync / 重启恢复 / torn tail 截断 / TruncateSuffix
- RocksDB state machine：Put / Delete / Get / Scan / last_applied 原子写
- 单节点 KVService：Put / Get / Delete / Scan / Status
- 阻塞式 C++ client SDK + `cairnctl`
- GTest：encoding / WAL / hard state / state machine / KVService

**W2 完整实现（Raft core）**：

- Raft election：随机 election timeout、RequestVote、term/vote 持久化后再回复
- AppendEntries：`prevLogIndex/prevLogTerm` 校验、冲突 suffix 截断、fsync 后再 success
- commit rule：只提交当前 term 且多数派 match 的条目；leader 上台追加 no-op
- leader redirect：follower 返回 leader 地址，client 自动重连并重试
- peer transport：每 peer 一个发送线程 + 阻塞 socket，Raft core 不做网络 I/O
- Raft core：单线程任务队列，所有 Raft 状态只在一个线程里修改
- GTest：RaftMessage 编解码、单节点选举/重启恢复/删除

**W3 完整实现**：

- `Get` 默认走 **ReadIndex**：leader 确认 current-term no-op 已提交 → 记录 readIndex → 向多数派发 heartbeat → 等 `lastApplied >= readIndex` 后再读
- `kStale` 读：任意节点本地读，返回 `applied_index`，不保证最新
- 读请求在 leader 未 ready 时排队，step down 时统一失败并携带 leader hint
- 测试专用 partition 钩子：`cairnctl partition <ids>` / `unpartition`，同时丢弃出入向 Raft traffic
- `scripts/partition-smoke.sh`：隔离 leader，验证 linearizable read 失败、stale read 成功、多数派选新 leader、恢复后收敛

## 依赖

- Linux（epoll、signalfd、eventfd）
- g++ 12+ 或 clang 15+
- CMake 3.20+
- RocksDB（`librocksdb-dev`）
- GoogleTest（`libgtest-dev` 或已安装的 GTest CMake package）

Ubuntu/Debian：

```bash
sudo apt install build-essential cmake librocksdb-dev libgtest-dev
```

## 构建

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

## 测试

```bash
ctest --test-dir build --output-on-failure
```

预期：`encoding_test`、`wal_test`、`hard_state_test`、`state_machine_test`、`kv_service_test`、`raft_message_test`、`raft_node_test` 全部通过。

## 运行

```bash
mkdir -p data/node-1
./build/cairnkd --id node-1 --listen 127.0.0.1:6666 --data-dir data/node-1 --io-threads 4
```

另开终端：

```bash
./build/cairnctl --addr 127.0.0.1:6666 put user:1 alice
./build/cairnctl --addr 127.0.0.1:6666 get user:1
./build/cairnctl --addr 127.0.0.1:6666 scan user:
./build/cairnctl --addr 127.0.0.1:6666 status
./build/cairnctl --addr 127.0.0.1:6666 delete user:1
```

单进程模式也会走完整的 Raft 选举，`status` 会在约 150–300ms 后显示 `role=leader`。重启 `cairnkd` 后数据仍在；删除 `data/node-1/state` 后，RaftNode 会从 `raft.log` 重放。

`cairnctl --stale get` 会走本地读；不加 `--stale` 时默认 ReadIndex 线性一致读。

### 多节点集群

脚本在本地启动多个进程，用于验证选主、复制、follower redirect 和故障切换。

```bash
./scripts/run-cluster.sh
./scripts/cluster-smoke.sh

# 读一致性 / 分区：
./build/tools/cairnctl/cairnctl --addr 127.0.0.1:6667 get smoke:key
./build/tools/cairnctl/cairnctl --addr 127.0.0.1:6667 --stale get smoke:key
./scripts/partition-smoke.sh

./scripts/stop-cluster.sh
```

脚本默认使用 `build/cairnkd` 和 `build/tools/cairnctl/cairnctl`；如果 VS 的构建目录不同：

```bash
BIN=/path/to/cairnkd CTL=/path/to/cairnctl ./scripts/cluster-smoke.sh
```

客户端连到 follower 会自动重定向到 leader；`cairnctl status` 直连某个节点时显示该节点自身的角色，方便观察 follower。

## 数据布局

```
data/node-1/raft.log   # WAL: [payload_len][crc32][payload]
data/node-1/state/     # RocksDB state machine
```

## 文档

- [总体设计 v2](DESIGN-draft-v2.md)
- [中间件边界](MIDDLEWARE-BOUNDARY.md)
- [CHANGELOG](CHANGELOG.md)
- [Release notes](docs/releases/)

## 目录

```
include/cairnkv/   公共 API：status / types / client
src/common/        Status / encoding / logging
src/net/           epoll event loop / acceptor / connection / tcp server
src/rpc/           frame / message
src/raft/          log entry / WAL / hard state / RaftMessage / RaftNode / PeerClient
src/storage/       RocksDB state machine
src/consensus/     RPC server adapter（client dispatch + Raft message dispatch）
src/server/        kv_service（W1 单节点参考实现）/ cairnkd main
src/client/        libcairnkv
tools/cairnctl/    CLI
scripts/           run-cluster / stop-cluster / cluster-smoke / partition-smoke
tests/             unit tests
```
