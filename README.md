# CairnKV

Raft-backed KV middleware in C++20: linearizable writes, ReadIndex reads,
WAL durability, and a reusable client SDK (`libcairnkv`).

**v1.0.0** — API / wire protocol / on-disk formats frozen.

---

## 定位

CairnKV 是一个可复用的**强一致 KV 中间件**，不是业务数据库：它只负责
Raft 共识、日志持久化、状态机复制和客户端会话语义，业务表、缓存、审计
留给上层。

当前版本专注于**单 Raft group 的共识正确性与可验证性**：

- 3 节点集群；客户端可从任一节点接入（自动重定向 leader），写由 leader 串行化；
- 线性一致 `Put/Delete`；`Get` 默认 ReadIndex 线性一致，`kStale` 可选；
- 崩溃恢复、leader 故障切换、网络分区均可本地复现；
- 单 group 之上不引入分片，multi-Raft 是后续演进方向，不混进 v1.0 的
  正确性边界。

## 架构

```
        ┌──────────────┐        ┌──────────────┐
        │  cairnctl    │        │ libcairnkv   │
        └──────┬───────┘        └──────┬───────┘
               │  TCP + binary frame   │
               ▼                       ▼
        ┌──────────────────────────────────────┐
        │              cairnkd                 │
        │  epoll I/O loops ──► RPC server      │
        │        │                 │           │
        │        ▼                 ▼           │
        │   peer transport   single Raft core  │
        │        │                 │           │
        │        ▼                 ▼           │
        │   WAL group commit   RocksDB state   │
        └──────────────────────────────────────┘
               │                     │
               ▼                     ▼
          other cairnkd          raft.log + state/
```

- **网络层**：epoll 多 reactor、非阻塞 socket、二进制帧（magic / version /
  request_id / length / CRC32）。
- **Raft core**：单线程任务队列；所有 term / vote / log / commit 状态只在
  这一个线程修改，避免共识层锁竞争。
- **WAL**：独立组提交线程，`fdatasync` 按批合并；只有 durable 的条目才能
  被 majority commit。
- **状态机**：RocksDB；`last_applied` 与数据在同一个 WriteBatch 原子写入。
- **复制**：每 peer 最多 4 个 in-flight 批次，`next_index` 乐观推进，
  `match_index` 只接受已确认响应；1s 超时回退重传。
- **客户端**：阻塞式 C++ SDK，自动处理 `kNotLeader` 重定向与重连。

## 一致性模型

**写路径**

1. client 请求进入 leader；leader 追加日志并交给 WAL 线程；
2. WAL `fdatasync` 完成前，该条目不会被算作 durable；
3. leader 向 follower 流水线复制；follower 先 append、再 fsync、最后回 success；
4. 当前 term 且 majority `match_index` 覆盖的条目才推进 `commit_index`；
5. apply 到 RocksDB 后才向 client 返回。

**读路径**

- `ReadIndex`（默认）：leader 确认 current-term no-op 已提交 → 记录
  `read_index` → 向多数派发 heartbeat → 等 `last_applied >= read_index`
  再读。旧 leader 在分区期间不会返回线性一致结果。
- `kStale`：任意节点本地读，返回 `applied_index`，不保证最新；用于后台
  缓存、运营看板等可容忍陈旧的场景。

**故障语义**

| 场景 | 行为 |
|---|---|
| leader 挂掉 | 1–2s 内重新选主；已 commit 数据不丢 |
| leader 被隔离 | 线性一致读超时/失败；stale 读仍可返回本地值 |
| 少数派分区 | 无法 commit；多数派继续服务 |
| 分区恢复 | 日志收敛，`commit_index` / `last_applied` 对齐 |
| 进程重启 | WAL 重放；torn tail 自动截断 |
| follower 落后 | leader 回退 `next_index`，批量重传 |

## 验证

- 单元测试：encoding、WAL（含 torn tail / 截断）、hard state、状态机、
  RaftMessage 编解码、单节点选举/重启/删除。
- 集成测试：`cluster-smoke`（leader kill + failover）、`partition-smoke`
  （隔离 leader、多数派选主、恢复收敛）。
- Sanitizer：TSan 全量 ctest + cluster-smoke + 压测零报告；ASan+UBSan 同样
  零报告。
- 性能工具：`cairnkv-bench` + `scripts/bench.sh`，输出 p50–p999、错误分类
  与服务端快照；原始 JSON 与节点日志归档到 `docs/benchmarks/results/`。

```bash
ctest --test-dir build --output-on-failure
```

## 性能

环境：2 vCPU、ext4、RocksDB 8.9.1、Release、64B value、keyspace 1000。
数据来自 v0.4.2（v1.0 无协议/行为变更）。

| 场景 | 吞吐 | p99 |
|---|---:|---:|
| 单节点 Put 8t / 32t | 3574 / 3475 ops/s | — |
| 3 节点 Put 8t / 32t | **1894 / 1901 ops/s** | **6.5 / 20.9 ms** |
| 3 节点 linearizable Get 8t | 6760 ops/s | — |
| 3 节点 stale Get 8t | 16055 ops/s | — |

相比未优化版本（集群 Put 8t = 114 ops/s，p99 = 132 ms），主要收益来自：

- 消除“每条 proposal 全量广播”的放大（约 100× → 每 peer 每批一次）；
- WAL 组提交，把 fsync 从 core 线程移出；
- 每 peer 流水线 + 有界 in-flight，队列不再静默丢弃。

> 绝对数字与机器/磁盘状态强相关。仓库保留 `scripts/bench.sh`，用于同机
> A/B，而不是拿不同环境的绝对数比较。方法与基线见
> [v0.4 baseline](docs/benchmarks/v0.4-baseline.md)。

## 快速开始

依赖：

```bash
sudo apt install build-essential cmake librocksdb-dev libgtest-dev
```

构建与测试：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
ctest --test-dir build --output-on-failure
```

单节点：

```bash
mkdir -p data/node-1
./build/cairnkd --id node-1 --listen 127.0.0.1:6666 \
  --data-dir data/node-1 --io-threads 4
```

```bash
./build/tools/cairnctl/cairnctl --addr 127.0.0.1:6666 put user:1 alice
./build/tools/cairnctl/cairnctl --addr 127.0.0.1:6666 get user:1
./build/tools/cairnctl/cairnctl --addr 127.0.0.1:6666 scan user:
./build/tools/cairnctl/cairnctl --addr 127.0.0.1:6666 status
```

3 节点集群 + 故障注入：

```bash
# 选主 / 复制 / leader failover（会 kill 掉当时的 leader）
./scripts/run-cluster.sh
./scripts/cluster-smoke.sh
./scripts/stop-cluster.sh

# 分区一致性（必须新起集群）
./scripts/run-cluster.sh
./scripts/partition-smoke.sh
./scripts/stop-cluster.sh
```

SDK：

```cpp
#include "cairnkv/client.h"

cairnkv::ClientOptions options;
options.host = "127.0.0.1";
options.port = 6666;

auto client = cairnkv::Client::Open(options);
if (!client.ok())
{
    return 1;
}

(*client)->Put("user:1", "alice");
auto value = (*client)->Get("user:1", cairnkv::ReadOptions{});
```

完整示例见 [`examples/basic.cc`](examples/basic.cc)。

## 工程结构

```
include/cairnkv/      public SDK（client / status / version）
src/net/              epoll、Channel、EventLoop、Connection
src/rpc/              帧格式与消息编解码
src/raft/             Raft core、WAL、hard state、peer transport
src/storage/          RocksDB state machine
src/consensus/        RPC server、request pool
src/server/           cairnkd 入口
src/client/           SDK 实现
tools/                cairnctl、cairnkv-bench
tests/                GTest
scripts/              cluster / smoke / partition / bench
docs/                 design、case study、benchmark、release notes
```

## 文档

- [设计稿 v2](DESIGN-draft-v2.md)：网络、WAL、Raft、ReadIndex、worker 线程模型
- [W4 case study](docs/case-study-w4.md)：从广播风暴到流水线复制的调优过程
- [W5 case study](docs/case-study-w5.md)：v1.0 收尾、故障矩阵与性能方法论
- [v0.4 基线](docs/benchmarks/v0.4-baseline.md)：压测方法与原始数据
- [v1.0.0 release notes](docs/releases/v1.0.0.md)

## Roadmap

- **Snapshot / InstallSnapshot**：日志压缩与落后节点追赶（下一里程碑）。
- **Lease read / follower read**：降低跨地域读延迟。
- **动态成员变更**：扩缩容不重启集群。
- **Multi-Raft / sharding**：在单 group 正确性稳定后做横向扩展。

## 状态

- `v1.0.0` 已发布：API、wire protocol、on-disk 格式冻结。
