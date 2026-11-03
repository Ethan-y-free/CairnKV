# CairnKV 总体设计（v2）

> 2026/11/02 v2 定稿：并发模型改为 **epoll 多 reactor + 单 Raft core + 组提交 + 流水线**；读一致性默认 **ReadIndex（线性一致）+ STALE 可选**；排期 5 周。
> v1 见 `DESIGN-draft-v1.md`；中间件边界见 `MIDDLEWARE-BOUNDARY.md`。

## 0. 一句话

**3 节点、单 Raft group 的分布式 KV：先把 Raft 的安全性跑对，再用事件驱动架构把单 group 的并发天花板做高。**

## 1. 范围边界

### 必须完成

- 固定 3 节点、单 Raft group；Put / Get / Delete / Scan / Status；
- Leader 选举、心跳、日志复制、commit、apply；
- Raft log + hard state 持久化、重启恢复；
- kill leader、网络分区、torn write；
- 客户端 leader 重定向；
- **线性一致读（ReadIndex）+ 显式 STALE 读**；
- **事件驱动网络层 + 单 Raft core + WAL 组提交 + 复制流水线**；
- 单测、集成、混沌、压测、文档。

### 不做 / stretch

| 项 | 处理 |
|---|---|
| 分片 / multi-Raft | 不做；横向写扩展的唯一正解，但超出 5 周 |
| 动态成员变更 | 不做 |
| 事务 / 多 key 原子操作 | 不做 |
| Lease read | stretch（v1.x） |
| Follower ReadIndex | v1.x |
| Watch | v1.x |
| TLS / 认证 / 监控 | 不做；可信内网 |
| 多机部署 | 单机 3 进程 |
| Snapshot / InstallSnapshot | stretch |

### v1 → v2 的变化

| 项 | v1 | v2 |
|---|---|---|
| 网络 / 线程 | 阻塞 socket + 每连接一线程 | epoll 多 reactor + 非阻塞 |
| Raft 状态 | 一把 mutex | 单 Raft core 线程，无锁 |
| fsync | 每条日志 fsync | WAL 组提交 |
| 复制 | 一次一批 | 多批流水线 |
| 读 | leader-only，可能旧 | ReadIndex 线性一致 + STALE 可选 |
| 排期 | 4 周 | 5 周 |

## 2. 总体架构

```
Client ──TCP──▶ Acceptor
                    │
      ┌─────────────┼─────────────┐
      ▼             ▼             ▼
 IO Reactor 0  IO Reactor 1  IO Reactor N   (epoll + 非阻塞)
      │             │             │
      └──────┬──────┴──────┬──────┘
             │ MPSC 队列    │
             ▼             ▼
      ┌─────────────────────────────┐
      │   Raft Core（1 线程，无锁）  │
      └───┬─────────┬─────────┬─────┘
          │         │         │
          ▼         ▼         ▼
    WAL writer   Apply     出站队列 → IO Reactor
    (组提交)     线程        Read pool ← 读请求
```

分层：

- **net**：Acceptor、IO Reactor、Connection、Buffer、背压；
- **rpc**：二进制帧编解码、RPC Server/Client；
- **raft**：LogEntry、LogStore、HardState、RaftNode、ReadIndex；
- **storage**：StateMachine、ApplyRunner、ReadPool；
- **server**：cairnkd 入口、配置、指标；
- **client**：libcairnkv SDK。

## 3. 模块划分

```
include/cairnkv/          # 公共 API：client.h / status.h / options.h
src/
├── common/               # Status、Config、Logging、encoding
├── net/                  # EventLoop、Acceptor、Connection、Buffer
├── rpc/                  # frame codec、rpc_server、rpc_client
├── raft/                 # log_entry、log_store、hard_state、raft_node、read_index
├── storage/              # state_machine、apply_runner、read_pool
├── server/               # kv_service、node_main
└── client/               # libcairnkv
tools/cairnctl/
tests/unit/ tests/integration/
```

### 关键接口

```cpp
class LogStore
{
public:
    Status Append(const LogEntry& entry);
    StatusOr<LogEntry> Get(uint64_t index) const;
    uint64_t LastIndex() const;
    uint64_t LastTerm() const;
    Status TruncateSuffix(uint64_t from_index);
    Status Sync();
};

class StateMachine
{
public:
    Status Apply(const LogEntry& entry);      // 确定性
    StatusOr<std::string> Get(const std::string& key);
    uint64_t LastApplied() const;
};

class RaftNode
{
public:
    Status Start();
    StatusOr<std::string> Propose(const Command& cmd); // 写
    StatusOr<GetResult> Get(const std::string& key, ReadOptions opts);
    void OnMessage(const RpcMessage& msg);
};
```

## 4. Raft 核心

### 4.1 状态

| 类型 | 字段 | 是否持久化 |
|---|---|---|
| Persistent | `currentTerm` | ✅ hard state |
| Persistent | `votedFor` | ✅ hard state |
| Persistent | `log[]` | ✅ WAL |
| Volatile | `commitIndex` | ❌（可重建） |
| Volatile | `lastApplied` | ✅ 跟状态机一起存 |
| Volatile | `state`（Follower/Candidate/Leader） | ❌ |
| Volatile | `leaderId` | ❌ |
| Volatile | `nextIndex[]`、`matchIndex[]` | ❌（leader 重启后重新初始化） |

> 规则：**凡是"对别人做过的承诺"或"别人已经依赖的数据"必须落盘；凡是能重新推导的都放内存。**

### 4.2 消息

| 消息 | 字段 |
|---|---|
| `RequestVote` | term, candidateId, lastLogIndex, lastLogTerm |
| `RequestVoteResp` | term, voteGranted |
| `AppendEntries` | term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit |
| `AppendEntriesResp` | term, success, matchIndex |
| `ClientRequest` | op, key, value, requestId, readMode |
| `ClientResponse` | ok, value, leaderHint, appliedIndex, readIndex |
| `InstallSnapshot` | stretch |

### 4.3 选举

```
Follower 等待心跳
  → 超时（150–300ms 随机）
  → Candidate: term++, 投自己, 发 RequestVote
  → 多数票 → Leader
  → 否则退回 Follower / 重新选
```

投票条件：

1. 候选人 term ≥ 自己 term；
2. 本 term 没投过别人；
3. 候选人日志至少和自己一样新（先比 lastLogTerm，再比 lastLogIndex）。

成为 Leader 后：

- 初始化 `nextIndex[peer] = lastLogIndex + 1`；
- 初始化 `matchIndex[peer] = 0`；
- **追加一条当前 term 的 no-op**；
- 立即发心跳。

> no-op 有两个作用：安全推进 commitIndex；让新 leader 在服务 ReadIndex 读之前确认自己知道最新 commitIndex。

### 4.4 日志复制

**Leader 收到写：**

```
1. 本地 AppendLog({index, term, cmd})
2. 向每个 follower 发 AppendEntries（多批流水线，不等本地 fsync）
3. 本地 durable + 多数 success → 更新 matchIndex
   → 若 entry.term == currentTerm 且多数 → commitIndex = index
4. apply 到状态机
5. 下一次 AppendEntries 把 leaderCommit 带给 follower
```

**Follower 收到 AppendEntries：**

```
1. term < currentTerm → 拒绝，返回当前 term
2. prevLogIndex/prevLogTerm 不匹配 → 拒绝
3. 删除冲突 suffix
4. 追加新 entries
5. 组提交 fsync
6. 更新 commitIndex = min(leaderCommit, lastNewIndex)
7. 按序 apply
8. 回复 success(matchIndex)
```

### 4.5 commit 规则

```cpp
bool CanCommit(uint64_t index)
{
    return log_[index].term == current_term_ &&
           match_count(index) >= majority();
}
```

旧 term 的日志只能**间接提交**：等当前 term 的 no-op 或新日志提交后，前面的旧日志才算提交。

原因：旧 leader 可能把旧 term 条目复制到多数派但没提交就崩溃；新 leader 仍可能当选并覆盖它。只有"当前 term 条目多数匹配"才能保证未来 leader 都包含它，并连带锁定它之前的日志。

### 4.6 持久化顺序

**回复 AppendEntries 前：**

```
1. 写 log entries
2. fsync log
3. 更新 currentTerm/votedFor（如变了）
4. fsync hard state
5. 回复 success
```

**回复 RequestVote 前：**

```
1. 更新 currentTerm/votedFor
2. fsync hard state
3. 回复 voteGranted
```

顺序不能反。**先回复再落盘 = 向别人撒谎**：断电后忘了承诺，会一 term 两投或丢失已承诺日志。

## 5. 存储设计

### 5.1 Raft Log WAL

```
record:
  [u32 payload_len][u32 crc32][payload]

payload:
  magic "RLG1"
  version u8
  type u8        # Noop / Put / Delete
  index u64
  term u64
  key_len u32 + key
  value_len u32 + value
```

恢复：

- 文件末尾不足 8 字节 → 截断；
- `payload_len` 超过剩余字节 → 截断；
- CRC 不匹配且在最后一条 → 截断（torn write）；
- CRC 不匹配且在中间 → 报错，不静默跳过。

### 5.2 Hard State

```cpp
struct HardState
{
    uint64_t current_term;
    std::string voted_for;   // 空表示没投
};
```

写入：`写 hard_state.tmp → fsync → rename → fsync(dir)`。

### 5.3 KV 状态机

```
user key            → value
__meta/last_applied → uint64
```

Apply：

```cpp
rocksdb::WriteBatch batch;
// Put / Delete
batch.Put("__meta/last_applied", index);
db->Write(batch);
```

`last_applied` 和数据在**同一个 WriteBatch**，保证"数据改了、进度也改了"原子发生。

### 5.4 启动恢复

```
1. 读 hard state
2. 扫 WAL，校验 length/CRC，截断 torn tail
3. 恢复 log 到内存
4. 从 RocksDB 读 lastApplied
5. lastApplied 之后的日志不能自行 apply
6. 进入 Follower，等待心跳/选举
```

> commitIndex 不持久化，重启后本地无法知道哪些条目已提交。状态机停在 lastApplied；之后的条目等新 leader 的 `leaderCommit` 推进，或自己当选后由当前 term 的 no-op 带上来。

### 5.5 WAL 组提交

- 单 WAL writer 线程；
- 攒一批 append → `writev` → `fdatasync` → 通知 core `durable_index`；
- core 可以**先发 AppendEntries、后等本地 durable**，网络和磁盘并行；
- 只有 `durable_index >= index` 且多数派 match 才能 commit；
- follower 同样组提交，fsync 后才能回 success。

### 5.6 Snapshot（stretch）

```
Snapshot:
  magic + version
  lastIncludedIndex
  lastIncludedTerm
  kv_count
  [key_len][key][value_len][value] * N
  crc32
```

触发：WAL 条目数或字节数超阈值。安装时先写临时文件、校验、原子替换，再截断日志。

## 6. 网络与 RPC

### 6.1 帧格式

```
magic "RRPC" (4)
version u8
type u8
request_id u64
payload_len u32
payload
crc32 u32
```

### 6.2 多 reactor

- 1 个 Acceptor + N 个 IO Reactor（每线程一个 epoll）；
- 非阻塞 socket，EPOLLET；
- 每连接读缓冲/写队列，处理半包与写不完；
- 连接只属于一个 IO 线程，无锁；
- 出站消息通过 per-peer 队列交给对应 IO 线程。

### 6.3 背压

- 每连接最大 in-flight 请求数；
- 每连接最大写缓冲；
- 全局/核心队列有界；
- 超限返回 `BUSY` 或暂停读（EPOLLIN off）；
- 绝不无界堆积，防止 OOM。

### 6.4 幂等与超时

- Put / Delete 本身幂等；
- `requestId` 保留，为以后去重；
- 客户端超时重试在 KV 语义下安全；
- **写超时不等于失败**：客户端应查询确认，不盲目重试。

### 6.5 校验

所有 handler 先校验 magic/version/length/CRC，再分配内存；长度字段不可信。

## 7. 客户端与读 API

### 7.1 API

```cpp
namespace cairnkv
{

enum class ReadMode
{
    kLinearizable,
    kStale
};

struct ReadOptions
{
    ReadMode mode = ReadMode::kLinearizable;
};

struct GetResult
{
    std::string value;
    uint64_t applied_index = 0;
};

class Client
{
public:
    static StatusOr<std::unique_ptr<Client>> Open(const ClientOptions& opts);

    Status Put(const std::string& key, const std::string& value);
    StatusOr<GetResult> Get(const std::string& key, ReadOptions opts = {});
    Status Delete(const std::string& key);

    StatusOr<std::vector<KeyValue>> Scan(const std::string& prefix,
                                         const ScanOptions& opts);
    StatusOr<ClusterStatus> GetStatus();
};

}
```

### 7.2 重定向与重试

```
连接任意节点
  → 写：不是 leader 就返回 NOT_LEADER + hint → 重连 leader
  → 线性一致读：同上
  → STALE 读：任意节点本地读，不回 leader
  → 超时/失败：指数退避；写请求先确认再重试
```

### 7.3 一致性边界

- 写：多数派提交 + apply 后返回；
- 线性一致读：ReadIndex，保证读到最新已提交值；
- STALE 读：可能旧，返回 `applied_index`；
- 分区期间旧 leader 的线性一致读会被多数派拒绝，不会返回旧值；
- 文档写清每种模式的边界。

## 8. 并发模型

### 8.1 所有权

1. 连接属于且仅属于一个 IO 线程；
2. Raft 状态属于且仅属于 Raft core；
3. WAL writer、Apply、ReadPool 各自拥有资源；
4. 跨线程只传消息，不共享可变状态。

### 8.2 Raft core

- 单线程事件循环，从 MPSC 队列取事件；
- 事件：客户端请求、peer 消息、定时器、WAL durable 通知、apply 完成；
- 无锁；批量处理；出站消息批量入队；
- **绝不在 core 里做 fsync、磁盘读、网络写**。

### 8.3 写路径

```
IO 线程解析 → core 追加内存日志 → WAL writer 组提交
core 同时发 AppendEntries（流水线）→ 多数派 ack + 本地 durable
→ commitIndex 推进 → Apply 线程批量写 RocksDB → 完成响应
```

### 8.4 复制流水线

- 每个 follower 允许多批 in-flight（窗口 8–16）；
- ack 带 matchIndex，只前进不后退；
- 失败回退 `nextIndex` 重试；
- 吞吐 ≈ 窗口 / RTT。

### 8.5 Apply

- 单线程、严格按 index 顺序；
- 多条 entries 合并成一个 WriteBatch；
- RocksDB 不逐条 fsync（Raft log 是真相，重启可重放）；
- 完成后发布 `lastApplied` 与可读快照。

### 8.6 读

- 线性一致读：core 做 ReadIndex；满足后交给 ReadPool；
- ReadPool 用 RocksDB snapshot 并发读；
- STALE 读：IO 线程可直接交给 ReadPool（需检查本地 role/lease 由配置决定）；
- 读不阻塞写，写不阻塞读。

### 8.7 定时器

- core 使用 `epoll_wait` 超时或 timerfd 检查选举/心跳 deadline；
- 不在 IO 线程做 Raft 定时逻辑。

### 8.8 性能目标（基线，非承诺）

单机 3 进程、64B value、256 并发、pipeline 16：

- 写 ≥ 10k ops/s；
- 读 ≥ 50k ops/s；
- p50/p99 记录实测；
- 用 perf / flamegraph 找瓶颈，报告写入 CASE-STUDY。

> 硬件不同，数字只做同环境对比；横向写扩展 = multi-Raft，明确不做。

## 9. 读一致性

### 9.1 定义

**线性一致读**：如果一次写已经返回成功，那么之后开始的任何读，都必须看到这次写或更新的值。

### 9.2 旧 leader 的失败场景

```
t1  A 是 leader，客户端写 k=1，A 多数派提交并返回 OK
t2  A 与 B/C 分区；B/C 选 D 为新 leader，客户端写 k=2，D 提交并返回 OK
t3  客户端向 A 读 k：
    - 朴素 leader-only：A 还以为自己是 leader，返回 k=1  ← 读到旧值
    - ReadIndex：A 向多数派确认，B/C 回"你 term 旧了"，A 下台，
      客户端重定向到 D，读到 k=2
```

### 9.3 模式

| 模式 | 保证 | 做法 | 延迟 | 代价 |
|---|---|---|---|---|
| `kLinearizable` | ✅ 线性一致 | ReadIndex + 等 apply | 一次心跳 RTT（可批量） | 实现复杂 |
| `kStale` | ❌ 可能旧 | 本地快照读 | 最低 | 返回 applied_index |
| Lease read（stretch） | ✅（时钟假设） | 租约内本地读 | 最低 | 依赖时钟漂移有界 |
| Follower ReadIndex（v1.x） | ✅ | follower 向 leader 取 readIndex | 多一跳 | 实现复杂 |

### 9.4 ReadIndex 步骤

```
1. leader 确认已提交当前 term 的 no-op（否则可能不知道最新 commitIndex）
2. readIndex = commitIndex
3. 向所有 follower 发一轮心跳
4. 多数派在当前 term 回复 → 确认仍是 leader
5. 等 lastApplied >= readIndex
6. 从状态机快照读
```

多个读可共用一轮心跳；term 变化则失败重定向。

### 9.5 STALE 语义

- 任意节点本地读，不做多数派确认；
- 可能读到旧值；
- 返回 `applied_index`，调用方自行判断新鲜度；
- 适合缓存刷新、监控、展示类读；不适合扣款、库存等关键读。

### 9.6 与业务层的关系

端到端一致性 = 所有层里最弱的一环。CairnShop 有 500ms 配置缓存，即使 CairnKV 线性一致，业务读到的新鲜度仍由缓存决定；关键路径要么绕过缓存，要么 fail closed。

## 10. 失败语义

| 场景 | 行为 |
|---|---|
| Follower 宕机 | 多数派仍在，服务继续；重启后追日志 |
| Leader 宕机 | 1–2s 内重新选主；已 commit 数据不丢 |
| 分区·少数派 | 不能选主、不能提交；线性一致读被拒绝 |
| 分区·多数派 | 继续服务 |
| 分区恢复 | 日志收敛，少数派被覆盖/追上 |
| 断电/torn write | WAL 截断尾部；已 commit 数据不丢 |
| 客户端超时重试 | Put/Delete 幂等；先确认再重试 |
| 队列满/过载 | 返回 BUSY；背压保护，不 OOM |
| leader 切换中的读 | 线性一致读失败/重定向；STALE 读可能旧 |
| WAL 组提交未完成 | 不回复 success；重试或超时 |

## 11. 测试与压测

### 单元

- Encode/Decode round-trip；坏 magic/version/长度/CRC；
- WAL torn tail 截断；
- 选举状态转换；
- commitIndex 只推进当前 term；
- 日志落后的节点不能当选；
- ReadIndex：旧 leader 被拒、no-op 未提交不服务读。

### 集成 / 混沌

- 3 节点选主、复制、kill leader、重启、分区；
- 压测中 kill leader：0 失败写、读一致性正确；
- 组提交崩溃注入：不回复未 fsync 的写；
- 流水线乱序 ack；
- 背压：队列满时 BUSY，不 OOM。

### 并发正确性

- TSan / ASan；
- 高并发压测 + 超时/重试；
- 所有权检查：无跨线程共享可变状态。

### 压测

- 单机 3 进程；64B / 1KB value；256 并发；pipeline 8/16；
- 输出 ops/s、p50/p99、CPU、fsync 批量大小；
- 同环境对比，不夸大。

## 12. 5 周排期

| 周 | 目标 | 交付 |
|---|---|---|
| W1 | 事件驱动网络层 + 二进制帧 + WAL 组提交 + 单机 KV | 单机 put/get/重启不丢 |
| W2 | Raft core + 选举 + 复制 + 流水线 + 3 节点 | 3 节点选主/复制/commit |
| W3 | ReadIndex + 重启恢复 + kill leader + 分区 | 线性一致读 + 故障测试 |
| W4 | SDK / CLI / metrics + 压测调优 | v1.0 可被 CairnShop 依赖 |
| W5 | 加固、文档、CASE-STUDY、缓冲 | 发布 v1.0 |

如果 W2 末选举不稳：**砍 snapshot、砍 lease read、砍 follower read，保选举/复制/恢复/ReadIndex**。

## 13. 决策

| 决策 | 结论 |
|---|---|
| RPC / 线程模型 | **epoll 多 reactor + 单 Raft core + 组提交 + 流水线** |
| 读一致性 | **默认 ReadIndex（线性一致）+ STALE 可选**；lease read stretch |
| Snapshot | stretch，不阻塞主线 |
| 存储 | Raft log 自研 WAL + KV 用 RocksDB |
| 排期 | **5 周**；CairnShop W6–W9 |

## 14. 下一步

1. 回答 3 个 Raft 问题（理解验收）：
   - 为什么 leader 不能直接提交旧 term 的日志？
   - AppendEntries 为什么需要 `prevLogIndex/prevLogTerm`？
   - 收到 AppendEntries 后，持久化和回复的顺序是什么？
2. 进入 Week 1：仓库骨架、common 层、事件循环、WAL。

## 15. 与 CairnShop 的边界

CairnKV 保持业务无关：只提供 KV、Scan、Status、ReadOptions；管理面、历史/回滚、审计、MySQL、前端全部归 CairnShop。CairnShop 通过 `libcairnkv` 固定 tag 依赖 CairnKV，作为真实消费者验证可行性。
