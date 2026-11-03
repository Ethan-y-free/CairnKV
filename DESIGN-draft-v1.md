# CairnKV 总体设计（v1 草案）

> 2026/10/28 定稿。下次继续时，先把本文件原样重讲一遍，再进入 Week 1。

## 0. 一句话

**3 节点、单 Raft group 的分布式 KV：先把 Raft 的安全性跑对，再谈性能和功能。**

名字：CairnKV。cairn = 石堆路标；每条 Raft 日志是一块石头，commitIndex 是大家都认可的堆顶。

---

## 1. 范围边界

### 必须完成

- 固定 3 节点，单 Raft group；
- `Put / Get / Delete`；
- Leader 选举、心跳、日志复制、commit、apply；
- Raft log + hard state 持久化；
- 重启恢复、kill leader、网络分区；
- 客户端 leader 重定向；
- 单测 + 集成测试 + README/DESIGN + 压测数据。

### 明确不做

| 不做 | 原因 |
|---|---|
| 分片 / multi-Raft | 3–4 周做不完 |
| 动态成员变更 | Raft 进阶，先跳过 |
| 事务 / 多 key 原子操作 | 不是本项目重点 |
| ReadIndex / lease read | 第一版 leader-only reads，文档写明边界 |
| TLS / 认证 / 监控 | 与核心无关 |
| 多机部署 | 单机 3 进程足够 |

Snapshot / InstallSnapshot 是 **stretch**：第 3 周有富余再做，否则写 future work。

---

## 2. 总体架构

```
┌─────────────┐
│  Client CLI │
└──────┬──────┘
       │ TCP，长度前缀 + CRC
       ▼
┌────────────────────────────────────────────┐
│                CairnKV Node                │
│                                            │
│  ┌────────────┐      ┌──────────────────┐  │
│  │ RPC Server │─────▶│    RaftNode      │  │
│  └────────────┘      │  - 选举           │  │
│                      │  - 日志复制       │  │
│                      │  - commit/apply   │  │
│                      └───┬──────────┬───┘  │
│                          │          │      │
│                 ┌────────▼───┐  ┌───▼────┐ │
│                 │ LogStore   │  │ KVState│ │
│                 │ WAL+fsync  │  │ RocksDB│ │
│                 └────────────┘  └────────┘ │
└────────────────────────────────────────────┘
```

---

## 3. 模块划分

```
src/
├── common/
│   ├── status.h/.cc        # Status / StatusOr
│   ├── config.h/.cc        # node_id, peers, timeouts
│   ├── logging.h/.cc
│   └── encoding.h/.cc      # magic/version/length/crc 工具
│
├── rpc/
│   ├── message.h/.cc       # RPC 消息 Encode/Decode
│   ├── tcp_server.h/.cc    # 监听、每连接一线程
│   └── tcp_client.h/.cc    # 同步调用 + 超时
│
├── raft/
│   ├── log_entry.h/.cc     # 日志条目 + 序列化
│   ├── log_store.h/.cc     # append-only WAL、fsync、恢复
│   ├── hard_state.h/.cc    # currentTerm/votedFor 原子写
│   ├── raft_node.h/.cc     # 状态机、选举、复制、commit
│   └── snapshot.h/.cc      # stretch
│
├── storage/
│   └── state_machine.h/.cc # Put/Get/Delete + Apply + lastApplied
│
├── server/
│   └── kv_service.h/.cc    # ClientRequest 路由到 RaftNode
│
├── main/
│   ├── node_main.cc        # 启动一个节点
│   └── client_cli.cc       # put/get/delete/status
│
└── tests/
    ├── unit/
    └── integration/
```

### 关键接口

```cpp
class LogStore {
 public:
  Status Append(const LogEntry& entry);
  StatusOr<LogEntry> Get(uint64_t index) const;
  uint64_t LastIndex() const;
  uint64_t LastTerm() const;
  Status TruncateSuffix(uint64_t from_index);
  Status Sync();
};

class StateMachine {
 public:
  Status Apply(const LogEntry& entry);      // 确定性
  StatusOr<std::string> Get(const std::string& key);
  uint64_t LastApplied() const;
};

class RaftNode {
 public:
  Status Start();
  StatusOr<std::string> Propose(const Command& cmd); // 写
  StatusOr<std::string> Get(const std::string& key); // leader-only 读
  void OnMessage(const RpcMessage& msg);
};
```

---

## 4. Raft 核心设计

### 4.1 状态

| 类型 | 字段 | 是否持久化 |
|---|---|---|
| Persistent | `currentTerm` | ✅ hard state |
| Persistent | `votedFor` | ✅ hard state |
| Persistent | `log[]` | ✅ WAL |
| Volatile | `commitIndex` | ❌（可重建） |
| Volatile | `lastApplied` | ✅ 跟状态机一起存 |
| Volatile | `state` (Follower/Candidate/Leader) | ❌ |
| Volatile | `leaderId` | ❌ |
| Volatile | `nextIndex[]`, `matchIndex[]` | ❌（leader 重启后重新初始化） |

> `lastApplied` 存进 RocksDB，和每次 apply 的数据在同一个 WriteBatch 里，保证“数据改了，appliedIndex 也改了”原子发生。

### 4.2 消息

| 消息 | 字段 |
|---|---|
| `RequestVote` | term, candidateId, lastLogIndex, lastLogTerm |
| `RequestVoteResp` | term, voteGranted |
| `AppendEntries` | term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit |
| `AppendEntriesResp` | term, success, matchIndex |
| `ClientRequest` | op, key, value, requestId |
| `ClientResponse` | ok, value, leaderHint |
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
- **追加一条当前 term 的 no-op**，用于安全地推进 commitIndex；
- 立即发心跳。

### 4.4 日志复制

```
Client → Leader: Put(k,v)
Leader:
  1. AppendLog({index, term, cmd})
  2. 向每个 follower 发 AppendEntries(prevLogIndex, prevLogTerm, entries, leaderCommit)
  3. 收到多数 success → 更新 matchIndex → 若 entry.term == currentTerm 且多数 → commitIndex = index
  4. apply 到状态机
  5. 下一次 AppendEntries 把 leaderCommit 带给 follower
```

Follower 收到 AppendEntries：

```
1. term < currentTerm → 拒绝，返回当前 term
2. prevLogIndex/prevLogTerm 不匹配 → 拒绝
3. 删除冲突的 suffix（如果有）
4. 追加新 entries
5. fsync
6. 更新 commitIndex = min(leaderCommit, lastNewIndex)
7. apply 到状态机
8. 回复 success
```

### 4.5 commit 规则（重点）

```cpp
bool CanCommit(uint64_t index) {
    return log_[index].term == current_term_ &&
           match_count(index) >= majority();
}
```

旧 term 的日志只能**间接提交**：等当前 term 的 no-op 或新日志提交后，前面的旧日志才算提交。

### 4.6 持久化顺序

**AppendEntries 回复前：**

```
1. 写 log entries
2. fsync log
3. 更新 currentTerm/votedFor（如果变了）
4. fsync hard state
5. 回复 success
```

**RequestVote 投票前：**

```
1. 更新 currentTerm/votedFor
2. fsync hard state
3. 回复 voteGranted
```

顺序不能反。先回复再落盘，断电会破坏选举安全/日志安全。

---

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

恢复时：

- 文件末尾不足 8 字节 → 截断；
- `payload_len` 超过剩余字节 → 截断；
- CRC 不匹配且是最后一条 → 截断（torn write）；
- CRC 不匹配且在中间 → 报错，不静默跳过。

### 5.2 Hard State

```cpp
struct HardState {
  uint64_t current_term;
  std::string voted_for;   // 空表示没投
};
```

写入方式：

```
写 hard_state.tmp → fsync → rename → fsync(dir)
```

### 5.3 KV State Machine

RocksDB key 设计：

```
user key        → value
__meta/last_applied → uint64
```

Apply：

```cpp
rocksdb::WriteBatch batch;
// Put / Delete
batch.Put("__meta/last_applied", index);
db->Write(batch);
```

### 5.4 启动恢复

```
1. 读 hard state
2. 扫描 WAL，校验 length/CRC，截断 torn tail
3. 恢复 log 到内存
4. 从 RocksDB 读 lastApplied
5. 重放 lastApplied 之后的已提交日志
6. 进入 Follower，等待心跳/选举
```

### 5.5 Snapshot（stretch）

```
Snapshot:
  magic + version
  lastIncludedIndex
  lastIncludedTerm
  kv_count
  [key_len][key][value_len][value] * N
  crc32
```

触发条件：WAL 条目数或字节数超过阈值。安装时先写临时文件、校验、原子替换，再截断日志。

---

## 6. RPC 设计

### 消息格式

```
magic "RRPC" (4)
version u8
type u8
request_id u64
payload_len u32
payload
crc32 u32
```

### 调用模型

- 每个节点既跑 RPC Server 也跑 RPC Client；
- 客户端对每个 peer 维持一个连接；
- 同步调用：发送请求 → 等待 response（超时）；
- 超时时间：选举/心跳 50–100ms，客户端 1–3s；
- 所有 handler 先校验 magic/version/length/CRC，再分配内存。

### 幂等性

- `Put` / `Delete` 本身幂等；
- `requestId` 保留，为以后去重；
- 客户端超时重试时，可能重复发送，但 KV 语义下安全。

---

## 7. 客户端

```
cairn-cli put k v
cairn-cli get k
cairn-cli delete k
cairn-cli status
```

流程：

```
连接任意节点
  → 如果是 follower，返回 leader 地址
  → 客户端改连 leader
  → 超时/失败 → 重试（指数退避）
```

一致性：

- 写：leader commit + apply 后返回；
- 读：leader-only，不保证分区期间不返回旧值（文档写明）；
- 如果时间允许，Week 4 加 ReadIndex 作为 stretch。

---

## 8. 并发模型

第一版故意选简单模型：

```
RPC Server: 每连接一个线程（阻塞 read/write）
RaftNode:   一把 mutex 保护状态
Timer:      一个线程检查选举/心跳 deadline
Apply:      在 RaftNode 锁内或单独 apply 线程
```

原则：

- **不要在持有 Raft 锁时做阻塞网络 IO**；
- 先在锁内收集要发的消息，释放锁后再发送；
- apply 必须按 index 顺序，不能并发 apply 不同 index。

3 节点、单机、低并发，这个模型足够；面试时再讲“如何换成 epoll/EventLoop”。

---

## 9. 失败语义

| 场景 | 行为 |
|---|---|
| Follower 宕机 | 多数派仍在，服务继续；重启后追日志 |
| Leader 宕机 | 1–2s 内重新选主；已 commit 数据不丢 |
| 网络分区（少数派） | 不能选主、不能提交 |
| 网络分区（多数派） | 继续服务 |
| 分区恢复 | 日志收敛，少数派被覆盖/追上 |
| 断电/torn write | WAL 截断尾部；已 commit 数据不丢 |
| 客户端超时重试 | Put/Delete 幂等；不会重复产生副作用 |

---

## 10. 测试计划

### 单元测试

- LogEntry / Snapshot Encode-Decode round-trip；
- 坏 magic、坏 version、长度越界、CRC 错；
- WAL torn tail 截断；
- 选举状态转换；
- `commitIndex` 只推进当前 term 条目；
- 选举限制：日志落后的节点不能当选。

### 集成测试

- 3 节点启动，选出 leader；
- Put/Get/Delete；
- kill leader → 新 leader → 数据仍在；
- 重启 follower → 追上；
- 网络分区 → 少数派不能提交，多数派继续；
- 分区恢复 → 日志一致。

### 压测

- 自定义客户端，多线程 Put/Get；
- 输出 ops/s、p50/p99；
- 记录 3 节点单机环境，不夸大。

---

## 11. 4 周排期

| 周 | 目标 | 交付 |
|---|---|---|
| W1 | 地基 + 单机 KV | repo、RPC、WAL、单机 Put/Get、重启恢复 |
| W2 | 选举 + 复制 | 3 节点选主、日志复制、commit、leader 重定向 |
| W3 | 恢复 + 故障 | 重启恢复、日志一致性、kill leader、分区；snapshot 为 stretch |
| W4 | 客户端 + 测试 + 文档 | CLI、并发测试、压测、DESIGN/README、面试 Q&A |

如果第 2 周末选举还不稳：**砍快照、砍 ReadIndex，保选举/复制/恢复**。

---

## 12. 需要确认的 4 个决策

| 决策 | 建议 | 备选 |
|---|---|---|
| RPC / 线程模型 | 自研最小 TCP + 阻塞 socket + 每连接一线程 | 复用 PulseRPC / EventLoop |
| 读一致性 | 第一版 leader-only，文档写明边界 | Week 4 加 ReadIndex |
| Snapshot | stretch，不阻塞主线 | 必须做（会挤压测试时间） |
| 存储 | Raft log 自研 WAL + KV 用 RocksDB | 全部塞 RocksDB |

---

## 13. 下次继续的方式

1. 先原样重讲本文件（按第 0–12 节顺序）；
2. 确认第 12 节的 4 个决策；
3. 回答 3 个 Raft 设计问题：
   - 为什么 leader 不能直接提交旧 term 的日志？
   - AppendEntries 为什么需要 `prevLogIndex/prevLogTerm`？
   - 收到 AppendEntries 后，持久化和回复的顺序是什么？
4. 进入 Week 1：仓库骨架、RPC、WAL、单机 KV。
