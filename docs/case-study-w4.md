# CairnKV W4 性能案例：从广播风暴到流水线复制

这是一次完整的"测量 → 定位 → 优化 → 复测"过程，数据全部来自本仓库的 `docs/benchmarks/`。

## 1. 测量方法

- **客户端模型**：closed-loop，每个压测线程持有一个阻塞 `Client`，同一时刻只有一个在途请求，并发度等于线程数。
- **时间窗口**：预热 2s（不计入结果）+ 测量 10s；所有线程用 `std::barrier` 对齐两个窗口的边界。
- **延迟**：只统计成功请求的 p50/p90/p99/p999/max；超时与错误单独计数。
- **服务端快照**：测量前后各取一次 `status`，用 `commit_index` / `last_applied` / `wal_bytes` 的差值交叉验证客户端吞吐。
- **服务端指标**：Raft core 每秒输出 proposals、fsync 次数/耗时、apply 次数/耗时、AppendEntries 发送/响应/拒绝数，以及每个 peer 的 `next`/`match`/`inflight`/队列深度。
- **环境**：2 vCPU（AMD Ryzen 9 8945HX）VMware 虚拟机，ext4，RocksDB 8.9.1，Release 构建，value 64B，keyspace 1000。

已知偏差：closed-loop 存在 coordinated omission，服务端卡顿时客户端会自动少发，p99 偏乐观。本案例只用于优化前后对比。

## 2. 基线发现

### 2.1 单节点：fsync 串行是唯一瓶颈

| Put 线程 | 吞吐 | p50 | p99 |
|---|---|---|---|
| 1 | 1176 ops/s | 834 µs | 1128 µs |
| 8 | 1532 ops/s | 5.4 ms | 6.5 ms |
| 32 | 1796 ops/s | 17.7 ms | 19.2 ms |

服务端指标：每秒 `sync_calls ≈ proposals ≈ 1830`（**1:1**），`sync_avg_us ≈ 500`。core 线程约 91% 的时间阻塞在 `fdatasync` 上。

结论：每条 proposal 单独 fsync，增加并发只增加排队，不提高合并率。用 Little's law 验证：32 个在途请求 / 3400 ops/s ≈ 9.4ms，与 32 线程 p50 9.26ms 吻合，说明系统在 core 处饱和。

### 2.2 集群：高并发 Put 被广播风暴打崩

| Put 线程 | 吞吐 | p50 | p99 |
|---|---|---|---|
| 1 | 513 ops/s | 1.9 ms | 3.2 ms |
| 8 | 114 ops/s | 64 ms | 132 ms |
| 32 | 125 ops/s | 242 ms | 452 ms |

所有请求 0 超时、0 错误——不是失败，是排队。

证据链：

- leader 每秒发出 **12k–16k 条 AppendEntries**，却只完成 110–150 条 proposal（约 100 倍放大）；
- 两个 peer 的发送队列长期处于 190–256（上限 256，满时 `pop_front` 静默丢最老消息）；
- follower 每秒处理 `append_req` 5000–8700 次，而 `appended` 常为 0（几乎全是重复或空请求）；
- 压测结束后，积压的队列消息仍需约 10 秒才能排空。

放大回路：

1. 每条 proposal 都 `BroadcastAppendEntries()`；
2. `next_index` 只在响应成功后推进，发送时不推进，所以每次都从同一位置重发同一批；
3. 成功响应后若 `next <= last_index` 立即再发，新 proposal 又让 `last_index` 前进，追赶条件永远成立；
4. `PeerClient` 一次只发一条、等响应；队列满时静默丢弃，丢掉的响应永远不会回来，下一轮继续重发。

三个条件缺一不可：**全量广播 + 串行发送 + 有界队列丢消息**。少任何一个，都不会形成正反馈退化。

## 3. 优化一：WAL 组提交

**设计**：core 线程只 `Append`（写 page cache），把 `{index, promise}` 放入 `pending_commits_`；独立 WAL 线程批量 drain，一次 `fdatasync`，再通过 core 事件循环回调 `OnWalSynced`。

**正确性约束**：

- core 维护 `last_synced_index_`；`AdvanceCommitIndex` 增加 `index <= last_synced_index_`，leader 本地日志未持久化前绝不 commit；
- follower 截断日志时同步下调水位；已截断的提交点直接失败；
- fsync 失败进入 `wal_failed_` 状态，拒绝新提案并失败所有在途 promise，不静默继续。

**效果**：单节点 8t 1532 → 3232 ops/s，32t 1796 → 3426 ops/s；p50 5.4/17.7ms → 2.4/9.3ms。`sync_calls:proposals` 从 1:1 降到约 1:2。

**为什么批只有 2 条**：fsync 约 500µs，2 vCPU 上 core 每 500µs 只能处理约 1.7 条 proposal（32 个客户端线程、RPC 线程、WAL 线程共享两个核）。批大小不是组提交逻辑的上限，而是 core 吞吐的上限。

## 4. 优化二：复制调度重写

**设计**：

- `PeerClient` 拆成 sender / receiver 两条线程；`Send()` 返回 `bool`，队列满时调用方回退而不是静默丢弃；
- 每个 peer 维护乐观推进的 `next_index`、只由成功响应推进的 `match_index`、最多 4 个在途批次的 `inflight`；
- 删除"每条 proposal 全量广播"，改为状态驱动补窗：`OnWalSynced`、响应到达、心跳时调用 `MaybeSendAppendEntries`；
- 失败响应：回退到 `match+1`、清空 inflight、重发；
- 1 秒超时重传：响应丢失时回退重试，避免窗口永久卡死；
- 心跳只在窗口有空位时发送；有数据在途时数据批次本身充当心跳。

**效果**：

| 指标 | 基线 | 优化后 |
|---|---|---|
| 集群 Put 8t | 114 ops/s, p99 132ms | **1894 ops/s, p99 6.5ms** |
| 集群 Put 32t | 125 ops/s, p99 452ms | **1901 ops/s, p99 20.9ms** |
| leader `appends:proposals` | ~100:1 | ~0.65:1 |
| peer 发送队列 | 190–256 | 0 |
| `inflight` | — | 0–3 |

## 5. 剩余瓶颈

- **集群 ≈ 单节点的 53%**（1894 / 3574）：leader 组提交批约 2.2 条、fsync 500µs；follower 每批 AppendEntries 仍在 core 线程串行 fsync 约 600µs，两边串行叠加。
- **单节点上限约 3.5k ops/s**：受 2 vCPU 的 core 处理速率限制。可选的下一步是凑批窗口（延迟换吞吐）、降低单条 Append 成本，或把压测客户端放到宿主机。
- **linearizable Get 约 7.4k ops/s**：ReadIndex 需要多数派往返；stale 读仍走 core 队列，旁路 core 是后续优化。

## 6. 面试问答

**Q1：为什么要先建基线，而不是直接优化？**
基线把"感觉慢"变成可复现的数字，并给出优化前后的对照。没有基线的优化无法证明有效，也无法判断瓶颈是否转移。基线还暴露了预期之外的问题（集群高并发塌陷）。

**Q2：closed-loop 压测的 coordinated omission 是什么？**
客户端只在收到响应后才发下一个请求，服务端卡顿时客户端自动降速，长尾被系统性低估。p99 偏乐观。解决办法是开环定速或按预期时间戳计算延迟；本项目只在前后对比中使用 closed-loop，并在文档里标注了这个偏差。

**Q3：怎么判断瓶颈是 fsync 而不是 CPU？**
看服务端指标：`sync_calls:proposals` 是 1:1、`sync_avg_us` 500µs、core 时间几乎被 fsync 占满。如果是 CPU 瓶颈，appends/apply 的耗时占比会更高，而 sync 次数不会与 proposal 严格成比例。

**Q4：组提交为什么能提升吞吐？为什么单线程没有收益？**
一次 `fdatasync` 覆盖 N 条日志，fsync 次数降为 1/N。单线程只有一个在途请求，批大小恒为 1，还要多付两次线程切换，所以持平或略降；收益来自并发。

**Q5：组提交时 fsync 失败怎么办？**
WAL 线程把错误回传 core，设置 `wal_failed_`，失败当前批次和队列中所有 promise，之后拒绝新提案。不能静默继续，否则客户端会以为写入成功，而日志可能没有落盘。

**Q6：为什么 commit 不能超过 `last_synced_index_`？**
Raft 要求被提交的条目在多数派上持久化。leader 广播前先本地 fsync，虽然逻辑上保证顺序，但代码里显式加约束可以防止未来改成"复制与 fsync 并行"时提前提交。

**Q7：流水线里 `next_index` 和 `match_index` 为什么要拆开？**
`next_index` 是"下一次该发哪里"的发送游标，可以乐观推进；`match_index` 是"对方确认收到了哪里"，只能由成功响应推进。拆开后发送不必等响应，同时仍能准确判断多数派。

**Q8：失败响应为什么要清空整个 inflight？**
所有在途批次都基于同一个未确认的前缀（prev_index/prev_term）。一旦 follower 拒绝该前缀，后续批次全部失效，逐个回退不如整体清空后从 `match+1` 重发。

**Q9：广播风暴的三个必要条件是什么？**
全量广播、串行发送、有界队列静默丢弃。去掉任意一个都能打破正反馈：状态驱动补窗去掉第一个；收发解耦去掉第二个；`Send` 返回 false 并回退去掉第三个。

**Q10：队列满时为什么不能丢最老的消息？**
丢掉的可能是携带日志的 AppendEntries，它的响应永远不会回来，但发送方已经推进了 `next_index`/窗口计数，导致重复发送和窗口泄漏。正确做法是让发送失败、回退乐观状态，由上层重试。

**Q11：为什么集群吞吐约为单节点的一半？**
leader 先本地 fsync（~500µs），再把批发给 follower；follower 在自己的 core 线程同步 fsync（~600µs）。两边串行，吞吐约为单节点的 1/2。

**Q12：TSan 发现了什么？怎么修的？**
`EventLoop::Run()` 重复写 `thread_id_`，与主线程的 `IsInLoopThread()` 读取竞争。构造函数已经在线程池的目标线程里记录了正确的 id，且线程池用 promise/future 发布指针，happens-before 已成立；删掉重复写并把成员改为 `const` 即可。
