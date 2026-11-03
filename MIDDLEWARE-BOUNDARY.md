# CairnKV 中间件边界（v1）

> 2026/11/02 定稿。CairnKV 作为业务无关的共识 KV 中间件独立开发；CairnShop 是第一个真实消费者。
> 本文是对 `DESIGN-draft-v1.md` 的边界修订，Raft 核心设计不变。

## 0. 定位

一句话：**3 节点、单 Raft group、强一致的 KV 中间件，面向小而关键的元数据（配置、开关、路由表、选主信息）。**

CairnKV 不做业务语义：不内置配置中心、用户、权限、审计、前端。这些属于消费者。

## 1. 与 DESIGN-draft-v1 的差异

| 项 | v1 草案 | 本次修订 |
|---|---|---|
| 管理面 | 曾设想内置 cairn-admin / 控制台 | **移出核心**，归 CairnShop |
| 命令载荷 | 曾设想 operator / request_ts | **移除业务字段**；value 保持不透明 |
| 只读接口 | 无 | 新增 `Status`、`Scan` |
| 写响应 | 无索引 | 新增 `applied_index` |
| 交付形态 | 仅源码 / 设计 | 新增 `libcairnkv` SDK、`cairnctl`、Docker、CI |
| 历史 / 回滚 | 曾设想核心内实现 | 归消费者（业务层自行实现） |

Raft 核心（选举、复制、commit、持久化、恢复）不变。

## 2. 交付物

| 交付物 | 说明 |
|---|---|
| `cairnkd` | 服务端二进制；JSON 配置文件；3 节点集群 |
| `libcairnkv` | C++ 客户端 SDK；线程安全；leader 重定向、重试、超时、连接池 |
| `cairnctl` | 运维 / 调试 CLI：put / get / delete / scan / status |
| Docker 镜像 + compose | 3 节点一键启动 |
| `/healthz` `/readyz` `/metrics` | 健康检查与 Prometheus 指标 |
| 文档 | QUICKSTART / API / OPS / DESIGN / CASE-STUDY / CHANGELOG |

## 3. 公开 API（v1）

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

- `Get` 默认 `kLinearizable`（ReadIndex）；`kStale` 显式放宽，返回 `applied_index`；
- value 不透明，上限 64 KiB；
- `Scan` 仅前缀内、分页、限制条数（默认 100，最大 1000）；
- `ClusterStatus`：role / term / leaderId / commitIndex / lastApplied / WAL 大小 / 各 peer matchIndex；
- 错误码：`OK / NOT_LEADER / TIMEOUT / KEY_NOT_FOUND / INVALID_ARGUMENT / UNAVAILABLE / INTERNAL`；
- 写响应带 `applied_index`，供消费者做幂等与审计。

## 4. 复用契约

- 公共头文件只在 `include/cairnkv/`；
- CMake `install()` + `cairnkvConfig.cmake`，支持 `find_package(cairnkv)`；
- 消费者用 `FetchContent` 固定 tag（如 `v1.0.0`），不复制源码；
- 语义化版本；wire protocol 带版本号；客户端/服务端兼容矩阵写进文档；
- 不向消费者暴露 Raft / LogStore 内部类型。

## 5. 不做（v1）

- 配置中心语义：namespace / group / dataId、灰度、历史、回滚；
- 用户 / 权限 / 审计 / 前端 / MySQL；
- Watch / CAS / Batch / 事务 / 分片 / 动态成员变更；
- Lease read / Follower ReadIndex 归 v1.x；v1 只做 ReadIndex + STALE；
- 多机部署脚本 / K8s。

> 这些是消费者或后续版本的事；CairnShop MVP 不需要 CAS / Watch。

## 6. 质量门槛

- 单测：编码/解码、WAL torn tail、选举、commit 规则、ReadIndex 旧 leader 拒绝；
- 集成：3 节点选主、复制、kill leader、重启、分区；
- 并发正确性：TSan / ASan；
- 压测：单机 3 进程、64B value、256 并发、pipeline 16，记录 ops/s、p50/p99；
- CI 全绿；Docker compose 一键起；
- 优雅关闭；结构化日志；request_id；指标。

## 7. 版本路线

| 版本 | 内容 | 验收 |
|---|---|---|
| v0.1 | 单节点 KV + WAL + 重启恢复 | 单机 put/get/重启不丢 |
| v0.2 | 3 节点选举 + 复制 + commit + leader 重定向 | 3 节点跑通 |
| v0.3 | ReadIndex + 恢复 / 一致性 / kill leader / 分区 | 故障测试通过 |
| v1.0 | SDK / CLI / metrics / compose / 文档 / 压测 | CairnShop 可依赖 |
| v1.x | Lease read / Follower ReadIndex / Watch / CAS（由消费者驱动） | 按需 |

## 8. 消费者验证（CairnShop）

CairnShop 通过 `libcairnkv` 使用 CairnKV，验证：

- 配置读写延迟与开关生效时间；
- kill leader 时业务不中断；
- 重启 / 分区后的 SDK 重连；
- Scan / Status 在控制台的使用。

结果写入 `docs/CASE-STUDY.md`。
