#include "server/kv_service.h"

#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <utility>

#include "common/logging.h"

namespace cairnkv
{

namespace
{

constexpr uint64_t kSingleNodeTerm = 1;

}

KVService::KVService(std::string node_id, std::string data_dir)
    : node_id_(std::move(node_id)), data_dir_(std::move(data_dir))
{
}

Status KVService::Open()
{
    std::error_code ec;
    std::filesystem::create_directories(data_dir_, ec);
    if (ec)
    {
        return Status(StatusCode::kInternal,
                      "create data directory: " + ec.message());
    }

    CAIRNKV_RETURN_IF_ERROR(log_.Open(data_dir_ + "/raft.log"));
    CAIRNKV_RETURN_IF_ERROR(state_.Open(data_dir_ + "/state"));
    return Replay();
}

Status KVService::Replay()
{
    const uint64_t applied = state_.LastApplied();
    const uint64_t last_index = log_.LastIndex();
    if (applied > last_index)
    {
        return Status(StatusCode::kCorruption,
                      "state machine is ahead of the Raft log");
    }

    for (uint64_t index = applied + 1; index <= last_index; ++index)
    {
        StatusOr<LogEntry> entry = log_.Get(index);
        if (!entry.ok())
        {
            return entry.status();
        }
        CAIRNKV_RETURN_IF_ERROR(state_.Apply(*entry));
    }

    if (last_index > applied)
    {
        CAIRNKV_LOG_INFO("replayed " << (last_index - applied)
                                     << " log entries into the state machine");
    }
    return Status::Ok();
}

StatusOr<uint64_t> KVService::Put(const std::string& key,
                                  const std::string& value)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);

    const uint64_t index = log_.LastIndex() + 1;
    const LogEntry entry{index, kSingleNodeTerm, EntryType::kPut, key, value};

    CAIRNKV_RETURN_IF_ERROR(log_.Append(entry));
    CAIRNKV_RETURN_IF_ERROR(log_.Sync());
    CAIRNKV_RETURN_IF_ERROR(state_.Apply(entry));
    return index;
}

Status KVService::Delete(const std::string& key)
{
    std::unique_lock<std::shared_mutex> lock(mutex_);

    const uint64_t index = log_.LastIndex() + 1;
    const LogEntry entry{index, kSingleNodeTerm, EntryType::kDelete, key, ""};

    CAIRNKV_RETURN_IF_ERROR(log_.Append(entry));
    CAIRNKV_RETURN_IF_ERROR(log_.Sync());
    return state_.Apply(entry);
}

StatusOr<GetResult> KVService::Get(const std::string& key, ReadOptions opts)
{
    (void)opts;   // W1 is a single node, so both modes read the local state.

    std::shared_lock<std::shared_mutex> lock(mutex_);
    StatusOr<std::string> value = state_.Get(key);
    if (!value.ok())
    {
        return value.status();
    }
    return GetResult{*value, state_.LastApplied()};
}

StatusOr<std::vector<KeyValue>> KVService::Scan(const std::string& prefix,
                                                 uint32_t limit)
{
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return state_.Scan(prefix, limit);
}

StatusOr<ClusterStatus> KVService::GetStatus()
{
    std::shared_lock<std::shared_mutex> lock(mutex_);

    ClusterStatus status;
    status.role = NodeRole::kLeader;   // W1 has one node, so it is always the leader.
    status.term = kSingleNodeTerm;
    status.leader_id = node_id_;
    status.commit_index = log_.LastIndex();
    status.last_applied = state_.LastApplied();
    status.log_last_index = log_.LastIndex();
    status.wal_bytes = log_.FileSize();
    return status;
}

}
