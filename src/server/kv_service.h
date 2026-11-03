#pragma once

#include <cstdint>
#include <shared_mutex>
#include <string>
#include <vector>

#include "cairnkv/status.h"
#include "cairnkv/types.h"
#include "raft/log_store.h"
#include "storage/state_machine.h"

namespace cairnkv
{

// W1 single-node service: every append is immediately committed and applied.
// W2 replaces this with RaftNode's replicated state machine.
class KVService
{
public:
    KVService(std::string node_id, std::string data_dir);

    Status Open();

    StatusOr<uint64_t> Put(const std::string& key, const std::string& value);
    Status Delete(const std::string& key);
    StatusOr<GetResult> Get(const std::string& key, ReadOptions opts);
    StatusOr<std::vector<KeyValue>> Scan(const std::string& prefix,
                                          uint32_t limit);
    StatusOr<ClusterStatus> GetStatus();

private:
    Status Replay();

    std::string node_id_;
    std::string data_dir_;
    LogStore log_;
    StateMachine state_;
    mutable std::shared_mutex mutex_;
};

}
