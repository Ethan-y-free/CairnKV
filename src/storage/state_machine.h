#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "cairnkv/status.h"
#include "cairnkv/types.h"
#include "raft/log_entry.h"

namespace rocksdb
{
class DB;
}

namespace cairnkv
{

class StateMachine
{
public:
    StateMachine() = default;
    ~StateMachine();

    StateMachine(const StateMachine&) = delete;
    StateMachine& operator=(const StateMachine&) = delete;

    Status Open(const std::string& path);

    // Apply must be deterministic and strictly ordered by log index.
    Status Apply(const LogEntry& entry);

    StatusOr<std::string> Get(const std::string& key) const;
    StatusOr<std::vector<KeyValue>> Scan(const std::string& prefix,
                                          uint32_t limit) const;
    uint64_t LastApplied() const;

private:
    rocksdb::DB* db_ = nullptr;
    std::atomic<uint64_t> last_applied_{0};
};

}
