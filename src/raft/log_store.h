#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "cairnkv/status.h"
#include "raft/log_entry.h"

namespace cairnkv
{

class LogStore
{
public:
    LogStore() = default;
    ~LogStore();

    LogStore(const LogStore&) = delete;
    LogStore& operator=(const LogStore&) = delete;

    Status Open(const std::string& path);
    Status Append(const LogEntry& entry);
    Status Sync();

    StatusOr<LogEntry> Get(uint64_t index) const;
    uint64_t LastIndex() const;
    uint64_t LastTerm() const;
    uint64_t FileSize() const;

    // Remove entries at and after from_index. Used when a follower conflicts with the leader.
    Status TruncateSuffix(uint64_t from_index);

private:
    Status Recover(const std::string& data);

    int fd_ = -1;
    std::string path_;
    std::vector<LogEntry> entries_;
    std::vector<uint64_t> offsets_;
    uint64_t file_size_ = 0;
};

}
