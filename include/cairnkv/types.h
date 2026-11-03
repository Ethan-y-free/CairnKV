#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace cairnkv
{

enum class ReadMode : uint8_t
{
    kLinearizable = 0,
    kStale = 1
};

struct ReadOptions
{
    ReadMode mode = ReadMode::kLinearizable;
};

struct ScanOptions
{
    uint32_t limit = 100;
};

enum class NodeRole : uint8_t
{
    kFollower = 0,
    kCandidate = 1,
    kLeader = 2
};

struct KeyValue
{
    std::string key;
    std::string value;
};

struct GetResult
{
    std::string value;
    uint64_t applied_index = 0;
};

struct ClusterStatus
{
    NodeRole role = NodeRole::kFollower;
    uint64_t term = 0;
    std::string leader_id;
    uint64_t commit_index = 0;
    uint64_t last_applied = 0;
    uint64_t log_last_index = 0;
    uint64_t wal_bytes = 0;
};

}
