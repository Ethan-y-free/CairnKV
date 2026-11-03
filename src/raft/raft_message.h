#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "cairnkv/status.h"
#include "raft/log_entry.h"

namespace cairnkv
{

enum class RaftMessageType : uint8_t
{
    kRequestVote = 1,
    kRequestVoteResp = 2,
    kAppendEntries = 3,
    kAppendEntriesResp = 4
};

struct RaftMessage
{
    RaftMessageType type = RaftMessageType::kRequestVote;
    uint64_t term = 0;
    std::string from_id;
    std::string to_id;

    // RequestVote
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;
    bool vote_granted = false;

    // AppendEntries
    std::string leader_id;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    std::vector<LogEntry> entries;
    uint64_t leader_commit = 0;

    // AppendEntriesResp
    bool success = false;
    uint64_t match_index = 0;

    // ReadIndex heartbeat correlation; 0 means a normal AppendEntries.
    uint64_t read_context = 0;
};

std::string EncodeRaftMessage(const RaftMessage& message);
StatusOr<RaftMessage> DecodeRaftMessage(std::string_view payload);

}
