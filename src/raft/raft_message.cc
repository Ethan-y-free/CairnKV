#include "raft/raft_message.h"

#include "common/encoding.h"

namespace cairnkv
{

namespace
{

constexpr char kRaftMessageMagic[4] = {'R', 'M', 'S', 'G'};
constexpr uint8_t kRaftMessageVersion = 1;
constexpr uint32_t kMaxRaftEntries = 1024;

StatusOr<std::string> ReadBoundedString(std::string_view payload, size_t* pos)
{
    uint32_t len = 0;
    if (!encoding::ReadU32(payload, pos, &len))
    {
        return Status(StatusCode::kCorruption, "truncated raft string length");
    }
    if (*pos > payload.size() || len > payload.size() - *pos)
    {
        return Status(StatusCode::kCorruption, "truncated raft string");
    }
    std::string value(payload.data() + *pos, len);
    *pos += len;
    return value;
}

}

std::string EncodeRaftMessage(const RaftMessage& message)
{
    std::string out;
    out.reserve(4 + 1 + 1 + 8 + 64 + 64 + 8 + 8 + 1 + 64 + 8 + 8 + 4 +
                message.entries.size() * 32 + 8 + 1 + 8);

    encoding::AppendBytes(&out, kRaftMessageMagic, sizeof(kRaftMessageMagic));
    encoding::AppendU8(&out, kRaftMessageVersion);
    encoding::AppendU8(&out, static_cast<uint8_t>(message.type));
    encoding::AppendU64(&out, message.term);
    encoding::AppendString(&out, message.from_id);
    encoding::AppendString(&out, message.to_id);
    encoding::AppendU64(&out, message.last_log_index);
    encoding::AppendU64(&out, message.last_log_term);
    encoding::AppendU8(&out, message.vote_granted ? 1 : 0);
    encoding::AppendString(&out, message.leader_id);
    encoding::AppendU64(&out, message.prev_log_index);
    encoding::AppendU64(&out, message.prev_log_term);

    encoding::AppendU32(&out, static_cast<uint32_t>(message.entries.size()));
    for (const auto& entry : message.entries)
    {
        const std::string payload = EncodeLogEntry(entry);
        encoding::AppendU32(&out, static_cast<uint32_t>(payload.size()));
        encoding::AppendBytes(&out, payload.data(), payload.size());
    }

    encoding::AppendU64(&out, message.leader_commit);
    encoding::AppendU8(&out, message.success ? 1 : 0);
    encoding::AppendU64(&out, message.match_index);
    return out;
}

StatusOr<RaftMessage> DecodeRaftMessage(std::string_view payload)
{
    if (payload.size() < sizeof(kRaftMessageMagic) + 1 + 1)
    {
        return Status(StatusCode::kCorruption, "raft message too short");
    }
    if (payload.substr(0, sizeof(kRaftMessageMagic)) !=
        std::string_view(kRaftMessageMagic, sizeof(kRaftMessageMagic)))
    {
        return Status(StatusCode::kCorruption, "bad raft message magic");
    }

    size_t pos = sizeof(kRaftMessageMagic);
    uint8_t version = 0;
    uint8_t type = 0;
    if (!encoding::ReadU8(payload, &pos, &version) ||
        !encoding::ReadU8(payload, &pos, &type))
    {
        return Status(StatusCode::kCorruption, "truncated raft message header");
    }
    if (version != kRaftMessageVersion)
    {
        return Status(StatusCode::kCorruption,
                      "unsupported raft message version");
    }
    if (type < static_cast<uint8_t>(RaftMessageType::kRequestVote) ||
        type > static_cast<uint8_t>(RaftMessageType::kAppendEntriesResp))
    {
        return Status(StatusCode::kCorruption, "invalid raft message type");
    }

    RaftMessage message;
    message.type = static_cast<RaftMessageType>(type);
    if (!encoding::ReadU64(payload, &pos, &message.term))
    {
        return Status(StatusCode::kCorruption, "truncated raft term");
    }

    StatusOr<std::string> from = ReadBoundedString(payload, &pos);
    if (!from.ok())
    {
        return from.status();
    }
    message.from_id = *from;

    StatusOr<std::string> to = ReadBoundedString(payload, &pos);
    if (!to.ok())
    {
        return to.status();
    }
    message.to_id = *to;

    if (!encoding::ReadU64(payload, &pos, &message.last_log_index) ||
        !encoding::ReadU64(payload, &pos, &message.last_log_term))
    {
        return Status(StatusCode::kCorruption, "truncated raft log metadata");
    }

    uint8_t vote_granted = 0;
    if (!encoding::ReadU8(payload, &pos, &vote_granted))
    {
        return Status(StatusCode::kCorruption, "truncated vote flag");
    }
    message.vote_granted = vote_granted != 0;

    StatusOr<std::string> leader_id = ReadBoundedString(payload, &pos);
    if (!leader_id.ok())
    {
        return leader_id.status();
    }
    message.leader_id = *leader_id;

    if (!encoding::ReadU64(payload, &pos, &message.prev_log_index) ||
        !encoding::ReadU64(payload, &pos, &message.prev_log_term))
    {
        return Status(StatusCode::kCorruption, "truncated prev log metadata");
    }

    uint32_t entry_count = 0;
    if (!encoding::ReadU32(payload, &pos, &entry_count))
    {
        return Status(StatusCode::kCorruption, "truncated entry count");
    }
    if (entry_count > kMaxRaftEntries)
    {
        return Status(StatusCode::kCorruption, "too many raft entries");
    }
    message.entries.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; ++i)
    {
        uint32_t entry_len = 0;
        if (!encoding::ReadU32(payload, &pos, &entry_len))
        {
            return Status(StatusCode::kCorruption, "truncated entry length");
        }
        if (pos > payload.size() || entry_len > payload.size() - pos)
        {
            return Status(StatusCode::kCorruption, "truncated entry body");
        }
        StatusOr<LogEntry> entry =
            DecodeLogEntry(payload.substr(pos, entry_len));
        if (!entry.ok())
        {
            return entry.status();
        }
        message.entries.push_back(*entry);
        pos += entry_len;
    }

    uint8_t success = 0;
    if (!encoding::ReadU64(payload, &pos, &message.leader_commit) ||
        !encoding::ReadU8(payload, &pos, &success) ||
        !encoding::ReadU64(payload, &pos, &message.match_index))
    {
        return Status(StatusCode::kCorruption, "truncated raft message tail");
    }
    message.success = success != 0;

    if (pos != payload.size())
    {
        return Status(StatusCode::kCorruption,
                      "trailing bytes in raft message");
    }
    return message;
}

}
