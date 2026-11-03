#include "raft/log_entry.h"

#include "common/encoding.h"

namespace cairnkv
{

namespace
{

constexpr char kLogEntryMagic[4] = {'R', 'L', 'G', '1'};
constexpr uint8_t kLogEntryVersion = 1;

}

std::string EncodeLogEntry(const LogEntry& entry)
{
    std::string out;
    out.reserve(4 + 1 + 1 + 8 + 8 + 4 + entry.key.size() + 4 + entry.value.size());
    encoding::AppendBytes(&out, kLogEntryMagic, sizeof(kLogEntryMagic));
    encoding::AppendU8(&out, kLogEntryVersion);
    encoding::AppendU8(&out, static_cast<uint8_t>(entry.type));
    encoding::AppendU64(&out, entry.index);
    encoding::AppendU64(&out, entry.term);
    encoding::AppendString(&out, entry.key);
    encoding::AppendString(&out, entry.value);
    return out;
}

StatusOr<LogEntry> DecodeLogEntry(std::string_view payload)
{
    if (payload.size() > kMaxLogEntryPayload)
    {
        return Status(StatusCode::kCorruption, "log entry payload too large");
    }
    if (payload.size() < sizeof(kLogEntryMagic))
    {
        return Status(StatusCode::kCorruption, "log entry too short");
    }
    if (payload.substr(0, sizeof(kLogEntryMagic)) !=
        std::string_view(kLogEntryMagic, sizeof(kLogEntryMagic)))
    {
        return Status(StatusCode::kCorruption, "bad log entry magic");
    }

    size_t pos = sizeof(kLogEntryMagic);
    uint8_t version = 0;
    uint8_t type = 0;
    if (!encoding::ReadU8(payload, &pos, &version) ||
        !encoding::ReadU8(payload, &pos, &type))
    {
        return Status(StatusCode::kCorruption, "truncated log entry header");
    }
    if (version != kLogEntryVersion)
    {
        return Status(StatusCode::kCorruption, "unsupported log entry version");
    }
    if (type < static_cast<uint8_t>(EntryType::kNoop) ||
        type > static_cast<uint8_t>(EntryType::kDelete))
    {
        return Status(StatusCode::kCorruption, "invalid log entry type");
    }

    LogEntry entry;
    entry.type = static_cast<EntryType>(type);
    if (!encoding::ReadU64(payload, &pos, &entry.index) ||
        !encoding::ReadU64(payload, &pos, &entry.term) ||
        !encoding::ReadString(payload, &pos, &entry.key) ||
        !encoding::ReadString(payload, &pos, &entry.value))
    {
        return Status(StatusCode::kCorruption, "truncated log entry body");
    }
    if (pos != payload.size())
    {
        return Status(StatusCode::kCorruption, "trailing bytes in log entry");
    }
    return entry;
}

}
