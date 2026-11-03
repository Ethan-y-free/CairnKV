#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "cairnkv/status.h"

namespace cairnkv
{

constexpr uint32_t kMaxLogEntryPayload = 8u * 1024u * 1024u;

enum class EntryType : uint8_t
{
    kNoop = 1,
    kPut = 2,
    kDelete = 3
};

struct LogEntry
{
    uint64_t index = 0;
    uint64_t term = 0;
    EntryType type = EntryType::kNoop;
    std::string key;
    std::string value;
};

std::string EncodeLogEntry(const LogEntry& entry);
StatusOr<LogEntry> DecodeLogEntry(std::string_view payload);

}
