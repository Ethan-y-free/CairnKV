#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "cairnkv/status.h"

namespace cairnkv
{

inline constexpr std::string_view kRpcMagic = "RRPC";
constexpr uint8_t kRpcVersion = 1;
constexpr size_t kFrameHeaderSize = 18;   // magic + version + type + request_id + payload_len
constexpr uint32_t kMaxFramePayload = 8u * 1024u * 1024u;

enum class MessageType : uint8_t
{
    kClientRequest = 1,
    kClientResponse = 2,
    kRaftRequest = 3,
    kRaftResponse = 4
};

struct Frame
{
    MessageType type = MessageType::kClientRequest;
    uint64_t request_id = 0;
    std::string payload;
};

enum class DecodeResult
{
    kOk,
    kNeedMore,
    kError
};

std::string EncodeFrame(MessageType type,
                        uint64_t request_id,
                        std::string_view payload);

DecodeResult TryDecodeFrame(std::string_view data,
                            Frame* frame,
                            size_t* consumed,
                            Status* status);

}
