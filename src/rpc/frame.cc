#include "rpc/frame.h"

#include "common/encoding.h"

namespace cairnkv
{

std::string EncodeFrame(MessageType type,
                        uint64_t request_id,
                        std::string_view payload)
{
    std::string out;
    out.reserve(kFrameHeaderSize + payload.size() + 4);
    encoding::AppendBytes(&out, kRpcMagic.data(), kRpcMagic.size());
    encoding::AppendU8(&out, kRpcVersion);
    encoding::AppendU8(&out, static_cast<uint8_t>(type));
    encoding::AppendU64(&out, request_id);
    encoding::AppendU32(&out, static_cast<uint32_t>(payload.size()));
    encoding::AppendBytes(&out, payload.data(), payload.size());

    // CRC covers version..payload; the magic is validated separately.
    const uint32_t crc = encoding::Crc32(out.data() + 4, out.size() - 4);
    encoding::AppendU32(&out, crc);
    return out;
}

DecodeResult TryDecodeFrame(std::string_view data,
                            Frame* frame,
                            size_t* consumed,
                            Status* status)
{
    if (data.size() < kFrameHeaderSize)
    {
        return DecodeResult::kNeedMore;
    }
    if (data.substr(0, kRpcMagic.size()) != kRpcMagic)
    {
        *status = Status(StatusCode::kCorruption, "bad RPC frame magic");
        return DecodeResult::kError;
    }

    size_t pos = kRpcMagic.size();
    uint8_t version = 0;
    uint8_t type = 0;
    uint64_t request_id = 0;
    uint32_t payload_len = 0;
    if (!encoding::ReadU8(data, &pos, &version) ||
        !encoding::ReadU8(data, &pos, &type) ||
        !encoding::ReadU64(data, &pos, &request_id) ||
        !encoding::ReadU32(data, &pos, &payload_len))
    {
        *status = Status(StatusCode::kCorruption, "truncated RPC frame header");
        return DecodeResult::kError;
    }
    if (version != kRpcVersion)
    {
        *status = Status(StatusCode::kCorruption, "unsupported RPC version");
        return DecodeResult::kError;
    }
    if (type < static_cast<uint8_t>(MessageType::kClientRequest) ||
        type > static_cast<uint8_t>(MessageType::kRaftResponse))
    {
        *status = Status(StatusCode::kCorruption, "invalid RPC message type");
        return DecodeResult::kError;
    }
    if (payload_len > kMaxFramePayload)
    {
        *status = Status(StatusCode::kCorruption, "RPC frame too large");
        return DecodeResult::kError;
    }

    const size_t total = kFrameHeaderSize + payload_len + 4;
    if (data.size() < total)
    {
        return DecodeResult::kNeedMore;
    }

    size_t crc_pos = kFrameHeaderSize + payload_len;
    uint32_t stored_crc = 0;
    if (!encoding::ReadU32(data, &crc_pos, &stored_crc))
    {
        *status = Status(StatusCode::kCorruption, "truncated RPC CRC");
        return DecodeResult::kError;
    }
    const uint32_t actual_crc =
        encoding::Crc32(data.data() + 4, kFrameHeaderSize - 4 + payload_len);
    if (stored_crc != actual_crc)
    {
        *status = Status(StatusCode::kCorruption, "RPC frame CRC mismatch");
        return DecodeResult::kError;
    }

    frame->type = static_cast<MessageType>(type);
    frame->request_id = request_id;
    frame->payload.assign(data.data() + kFrameHeaderSize, payload_len);
    *consumed = total;
    *status = Status::Ok();
    return DecodeResult::kOk;
}

}
