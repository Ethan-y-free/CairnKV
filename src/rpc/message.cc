#include "rpc/message.h"

#include "common/encoding.h"

namespace cairnkv
{

namespace
{

constexpr uint32_t kMaxMessageString = 4u * 1024u * 1024u;
constexpr uint32_t kMaxScanItems = 1000;

StatusOr<std::string> ReadBoundedString(std::string_view payload, size_t* pos)
{
    uint32_t len = 0;
    if (!encoding::ReadU32(payload, pos, &len))
    {
        return Status(StatusCode::kCorruption, "truncated string length");
    }
    if (len > kMaxMessageString)
    {
        return Status(StatusCode::kCorruption, "message string too large");
    }
    if (*pos > payload.size() || len > payload.size() - *pos)
    {
        return Status(StatusCode::kCorruption, "truncated string body");
    }
    std::string value(payload.data() + *pos, len);
    *pos += len;
    return value;
}

}

std::string EncodeClientRequest(const ClientRequest& request)
{
    std::string out;
    encoding::AppendU8(&out, static_cast<uint8_t>(request.op));
    encoding::AppendU8(&out, static_cast<uint8_t>(request.read_mode));
    encoding::AppendString(&out, request.key);
    encoding::AppendString(&out, request.value);
    encoding::AppendString(&out, request.prefix);
    encoding::AppendU32(&out, request.limit);
    return out;
}

StatusOr<ClientRequest> DecodeClientRequest(std::string_view payload)
{
    size_t pos = 0;
    uint8_t op = 0;
    uint8_t mode = 0;
    if (!encoding::ReadU8(payload, &pos, &op) ||
        !encoding::ReadU8(payload, &pos, &mode))
    {
        return Status(StatusCode::kCorruption, "truncated client request");
    }
    if (op < static_cast<uint8_t>(Op::kPut) ||
        op > static_cast<uint8_t>(Op::kStatus))
    {
        return Status(StatusCode::kInvalidArgument, "invalid operation");
    }
    if (mode > static_cast<uint8_t>(ReadMode::kStale))
    {
        return Status(StatusCode::kInvalidArgument, "invalid read mode");
    }

    ClientRequest request;
    request.op = static_cast<Op>(op);
    request.read_mode = static_cast<ReadMode>(mode);

    StatusOr<std::string> key = ReadBoundedString(payload, &pos);
    if (!key.ok())
    {
        return key.status();
    }
    request.key = *key;

    StatusOr<std::string> value = ReadBoundedString(payload, &pos);
    if (!value.ok())
    {
        return value.status();
    }
    request.value = *value;

    StatusOr<std::string> prefix = ReadBoundedString(payload, &pos);
    if (!prefix.ok())
    {
        return prefix.status();
    }
    request.prefix = *prefix;

    if (!encoding::ReadU32(payload, &pos, &request.limit))
    {
        return Status(StatusCode::kCorruption, "truncated scan limit");
    }
    if (pos != payload.size())
    {
        return Status(StatusCode::kCorruption, "trailing bytes in client request");
    }
    return request;
}

std::string EncodeClientResponse(const ClientResponse& response)
{
    std::string out;
    encoding::AppendU8(&out, static_cast<uint8_t>(response.code));
    encoding::AppendString(&out, response.message);
    encoding::AppendString(&out, response.leader_hint);
    encoding::AppendString(&out, response.value);
    encoding::AppendU64(&out, response.applied_index);
    encoding::AppendU32(&out, static_cast<uint32_t>(response.items.size()));
    for (const auto& item : response.items)
    {
        encoding::AppendString(&out, item.key);
        encoding::AppendString(&out, item.value);
    }
    encoding::AppendU8(&out, static_cast<uint8_t>(response.cluster.role));
    encoding::AppendU64(&out, response.cluster.term);
    encoding::AppendString(&out, response.cluster.leader_id);
    encoding::AppendU64(&out, response.cluster.commit_index);
    encoding::AppendU64(&out, response.cluster.last_applied);
    encoding::AppendU64(&out, response.cluster.log_last_index);
    encoding::AppendU64(&out, response.cluster.wal_bytes);
    return out;
}

StatusOr<ClientResponse> DecodeClientResponse(std::string_view payload)
{
    size_t pos = 0;
    uint8_t code = 0;
    if (!encoding::ReadU8(payload, &pos, &code))
    {
        return Status(StatusCode::kCorruption, "truncated client response");
    }
    if (code > static_cast<uint8_t>(StatusCode::kUnimplemented))
    {
        return Status(StatusCode::kCorruption, "invalid status code");
    }

    ClientResponse response;
    response.code = static_cast<StatusCode>(code);

    StatusOr<std::string> message = ReadBoundedString(payload, &pos);
    if (!message.ok())
    {
        return message.status();
    }
    response.message = *message;

    StatusOr<std::string> leader_hint = ReadBoundedString(payload, &pos);
    if (!leader_hint.ok())
    {
        return leader_hint.status();
    }
    response.leader_hint = *leader_hint;

    StatusOr<std::string> value = ReadBoundedString(payload, &pos);
    if (!value.ok())
    {
        return value.status();
    }
    response.value = *value;

    if (!encoding::ReadU64(payload, &pos, &response.applied_index))
    {
        return Status(StatusCode::kCorruption, "truncated applied_index");
    }

    uint32_t item_count = 0;
    if (!encoding::ReadU32(payload, &pos, &item_count))
    {
        return Status(StatusCode::kCorruption, "truncated item count");
    }
    if (item_count > kMaxScanItems)
    {
        return Status(StatusCode::kCorruption, "too many items in response");
    }
    response.items.reserve(item_count);
    for (uint32_t i = 0; i < item_count; ++i)
    {
        StatusOr<std::string> key = ReadBoundedString(payload, &pos);
        if (!key.ok())
        {
            return key.status();
        }
        StatusOr<std::string> item_value = ReadBoundedString(payload, &pos);
        if (!item_value.ok())
        {
            return item_value.status();
        }
        response.items.push_back(KeyValue{*key, *item_value});
    }

    uint8_t role = 0;
    if (!encoding::ReadU8(payload, &pos, &role))
    {
        return Status(StatusCode::kCorruption, "truncated cluster role");
    }
    if (role > static_cast<uint8_t>(NodeRole::kLeader))
    {
        return Status(StatusCode::kCorruption, "invalid cluster role");
    }
    response.cluster.role = static_cast<NodeRole>(role);

    if (!encoding::ReadU64(payload, &pos, &response.cluster.term))
    {
        return Status(StatusCode::kCorruption, "truncated cluster term");
    }
    StatusOr<std::string> leader_id = ReadBoundedString(payload, &pos);
    if (!leader_id.ok())
    {
        return leader_id.status();
    }
    response.cluster.leader_id = *leader_id;

    if (!encoding::ReadU64(payload, &pos, &response.cluster.commit_index) ||
        !encoding::ReadU64(payload, &pos, &response.cluster.last_applied) ||
        !encoding::ReadU64(payload, &pos, &response.cluster.log_last_index) ||
        !encoding::ReadU64(payload, &pos, &response.cluster.wal_bytes))
    {
        return Status(StatusCode::kCorruption, "truncated cluster status");
    }
    if (pos != payload.size())
    {
        return Status(StatusCode::kCorruption,
                      "trailing bytes in client response");
    }
    return response;
}

}
