#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "cairnkv/status.h"
#include "cairnkv/types.h"

namespace cairnkv
{

enum class Op : uint8_t
{
    kPut = 1,
    kGet = 2,
    kDelete = 3,
    kScan = 4,
    kStatus = 5
};

struct ClientRequest
{
    Op op = Op::kGet;
    ReadMode read_mode = ReadMode::kLinearizable;
    std::string key;
    std::string value;
    std::string prefix;
    uint32_t limit = 100;
};

struct ClientResponse
{
    StatusCode code = StatusCode::kOk;
    std::string message;
    std::string leader_hint;
    std::string value;
    uint64_t applied_index = 0;
    std::vector<KeyValue> items;
    ClusterStatus cluster;
};

std::string EncodeClientRequest(const ClientRequest& request);
StatusOr<ClientRequest> DecodeClientRequest(std::string_view payload);

std::string EncodeClientResponse(const ClientResponse& response);
StatusOr<ClientResponse> DecodeClientResponse(std::string_view payload);

}
