#include "cairnkv/status.h"

namespace cairnkv
{

namespace
{

const char* StatusCodeName(StatusCode code)
{
    switch (code)
    {
        case StatusCode::kOk:
            return "OK";
        case StatusCode::kNotFound:
            return "NOT_FOUND";
        case StatusCode::kInvalidArgument:
            return "INVALID_ARGUMENT";
        case StatusCode::kNotLeader:
            return "NOT_LEADER";
        case StatusCode::kTimeout:
            return "TIMEOUT";
        case StatusCode::kUnavailable:
            return "UNAVAILABLE";
        case StatusCode::kBusy:
            return "BUSY";
        case StatusCode::kCorruption:
            return "CORRUPTION";
        case StatusCode::kInternal:
            return "INTERNAL";
        case StatusCode::kUnimplemented:
            return "UNIMPLEMENTED";
    }
    return "UNKNOWN";
}

}

std::string Status::ToString() const
{
    std::string result = StatusCodeName(code_);
    if (!message_.empty())
    {
        result += ": ";
        result += message_;
    }
    return result;
}

}
