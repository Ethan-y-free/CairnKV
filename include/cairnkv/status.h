#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace cairnkv
{

enum class StatusCode : uint8_t
{
    kOk = 0,
    kNotFound = 1,
    kInvalidArgument = 2,
    kNotLeader = 3,
    kTimeout = 4,
    kUnavailable = 5,
    kBusy = 6,
    kCorruption = 7,
    kInternal = 8,
    kUnimplemented = 9
};

class Status
{
public:
    Status() = default;

    Status(StatusCode code, std::string message)
        : code_(code), message_(std::move(message))
    {
    }

    static Status Ok()
    {
        return Status();
    }

    bool ok() const
    {
        return code_ == StatusCode::kOk;
    }

    StatusCode code() const
    {
        return code_;
    }

    const std::string& message() const
    {
        return message_;
    }

    std::string ToString() const;

private:
    StatusCode code_ = StatusCode::kOk;
    std::string message_;
};

template <typename T>
class StatusOr
{
public:
    StatusOr(Status status)
        : status_(std::move(status))
    {
    }

    StatusOr(T value)
        : value_(std::move(value))
    {
    }

    bool ok() const
    {
        return value_.has_value();
    }

    const Status& status() const
    {
        return status_;
    }

    T& value()
    {
        return *value_;
    }

    const T& value() const
    {
        return *value_;
    }

    T& operator*()
    {
        return *value_;
    }

    const T& operator*() const
    {
        return *value_;
    }

    T* operator->()
    {
        return &*value_;
    }

    const T* operator->() const
    {
        return &*value_;
    }

private:
    Status status_;
    std::optional<T> value_;
};

#define CAIRNKV_RETURN_IF_ERROR(expr)          \
    do                                         \
    {                                          \
        ::cairnkv::Status _cairnkv_s = (expr); \
        if (!_cairnkv_s.ok())                  \
        {                                      \
            return _cairnkv_s;                 \
        }                                      \
    } while (0)

}
