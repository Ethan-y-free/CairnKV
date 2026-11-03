#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace cairnkv::net
{

class Buffer
{
public:
    Buffer()
        : data_(1024)
    {
    }

    const char* Peek() const
    {
        return data_.data() + read_index_;
    }

    size_t ReadableBytes() const
    {
        return write_index_ - read_index_;
    }

    size_t WritableBytes() const
    {
        return data_.size() - write_index_;
    }

    void Retrieve(size_t len);
    std::string RetrieveAllAsString();

    void Append(const char* data, size_t len);
    void Append(const std::string& data)
    {
        Append(data.data(), data.size());
    }

    void EnsureWritable(size_t len);
    char* BeginWrite()
    {
        return data_.data() + write_index_;
    }

    void HasWritten(size_t len)
    {
        write_index_ += len;
    }

private:
    std::vector<char> data_;
    size_t read_index_ = 0;
    size_t write_index_ = 0;
};

}
