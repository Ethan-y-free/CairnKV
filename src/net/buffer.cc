#include "net/buffer.h"

#include <algorithm>
#include <cstring>

namespace cairnkv::net
{

void Buffer::Retrieve(size_t len)
{
    if (len >= ReadableBytes())
    {
        read_index_ = 0;
        write_index_ = 0;
        return;
    }
    read_index_ += len;
}

std::string Buffer::RetrieveAllAsString()
{
    std::string result(Peek(), ReadableBytes());
    read_index_ = 0;
    write_index_ = 0;
    return result;
}

void Buffer::Append(const char* data, size_t len)
{
    EnsureWritable(len);
    if (len > 0)
    {
        std::memcpy(BeginWrite(), data, len);
        HasWritten(len);
    }
}

void Buffer::EnsureWritable(size_t len)
{
    if (WritableBytes() >= len)
    {
        return;
    }

    // Compact first; the readable prefix is usually small.
    if (read_index_ > 0)
    {
        const size_t readable = ReadableBytes();
        std::memmove(data_.data(), data_.data() + read_index_, readable);
        read_index_ = 0;
        write_index_ = readable;
        if (WritableBytes() >= len)
        {
            return;
        }
    }

    data_.resize(std::max(data_.size() * 2, write_index_ + len));
}

}
