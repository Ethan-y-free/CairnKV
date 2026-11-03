#include "common/encoding.h"

#include <array>
#include <cstring>

namespace cairnkv::encoding
{

namespace
{

const std::array<uint32_t, 256>& Crc32Table()
{
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> result{};
        for (uint32_t i = 0; i < 256; ++i)
        {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k)
            {
                c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            result[i] = c;
        }
        return result;
    }();
    return table;
}

bool CheckRange(std::string_view in, size_t pos, size_t len)
{
    return pos <= in.size() && len <= in.size() - pos;
}

}

void AppendU8(std::string* out, uint8_t value)
{
    out->push_back(static_cast<char>(value));
}

void AppendU16(std::string* out, uint16_t value)
{
    AppendU8(out, static_cast<uint8_t>(value & 0xFFu));
    AppendU8(out, static_cast<uint8_t>((value >> 8) & 0xFFu));
}

void AppendU32(std::string* out, uint32_t value)
{
    for (int i = 0; i < 4; ++i)
    {
        AppendU8(out, static_cast<uint8_t>((value >> (8 * i)) & 0xFFu));
    }
}

void AppendU64(std::string* out, uint64_t value)
{
    for (int i = 0; i < 8; ++i)
    {
        AppendU8(out, static_cast<uint8_t>((value >> (8 * i)) & 0xFFu));
    }
}

void AppendBytes(std::string* out, const void* data, size_t len)
{
    if (len == 0)
    {
        return;
    }
    out->append(static_cast<const char*>(data), len);
}

void AppendString(std::string* out, std::string_view value)
{
    AppendU32(out, static_cast<uint32_t>(value.size()));
    AppendBytes(out, value.data(), value.size());
}

bool ReadU8(std::string_view in, size_t* pos, uint8_t* value)
{
    if (!CheckRange(in, *pos, 1))
    {
        return false;
    }
    *value = static_cast<uint8_t>(in[*pos]);
    ++(*pos);
    return true;
}

bool ReadU16(std::string_view in, size_t* pos, uint16_t* value)
{
    if (!CheckRange(in, *pos, 2))
    {
        return false;
    }
    uint16_t result = 0;
    for (int i = 0; i < 2; ++i)
    {
        result |= static_cast<uint16_t>(static_cast<uint8_t>(in[*pos + i])) << (8 * i);
    }
    *pos += 2;
    *value = result;
    return true;
}

bool ReadU32(std::string_view in, size_t* pos, uint32_t* value)
{
    if (!CheckRange(in, *pos, 4))
    {
        return false;
    }
    uint32_t result = 0;
    for (int i = 0; i < 4; ++i)
    {
        result |= static_cast<uint32_t>(static_cast<uint8_t>(in[*pos + i])) << (8 * i);
    }
    *pos += 4;
    *value = result;
    return true;
}

bool ReadU64(std::string_view in, size_t* pos, uint64_t* value)
{
    if (!CheckRange(in, *pos, 8))
    {
        return false;
    }
    uint64_t result = 0;
    for (int i = 0; i < 8; ++i)
    {
        result |= static_cast<uint64_t>(static_cast<uint8_t>(in[*pos + i])) << (8 * i);
    }
    *pos += 8;
    *value = result;
    return true;
}

bool ReadString(std::string_view in, size_t* pos, std::string* value)
{
    uint32_t len = 0;
    if (!ReadU32(in, pos, &len))
    {
        return false;
    }
    if (!CheckRange(in, *pos, len))
    {
        return false;
    }
    value->assign(in.data() + *pos, len);
    *pos += len;
    return true;
}

uint32_t Crc32(const void* data, size_t len)
{
    const auto& table = Crc32Table();
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i)
    {
        crc = table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

}
