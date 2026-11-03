#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace cairnkv::encoding
{

void AppendU8(std::string* out, uint8_t value);
void AppendU16(std::string* out, uint16_t value);
void AppendU32(std::string* out, uint32_t value);
void AppendU64(std::string* out, uint64_t value);
void AppendBytes(std::string* out, const void* data, size_t len);
void AppendString(std::string* out, std::string_view value);

bool ReadU8(std::string_view in, size_t* pos, uint8_t* value);
bool ReadU16(std::string_view in, size_t* pos, uint16_t* value);
bool ReadU32(std::string_view in, size_t* pos, uint32_t* value);
bool ReadU64(std::string_view in, size_t* pos, uint64_t* value);
bool ReadString(std::string_view in, size_t* pos, std::string* value);

uint32_t Crc32(const void* data, size_t len);

}
