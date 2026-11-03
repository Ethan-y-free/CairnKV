#pragma once

#include <cstdint>
#include <string>

namespace cairnkv::net
{

int SetNonBlocking(int fd);
int SetTcpNoDelay(int fd);
int SetReuseAddr(int fd);

bool ParseAddress(const std::string& address, std::string* host, uint16_t* port);

}
