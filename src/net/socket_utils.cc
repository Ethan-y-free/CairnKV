#include "net/socket_utils.h"

#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace cairnkv::net
{

int SetNonBlocking(int fd)
{
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0)
    {
        return -1;
    }
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int SetTcpNoDelay(int fd)
{
    int enable = 1;
    return ::setsockopt(fd,
                        IPPROTO_TCP,
                        TCP_NODELAY,
                        &enable,
                        sizeof(enable));
}

int SetReuseAddr(int fd)
{
    int enable = 1;
    return ::setsockopt(fd,
                        SOL_SOCKET,
                        SO_REUSEADDR,
                        &enable,
                        sizeof(enable));
}

bool ParseAddress(const std::string& address, std::string* host, uint16_t* port)
{
    const size_t pos = address.find_last_of(':');
    if (pos == std::string::npos || pos + 1 >= address.size())
    {
        return false;
    }

    const std::string host_part = address.substr(0, pos);
    const std::string port_part = address.substr(pos + 1);
    if (port_part.empty())
    {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(port_part.c_str(), &end, 10);
    if (errno != 0 || end == nullptr || *end != '\0' || parsed <= 0 ||
        parsed > 65535)
    {
        return false;
    }

    *host = host_part.empty() ? "0.0.0.0" : host_part;
    *port = static_cast<uint16_t>(parsed);
    return true;
}

}
