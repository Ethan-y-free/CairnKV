#include "net/acceptor.h"

#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/logging.h"
#include "net/channel.h"
#include "net/event_loop.h"
#include "net/socket_utils.h"

namespace cairnkv::net
{

Acceptor::Acceptor(EventLoop* loop, std::string listen_addr)
    : loop_(loop), listen_addr_(std::move(listen_addr))
{
}

Acceptor::~Acceptor()
{
    channel_.reset();
    if (listen_fd_ >= 0)
    {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void Acceptor::SetNewConnectionCallback(NewConnectionCallback callback)
{
    callback_ = std::move(callback);
}

Status Acceptor::Start()
{
    std::string host;
    uint16_t port = 0;
    if (!ParseAddress(listen_addr_, &host, &port))
    {
        return Status(StatusCode::kInvalidArgument, "invalid listen address");
    }

    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    const std::string port_text = std::to_string(port);
    struct addrinfo* result = nullptr;
    const int rc = ::getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result);
    if (rc != 0)
    {
        return Status(StatusCode::kInternal,
                      std::string("getaddrinfo: ") + ::gai_strerror(rc));
    }

    for (struct addrinfo* it = result; it != nullptr; it = it->ai_next)
    {
        const int fd = ::socket(it->ai_family,
                                it->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                                it->ai_protocol);
        if (fd < 0)
        {
            continue;
        }
        SetReuseAddr(fd);
        if (::bind(fd, it->ai_addr, it->ai_addrlen) == 0 &&
            ::listen(fd, SOMAXCONN) == 0)
        {
            listen_fd_ = fd;
            break;
        }
        ::close(fd);
    }
    ::freeaddrinfo(result);

    if (listen_fd_ < 0)
    {
        return Status(StatusCode::kInternal,
                      std::string("bind/listen failed: ") + std::strerror(errno));
    }

    channel_ = std::make_unique<Channel>(loop_, listen_fd_);
    channel_->SetReadCallback([this] { HandleRead(); });
    channel_->EnableReading();

    CAIRNKV_LOG_INFO("acceptor listening on " << listen_addr_);
    return Status::Ok();
}

void Acceptor::HandleRead()
{
    while (true)
    {
        struct sockaddr_in peer {};
        socklen_t len = sizeof(peer);
        const int fd = ::accept4(listen_fd_,
                                 reinterpret_cast<struct sockaddr*>(&peer),
                                 &len,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }
            if (errno == EINTR)
            {
                continue;
            }
            CAIRNKV_LOG_WARN("accept failed: " << std::strerror(errno));
            break;
        }

        SetTcpNoDelay(fd);
        if (callback_)
        {
            callback_(fd, peer);
        }
        else
        {
            ::close(fd);
        }
    }
}

}
