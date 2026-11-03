#pragma once

#include <functional>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <utility>

#include "cairnkv/status.h"

namespace cairnkv::net
{

class Channel;
class EventLoop;

class Acceptor
{
public:
    using NewConnectionCallback =
        std::function<void(int fd, const struct sockaddr_in& peer)>;

    Acceptor(EventLoop* loop, std::string listen_addr);
    ~Acceptor();

    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    Status Start();
    void SetNewConnectionCallback(NewConnectionCallback callback);

    int fd() const
    {
        return listen_fd_;
    }

private:
    void HandleRead();

    EventLoop* loop_;
    std::string listen_addr_;
    int listen_fd_ = -1;
    std::unique_ptr<Channel> channel_;
    NewConnectionCallback callback_;
};

}
