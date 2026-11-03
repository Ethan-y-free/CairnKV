#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <unordered_map>
#include <utility>

#include "cairnkv/status.h"
#include "net/buffer.h"

namespace cairnkv::net
{

class Acceptor;
class Connection;
class EventLoop;
class EventLoopThreadPool;

class TcpServer
{
public:
    using ConnectionCallback = std::function<void(const std::shared_ptr<Connection>&)>;
    using MessageCallback =
        std::function<void(const std::shared_ptr<Connection>&, Buffer*)>;

    TcpServer(EventLoop* base_loop, std::string listen_addr, int io_threads);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;

    Status Start();

    void SetConnectionCallback(ConnectionCallback callback);
    void SetMessageCallback(MessageCallback callback);

private:
    void OnNewConnection(int fd, const struct sockaddr_in& peer);
    void RemoveConnection(const std::shared_ptr<Connection>& connection);

    EventLoop* base_loop_;
    std::unique_ptr<Acceptor> acceptor_;
    std::unique_ptr<EventLoopThreadPool> pool_;

    std::mutex mutex_;
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;

    ConnectionCallback connection_callback_;
    MessageCallback message_callback_;
};

}
