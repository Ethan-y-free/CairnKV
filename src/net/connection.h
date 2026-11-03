#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "net/buffer.h"

namespace cairnkv::net
{

class Channel;
class EventLoop;

class Connection : public std::enable_shared_from_this<Connection>
{
public:
    using MessageCallback =
        std::function<void(const std::shared_ptr<Connection>&, Buffer*)>;
    using CloseCallback =
        std::function<void(const std::shared_ptr<Connection>&)>;

    Connection(EventLoop* loop, int fd);
    ~Connection();

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void Start();
    void Send(const std::string& data);
    void Close();

    EventLoop* GetLoop() const
    {
        return loop_;
    }

    int fd() const
    {
        return fd_;
    }

    void SetMessageCallback(MessageCallback callback)
    {
        message_callback_ = std::move(callback);
    }

    void SetCloseCallback(CloseCallback callback)
    {
        close_callback_ = std::move(callback);
    }

private:
    void SendInLoop(const std::string& data);
    void HandleRead();
    void HandleWrite();
    void HandleError();

    EventLoop* loop_;
    int fd_;
    std::unique_ptr<Channel> channel_;
    Buffer input_;
    Buffer output_;
    bool closed_ = false;
    MessageCallback message_callback_;
    CloseCallback close_callback_;
};

}
