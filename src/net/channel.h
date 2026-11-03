#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>

namespace cairnkv::net
{

class EventLoop;

class Channel
{
public:
    Channel(EventLoop* loop, int fd);
    ~Channel();

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    void SetReadCallback(std::function<void()> callback)
    {
        read_callback_ = std::move(callback);
    }

    void SetWriteCallback(std::function<void()> callback)
    {
        write_callback_ = std::move(callback);
    }

    void SetErrorCallback(std::function<void()> callback)
    {
        error_callback_ = std::move(callback);
    }

    void EnableReading();
    void DisableReading();
    void EnableWriting();
    void DisableWriting();
    void DisableAll();

    int fd() const
    {
        return fd_;
    }

    uint32_t events() const
    {
        return events_;
    }

    void SetRevents(uint32_t revents)
    {
        revents_ = revents;
    }

    // Tie the channel lifetime to its owner (for example, a Connection).
    // HandleEvent holds the owner alive for the whole callback.
    void Tie(const std::shared_ptr<void>& owner)
    {
        tie_ = owner;
        tied_ = true;
    }

    void HandleEvent();

private:
    void Update();

    EventLoop* loop_;
    int fd_;
    uint32_t events_ = 0;
    uint32_t revents_ = 0;
    bool tied_ = false;
    std::weak_ptr<void> tie_;

    std::function<void()> read_callback_;
    std::function<void()> write_callback_;
    std::function<void()> error_callback_;
};

}
