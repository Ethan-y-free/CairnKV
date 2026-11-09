#include "net/event_loop.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "common/logging.h"
#include "net/channel.h"

namespace cairnkv::net
{

EventLoop::EventLoop()
    : thread_id_(std::this_thread::get_id())
{
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0)
    {
        CAIRNKV_LOG_ERROR("epoll_create1 failed: " << std::strerror(errno));
        std::abort();
    }

    wakeup_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ < 0)
    {
        CAIRNKV_LOG_ERROR("eventfd failed: " << std::strerror(errno));
        std::abort();
    }

    wakeup_channel_ = std::make_unique<Channel>(this, wakeup_fd_);
    wakeup_channel_->SetReadCallback([this] { HandleWakeup(); });
    wakeup_channel_->EnableReading();
}

EventLoop::~EventLoop()
{
    Stop();
    wakeup_channel_.reset();
    if (wakeup_fd_ >= 0)
    {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
    }
    if (epoll_fd_ >= 0)
    {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

void EventLoop::Run()
{
    std::vector<struct epoll_event> events(64);
    while (running_.load())
    {
        const int count = ::epoll_wait(epoll_fd_,
                                       events.data(),
                                       static_cast<int>(events.size()),
                                       -1);
        if (count < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            CAIRNKV_LOG_ERROR("epoll_wait failed: " << std::strerror(errno));
            break;
        }

        for (int i = 0; i < count && running_.load(); ++i)
        {
            const int fd = events[i].data.fd;
            auto it = channels_.find(fd);
            if (it == channels_.end())
            {
                continue;
            }
            Channel* channel = it->second;
            channel->SetRevents(events[i].events);
            channel->HandleEvent();
        }

        DoPending();
    }

    // Drain queued completions (for example, response writes from RPC workers)
    // while the EventLoop object and its epoll fd are still alive.
    DoPending();
}

void EventLoop::Stop()
{
    running_.store(false);
    Wakeup();
}

void EventLoop::RunInLoop(std::function<void()> callback)
{
    if (IsInLoopThread())
    {
        callback();
        return;
    }
    QueueInLoop(std::move(callback));
}

void EventLoop::QueueInLoop(std::function<void()> callback)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.push_back(std::move(callback));
    }
    Wakeup();
}

bool EventLoop::IsInLoopThread() const
{
    return thread_id_ == std::this_thread::get_id();
}

void EventLoop::UpdateChannel(Channel* channel)
{
    const int fd = channel->fd();
    const uint32_t events = channel->events();

    if (events == 0)
    {
        auto it = channels_.find(fd);
        if (it != channels_.end())
        {
            ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
            channels_.erase(it);
        }
        return;
    }

    struct epoll_event event {};
    event.events = events;
    event.data.fd = fd;

    auto it = channels_.find(fd);
    if (it == channels_.end())
    {
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == 0)
        {
            channels_[fd] = channel;
        }
        return;
    }

    ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event);
}

void EventLoop::RemoveChannel(Channel* channel)
{
    const int fd = channel->fd();
    auto it = channels_.find(fd);
    if (it == channels_.end())
    {
        return;
    }
    ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    channels_.erase(it);
}

void EventLoop::Wakeup()
{
    const uint64_t value = 1;
    const ssize_t n = ::write(wakeup_fd_, &value, sizeof(value));
    (void)n;
}

void EventLoop::HandleWakeup()
{
    uint64_t value = 0;
    while (::read(wakeup_fd_, &value, sizeof(value)) > 0)
    {
    }
}

void EventLoop::DoPending()
{
    std::vector<std::function<void()>> pending;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending.swap(pending_);
    }
    for (auto& callback : pending)
    {
        callback();
    }
}

}
