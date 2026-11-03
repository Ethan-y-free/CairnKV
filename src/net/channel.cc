#include "net/channel.h"

#include <sys/epoll.h>

#include "net/event_loop.h"

namespace cairnkv::net
{

Channel::Channel(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd)
{
}

Channel::~Channel()
{
    if (loop_ != nullptr)
    {
        loop_->RemoveChannel(this);
    }
}

void Channel::EnableReading()
{
    events_ |= EPOLLIN | EPOLLRDHUP | EPOLLET;
    Update();
}

void Channel::DisableReading()
{
    events_ &= ~(EPOLLIN | EPOLLRDHUP);
    Update();
}

void Channel::EnableWriting()
{
    events_ |= EPOLLOUT | EPOLLET;
    Update();
}

void Channel::DisableWriting()
{
    events_ &= ~EPOLLOUT;
    Update();
}

void Channel::DisableAll()
{
    events_ = 0;
    Update();
}

void Channel::HandleEvent()
{
    // Keep the owner alive: a callback may drop the last external reference.
    std::shared_ptr<void> guard;
    if (tied_)
    {
        guard = tie_.lock();
        if (!guard)
        {
            return;
        }
    }

    if ((revents_ & EPOLLERR) || (revents_ & EPOLLHUP))
    {
        if (error_callback_)
        {
            error_callback_();
        }
        return;
    }

    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP))
    {
        if (read_callback_)
        {
            read_callback_();
        }
        // The callback may have disabled all events (for example, closed the connection).
        if (events_ == 0)
        {
            return;
        }
    }

    if (revents_ & EPOLLOUT)
    {
        if (write_callback_)
        {
            write_callback_();
        }
    }
}

void Channel::Update()
{
    loop_->UpdateChannel(this);
}

}
