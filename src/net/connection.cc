#include "net/connection.h"

#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

#include "common/logging.h"
#include "net/channel.h"
#include "net/event_loop.h"

namespace cairnkv::net
{

namespace
{

constexpr size_t kMaxInputBuffer = 16u * 1024u * 1024u;
constexpr size_t kMaxOutputBuffer = 16u * 1024u * 1024u;
constexpr size_t kReadChunk = 64u * 1024u;

}

Connection::Connection(EventLoop* loop, int fd)
    : loop_(loop), fd_(fd), channel_(std::make_unique<Channel>(loop, fd))
{
}

Connection::~Connection()
{
    channel_.reset();
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::Start()
{
    channel_->Tie(shared_from_this());
    channel_->SetReadCallback([this] { HandleRead(); });
    channel_->SetWriteCallback([this] { HandleWrite(); });
    channel_->SetErrorCallback([this] { HandleError(); });
    channel_->EnableReading();
}

void Connection::Send(const std::string& data)
{
    // Send is callable from RPC worker threads. closed_ is only touched on the
    // EventLoop thread, so the actual check happens in SendInLoop.
    if (loop_->IsInLoopThread())
    {
        SendInLoop(data);
        return;
    }
    auto self = shared_from_this();
    loop_->RunInLoop([self, data] { self->SendInLoop(data); });
}

void Connection::SendInLoop(const std::string& data)
{
    if (closed_)
    {
        return;
    }
    if (output_.ReadableBytes() + data.size() > kMaxOutputBuffer)
    {
        // Backpressure: a slow peer must not grow this buffer without bound.
        CAIRNKV_LOG_WARN("connection output buffer overflow, closing fd=" << fd_);
        Close();
        return;
    }
    output_.Append(data);
    if (output_.ReadableBytes() > 0)
    {
        channel_->EnableWriting();
    }
}

void Connection::Close()
{
    if (closed_)
    {
        return;
    }
    closed_ = true;

    auto self = shared_from_this();
    if (channel_)
    {
        channel_->DisableAll();
    }
    if (close_callback_)
    {
        close_callback_(self);
    }

    // Keep channel_ alive until the destructor: HandleEvent may still be on
    // the stack, and the Channel must not be destroyed under it.
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

void Connection::HandleRead()
{
    auto self = shared_from_this();
    char buffer[kReadChunk];

    while (true)
    {
        const ssize_t n = ::recv(fd_, buffer, sizeof(buffer), 0);
        if (n > 0)
        {
            input_.Append(buffer, static_cast<size_t>(n));
            if (input_.ReadableBytes() > kMaxInputBuffer)
            {
                CAIRNKV_LOG_WARN("connection input buffer overflow, closing fd="
                                 << fd_);
                Close();
                return;
            }
            continue;
        }
        if (n == 0)
        {
            Close();
            return;
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            break;
        }
        Close();
        return;
    }

    if (message_callback_ && !closed_)
    {
        message_callback_(self, &input_);
    }
}

void Connection::HandleWrite()
{
    auto self = shared_from_this();

    while (output_.ReadableBytes() > 0)
    {
        const ssize_t n = ::send(fd_,
                                 output_.Peek(),
                                 output_.ReadableBytes(),
                                 MSG_NOSIGNAL);
        if (n > 0)
        {
            output_.Retrieve(static_cast<size_t>(n));
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            return;
        }
        Close();
        return;
    }

    if (output_.ReadableBytes() == 0)
    {
        channel_->DisableWriting();
    }
}

void Connection::HandleError()
{
    CAIRNKV_LOG_WARN("connection error or hangup, closing fd=" << fd_);
    Close();
}

}
