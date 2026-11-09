#include "raft/peer_client.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <utility>

#include "common/encoding.h"
#include "common/logging.h"
#include "net/socket_utils.h"
#include "rpc/frame.h"

namespace cairnkv
{

namespace
{

constexpr size_t kMaxQueue = 256;
constexpr int kIoTimeoutMs = 300;

Status SendAll(int fd, const char* data, size_t len)
{
    size_t sent = 0;
    while (sent < len)
    {
        const ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0)
        {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR)
        {
            continue;
        }
        return Status(StatusCode::kUnavailable, "peer send failed");
    }
    return Status::Ok();
}

Status RecvExact(int fd, char* data, size_t len)
{
    size_t received = 0;
    while (received < len)
    {
        const ssize_t n = ::recv(fd, data + received, len - received, 0);
        if (n > 0)
        {
            received += static_cast<size_t>(n);
            continue;
        }
        if (n == 0)
        {
            return Status(StatusCode::kUnavailable, "peer closed connection");
        }
        if (errno == EINTR)
        {
            continue;
        }
        return Status(StatusCode::kUnavailable, "peer recv failed");
    }
    return Status::Ok();
}

}

PeerClient::PeerClient(std::string peer_id,
                       std::string address,
                       ResponseCallback callback)
    : peer_id_(std::move(peer_id)),
      address_(std::move(address)),
      callback_(std::move(callback))
{
}

PeerClient::~PeerClient()
{
    Stop();
}

void PeerClient::Start()
{
    if (running_)
    {
        return;
    }
    running_ = true;
    send_thread_ = std::thread([this] { SendLoop(); });
    recv_thread_ = std::thread([this] { ReceiveLoop(); });
}

void PeerClient::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        running_ = false;
    }
    cv_.notify_all();
    if (send_thread_.joinable())
    {
        send_thread_.join();
    }
    if (recv_thread_.joinable())
    {
        recv_thread_.join();
    }

    std::lock_guard<std::mutex> lock(fd_mutex_);
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

bool PeerClient::Send(const RaftMessage& message)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || blocked_)
        {
            return false;
        }
        if (queue_.size() >= kMaxQueue)
        {
            return false;
        }
        queue_.push_back(message);
    }
    cv_.notify_one();
    return true;
}

size_t PeerClient::QueueDepth()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

void PeerClient::SetBlocked(bool blocked)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_ = blocked;
        if (blocked)
        {
            queue_.clear();
        }
    }
    if (!blocked)
    {
        cv_.notify_one();
    }
}

int PeerClient::GetFd()
{
    std::lock_guard<std::mutex> lock(fd_mutex_);
    return fd_;
}

void PeerClient::CloseFd(int expected_fd)
{
    std::lock_guard<std::mutex> lock(fd_mutex_);
    if (fd_ == expected_fd)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

bool PeerClient::Connect()
{
    std::lock_guard<std::mutex> lock(fd_mutex_);
    if (fd_ >= 0)
    {
        return true;
    }

    std::string host;
    uint16_t port = 0;
    if (!net::ParseAddress(address_, &host, &port))
    {
        CAIRNKV_LOG_ERROR("peer " << peer_id_ << " has invalid address "
                                  << address_);
        return false;
    }

    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    const std::string port_text = std::to_string(port);
    struct addrinfo* result = nullptr;
    const int rc = ::getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result);
    if (rc != 0)
    {
        return false;
    }

    int fd = -1;
    for (struct addrinfo* it = result; it != nullptr; it = it->ai_next)
    {
        fd = ::socket(it->ai_family, it->ai_socktype | SOCK_CLOEXEC, it->ai_protocol);
        if (fd < 0)
        {
            continue;
        }
        if (::connect(fd, it->ai_addr, it->ai_addrlen) == 0)
        {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(result);

    if (fd < 0)
    {
        return false;
    }

    int enable = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enable, sizeof(enable));

    struct timeval timeout {};
    timeout.tv_sec = kIoTimeoutMs / 1000;
    timeout.tv_usec = (kIoTimeoutMs % 1000) * 1000;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    fd_ = fd;
    CAIRNKV_LOG_INFO("connected to peer " << peer_id_ << " at " << address_);
    return true;
}

bool PeerClient::SendFrame(int fd, const std::string& frame)
{
    return SendAll(fd, frame.data(), frame.size()).ok();
}

bool PeerClient::ReceiveFrame(int fd, std::string* payload)
{
    std::string header(kFrameHeaderSize, '\0');
    if (!RecvExact(fd, header.data(), header.size()).ok())
    {
        return false;
    }
    if (std::string_view(header).substr(0, kRpcMagic.size()) != kRpcMagic)
    {
        return false;
    }

    size_t pos = 14;   // magic + version + type + request_id
    uint32_t payload_len = 0;
    if (!encoding::ReadU32(header, &pos, &payload_len))
    {
        return false;
    }
    if (payload_len > kMaxFramePayload)
    {
        return false;
    }

    std::string data = header;
    data.resize(kFrameHeaderSize + payload_len + 4);
    if (!RecvExact(fd, data.data() + kFrameHeaderSize, payload_len + 4).ok())
    {
        return false;
    }

    Frame frame;
    size_t consumed = 0;
    Status status;
    const DecodeResult result =
        TryDecodeFrame(data, &frame, &consumed, &status);
    if (result != DecodeResult::kOk ||
        frame.type != MessageType::kRaftResponse)
    {
        return false;
    }
    *payload = std::move(frame.payload);
    return true;
}

void PeerClient::SendLoop()
{
    while (true)
    {
        RaftMessage message;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !running_ || !queue_.empty(); });
            if (!running_)
            {
                break;
            }
            if (blocked_)
            {
                queue_.clear();
                continue;
            }
            message = std::move(queue_.front());
            queue_.pop_front();
        }

        int fd = GetFd();
        if (fd < 0)
        {
            if (!Connect())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            fd = GetFd();
        }

        const std::string payload = EncodeRaftMessage(message);
        const std::string frame = EncodeFrame(
            MessageType::kRaftRequest,
            next_request_id_.fetch_add(1, std::memory_order_relaxed),
            payload);
        if (!SendFrame(fd, frame))
        {
            CloseFd(fd);
        }
    }
}

void PeerClient::ReceiveLoop()
{
    while (true)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_)
            {
                break;
            }
        }

        const int fd = GetFd();
        if (fd < 0)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }

        std::string payload;
        if (!ReceiveFrame(fd, &payload))
        {
            CloseFd(fd);
            continue;
        }

        StatusOr<RaftMessage> response = DecodeRaftMessage(payload);
        if (!response.ok())
        {
            CloseFd(fd);
            continue;
        }

        bool deliver = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            deliver = running_ && !blocked_;
        }
        if (deliver && callback_)
        {
            callback_(*response);
        }
    }
}

}
