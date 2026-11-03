#include "cairnkv/client.h"

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <utility>

#include "common/encoding.h"
#include "rpc/frame.h"
#include "rpc/message.h"

namespace cairnkv
{

namespace
{

constexpr size_t kMaxFrameSize = kFrameHeaderSize + kMaxFramePayload + 4;

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
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            return Status(StatusCode::kTimeout, "client send timeout");
        }
        return Status(StatusCode::kUnavailable, "client send failed");
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
            return Status(StatusCode::kUnavailable, "server closed connection");
        }
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            return Status(StatusCode::kTimeout, "client recv timeout");
        }
        return Status(StatusCode::kUnavailable, "client recv failed");
    }
    return Status::Ok();
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

    char* end = nullptr;
    errno = 0;
    const long parsed = std::strtol(port_part.c_str(), &end, 10);
    if (errno != 0 || end == nullptr || *end != '\0' || parsed <= 0 ||
        parsed > 65535)
    {
        return false;
    }
    *host = host_part.empty() ? "127.0.0.1" : host_part;
    *port = static_cast<uint16_t>(parsed);
    return true;
}

}

struct Client::Impl
{
    int fd = -1;
    std::string host;
    uint16_t port = 0;
    int timeout_ms = 1000;
    std::mutex mutex;
    std::atomic<uint64_t> next_request_id{1};

    ~Impl()
    {
        Close();
    }

    void Close()
    {
        if (fd >= 0)
        {
            ::close(fd);
            fd = -1;
        }
    }

    Status Connect();
    Status SendRequest(uint64_t request_id, const ClientRequest& request);
    Status ReceiveResponse(uint64_t request_id, ClientResponse* response);
    Status Rpc(const ClientRequest& request, ClientResponse* response);
};

Client::Client()
    : impl_(std::make_unique<Impl>())
{
}

Client::~Client() = default;

Status Client::Impl::Connect()
{
    Close();

    struct addrinfo hints {};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    const std::string port_text = std::to_string(port);
    struct addrinfo* result = nullptr;
    const int rc = ::getaddrinfo(host.c_str(), port_text.c_str(), &hints, &result);
    if (rc != 0)
    {
        return Status(StatusCode::kUnavailable,
                      std::string("getaddrinfo: ") + ::gai_strerror(rc));
    }

    int socket_fd = -1;
    for (struct addrinfo* it = result; it != nullptr; it = it->ai_next)
    {
        socket_fd =
            ::socket(it->ai_family, it->ai_socktype | SOCK_CLOEXEC, it->ai_protocol);
        if (socket_fd < 0)
        {
            continue;
        }
        if (::connect(socket_fd, it->ai_addr, it->ai_addrlen) == 0)
        {
            break;
        }
        ::close(socket_fd);
        socket_fd = -1;
    }
    ::freeaddrinfo(result);

    if (socket_fd < 0)
    {
        return Status(StatusCode::kUnavailable,
                      "connect to CairnKV server failed");
    }

    int enable = 1;
    ::setsockopt(socket_fd,
                 IPPROTO_TCP,
                 TCP_NODELAY,
                 &enable,
                 sizeof(enable));

    struct timeval timeout {};
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    ::setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(socket_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    fd = socket_fd;
    return Status::Ok();
}

StatusOr<std::unique_ptr<Client>> Client::Open(const ClientOptions& opts)
{
    auto client = std::unique_ptr<Client>(new Client());
    client->impl_->host = opts.host;
    client->impl_->port = opts.port;
    client->impl_->timeout_ms = opts.timeout_ms;

    const Status status = client->impl_->Connect();
    if (!status.ok())
    {
        return status;
    }
    return client;
}

Status Client::Impl::SendRequest(uint64_t request_id,
                                 const ClientRequest& request)
{
    const std::string payload = EncodeClientRequest(request);
    const std::string frame =
        EncodeFrame(MessageType::kClientRequest, request_id, payload);
    return SendAll(fd, frame.data(), frame.size());
}

Status Client::Impl::ReceiveResponse(uint64_t request_id,
                                     ClientResponse* response)
{
    std::string header(kFrameHeaderSize, '\0');
    Status status = RecvExact(fd, header.data(), header.size());
    if (!status.ok())
    {
        return status;
    }

    if (std::string_view(header).substr(0, kRpcMagic.size()) != kRpcMagic)
    {
        return Status(StatusCode::kCorruption, "bad response magic");
    }

    size_t pos = 14;   // magic(4) + version(1) + type(1) + request_id(8)
    uint32_t payload_len = 0;
    if (!encoding::ReadU32(header, &pos, &payload_len))
    {
        return Status(StatusCode::kCorruption, "bad response header");
    }
    if (payload_len > kMaxFramePayload ||
        kFrameHeaderSize + payload_len + 4 > kMaxFrameSize)
    {
        return Status(StatusCode::kCorruption, "response frame too large");
    }

    std::string tail(payload_len + 4, '\0');
    status = RecvExact(fd, tail.data(), tail.size());
    if (!status.ok())
    {
        return status;
    }

    std::string data = header + tail;
    Frame frame;
    size_t consumed = 0;
    Status decode_status;
    const DecodeResult result =
        TryDecodeFrame(data, &frame, &consumed, &decode_status);
    if (result == DecodeResult::kNeedMore)
    {
        return Status(StatusCode::kCorruption, "incomplete response frame");
    }
    if (result == DecodeResult::kError)
    {
        return decode_status;
    }
    if (frame.type != MessageType::kClientResponse)
    {
        return Status(StatusCode::kCorruption, "unexpected response type");
    }
    if (frame.request_id != request_id)
    {
        return Status(StatusCode::kInternal, "response request_id mismatch");
    }

    StatusOr<ClientResponse> decoded = DecodeClientResponse(frame.payload);
    if (!decoded.ok())
    {
        return decoded.status();
    }
    *response = std::move(*decoded);
    return Status::Ok();
}

Status Client::Impl::Rpc(const ClientRequest& request, ClientResponse* response)
{
    std::lock_guard<std::mutex> lock(mutex);

    Status last_status(StatusCode::kUnavailable, "client is not connected");
    for (int attempt = 0; attempt < 3; ++attempt)
    {
        if (fd < 0)
        {
            const Status connect_status = Connect();
            if (!connect_status.ok())
            {
                return connect_status;
            }
        }

        const uint64_t request_id = next_request_id.fetch_add(1);
        Status status = SendRequest(request_id, request);
        if (!status.ok())
        {
            last_status = status;
            Close();
            continue;
        }

        ClientResponse received;
        status = ReceiveResponse(request_id, &received);
        if (!status.ok())
        {
            last_status = status;
            Close();
            continue;
        }

        if (received.code == StatusCode::kNotLeader &&
            !received.leader_hint.empty())
        {
            std::string new_host;
            uint16_t new_port = 0;
            if (!ParseAddress(received.leader_hint, &new_host, &new_port))
            {
                return Status(StatusCode::kNotLeader, received.message);
            }
            last_status = Status(StatusCode::kNotLeader, received.message);
            host = new_host;
            port = new_port;
            Close();
            continue;
        }

        if (received.code != StatusCode::kOk)
        {
            return Status(received.code, received.message);
        }

        *response = std::move(received);
        return Status::Ok();
    }

    return last_status;
}

Status Client::Put(const std::string& key, const std::string& value)
{
    ClientRequest request;
    request.op = Op::kPut;
    request.key = key;
    request.value = value;

    ClientResponse response;
    return impl_->Rpc(request, &response);
}

StatusOr<GetResult> Client::Get(const std::string& key, ReadOptions opts)
{
    ClientRequest request;
    request.op = Op::kGet;
    request.read_mode = opts.mode;
    request.key = key;

    ClientResponse response;
    const Status status = impl_->Rpc(request, &response);
    if (!status.ok())
    {
        return status;
    }
    return GetResult{response.value, response.applied_index};
}

Status Client::Delete(const std::string& key)
{
    ClientRequest request;
    request.op = Op::kDelete;
    request.key = key;

    ClientResponse response;
    return impl_->Rpc(request, &response);
}

StatusOr<std::vector<KeyValue>> Client::Scan(const std::string& prefix,
                                              ScanOptions opts)
{
    ClientRequest request;
    request.op = Op::kScan;
    request.prefix = prefix;
    request.limit = opts.limit;

    ClientResponse response;
    const Status status = impl_->Rpc(request, &response);
    if (!status.ok())
    {
        return status;
    }
    return response.items;
}

StatusOr<ClusterStatus> Client::GetStatus()
{
    ClientRequest request;
    request.op = Op::kStatus;

    ClientResponse response;
    const Status status = impl_->Rpc(request, &response);
    if (!status.ok())
    {
        return status;
    }
    return response.cluster;
}

Status Client::SetBlockedPeers(const std::vector<std::string>& peer_ids)
{
    ClientRequest request;
    request.op = peer_ids.empty() ? Op::kUnpartition : Op::kPartition;

    std::string joined;
    for (const auto& peer_id : peer_ids)
    {
        if (!joined.empty())
        {
            joined += ',';
        }
        joined += peer_id;
    }
    request.blocked_peers = std::move(joined);

    ClientResponse response;
    return impl_->Rpc(request, &response);
}

}
