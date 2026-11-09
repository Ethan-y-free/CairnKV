#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "raft/raft_message.h"

namespace cairnkv
{

// A blocking, one-in-flight-in-per-peer transport for Raft messages.
// It runs on its own thread so the Raft core never blocks on network I/O.
class PeerClient
{
public:
    using ResponseCallback = std::function<void(const RaftMessage&)>;

    PeerClient(std::string peer_id,
               std::string address,
               ResponseCallback callback);
    ~PeerClient();

    PeerClient(const PeerClient&) = delete;
    PeerClient& operator=(const PeerClient&) = delete;

    void Start();
    void Stop();
    void Send(const RaftMessage& message);

    // Diagnostics: number of messages waiting to be sent to this peer.
    size_t QueueDepth();

    // Test/ops hook: drop outgoing traffic to this peer.
    void SetBlocked(bool blocked);

private:
    void Run();
    bool Connect();
    void Close();
    bool SendFrame(const std::string& frame);
    bool ReceiveFrame(std::string* payload);

    std::string peer_id_;
    std::string address_;
    ResponseCallback callback_;

    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<RaftMessage> queue_;
    bool running_ = false;
    bool blocked_ = false;
    int fd_ = -1;
    uint64_t next_request_id_ = 1;
};

}
