#pragma once

#include <atomic>
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

// Pipelined transport for Raft messages. One thread sends without waiting for
// responses; another receives responses and hands them to the Raft core via
// the callback. The leader bounds the number of in-flight batches, so the send
// queue stays small and messages are not silently dropped.
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

    // Returns false when the client is stopped or blocked, or when the send
    // queue is full. Callers must rewind their optimistic replication state.
    bool Send(const RaftMessage& message);

    // Diagnostics: number of messages waiting to be sent to this peer.
    size_t QueueDepth();

    // Test/ops hook: drop all traffic to and from this peer.
    void SetBlocked(bool blocked);

private:
    void SendLoop();
    void ReceiveLoop();
    bool Connect();
    int GetFd();
    void CloseFd(int expected_fd);
    bool SendFrame(int fd, const std::string& frame);
    bool ReceiveFrame(int fd, std::string* payload);

    std::string peer_id_;
    std::string address_;
    ResponseCallback callback_;

    std::thread send_thread_;
    std::thread recv_thread_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<RaftMessage> queue_;
    bool running_ = false;
    bool blocked_ = false;

    std::mutex fd_mutex_;
    int fd_ = -1;
    std::atomic<uint64_t> next_request_id_{1};
};

}
