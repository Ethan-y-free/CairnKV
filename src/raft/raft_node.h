#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "cairnkv/status.h"
#include "cairnkv/types.h"
#include "raft/hard_state.h"
#include "raft/log_store.h"
#include "raft/raft_message.h"
#include "storage/state_machine.h"

namespace cairnkv
{

class PeerClient;

class RaftNode
{
public:
    struct Options
    {
        std::string node_id;
        std::string listen_addr;
        std::string data_dir;
        std::map<std::string, std::string> peers;   // peer_id -> host:port
        int election_timeout_min_ms = 150;
        int election_timeout_max_ms = 300;
        int heartbeat_interval_ms = 50;
        int client_timeout_ms = 5000;
    };

    explicit RaftNode(Options options);
    ~RaftNode();

    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;

    Status Start();
    void Stop();

    StatusOr<uint64_t> Put(const std::string& key, const std::string& value);
    Status Delete(const std::string& key);
    StatusOr<GetResult> Get(const std::string& key, ReadOptions opts);
    StatusOr<std::vector<KeyValue>> Scan(const std::string& prefix,
                                          uint32_t limit);
    StatusOr<ClusterStatus> GetStatus();

    // Called by RpcServer's I/O thread. Dispatches in the Raft core and waits
    // for the response, so peers always observe a serialized state machine.
    RaftMessage HandleRaftRequest(const RaftMessage& request);

private:
    using Clock = std::chrono::steady_clock;

    void Enqueue(std::function<void()> task);
    void Run();
    void OnTimeout();
    void ResetElectionDeadline();
    void ResetHeartbeatDeadline();

    void OnPeerMessage(const RaftMessage& message);
    RaftMessage DispatchRaftMessage(const RaftMessage& request);
    RaftMessage HandleRequestVote(const RaftMessage& request);
    RaftMessage HandleAppendEntries(const RaftMessage& request);
    void HandleRequestVoteResponse(const RaftMessage& response);
    void HandleAppendEntriesResponse(const RaftMessage& response);

    void StartElection();
    void BecomeFollower(uint64_t term, const std::string& leader_id);
    void BecomeLeader();

    void SendAppendEntries(const std::string& peer_id);
    void BroadcastAppendEntries();
    void AdvanceCommitIndex();
    void ApplyCommitted();
    void FailPendingWrites(const Status& status);

    std::string LeaderAddressUnlocked() const;
    Status NotLeaderStatusUnlocked() const;
    uint64_t Majority() const;
    Status PersistHardStateUnlocked();
    bool IsLogUpToDate(uint64_t last_log_term, uint64_t last_log_index) const;

    StatusOr<uint64_t> ProposeEntry(EntryType type,
                                    const std::string& key,
                                    const std::string& value);

    Options options_;
    LogStore log_;
    StateMachine state_;
    std::string hard_state_path_;

    uint64_t current_term_ = 0;
    std::string voted_for_;
    NodeRole role_ = NodeRole::kFollower;
    std::string leader_id_;
    uint64_t commit_index_ = 0;
    uint64_t last_applied_ = 0;

    std::map<std::string, uint64_t> next_index_;
    std::map<std::string, uint64_t> match_index_;
    std::set<std::string> votes_received_;
    std::map<uint64_t, std::shared_ptr<std::promise<StatusOr<uint64_t>>>>
        pending_writes_;
    std::map<std::string, std::unique_ptr<PeerClient>> peer_clients_;

    std::atomic<uint64_t> term_snapshot_{0};
    std::atomic<bool> running_{false};
    std::thread thread_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;

    Clock::time_point election_deadline_;
    Clock::time_point heartbeat_deadline_;
    std::mt19937 rng_;
};

}
