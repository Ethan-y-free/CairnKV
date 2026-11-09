#include "raft/raft_node.h"

#include <algorithm>
#include <filesystem>
#include <utility>

#include "common/logging.h"
#include "raft/peer_client.h"

namespace cairnkv
{

namespace
{

constexpr auto kRaftRequestTimeout = std::chrono::milliseconds(1000);

uint64_t DurationNs(std::chrono::steady_clock::time_point start,
                    std::chrono::steady_clock::time_point end)
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
            .count());
}

}

RaftNode::RaftNode(Options options)
    : options_(std::move(options))
{
    std::random_device random_device;
    rng_.seed(random_device() ^
              static_cast<uint32_t>(std::hash<std::string>{}(options_.node_id)));
    hard_state_path_ = options_.data_dir + "/hard_state";
}

RaftNode::~RaftNode()
{
    Stop();
}

Status RaftNode::Start()
{
    if (running_.load())
    {
        return Status(StatusCode::kInvalidArgument, "raft node already started");
    }

    std::error_code ec;
    std::filesystem::create_directories(options_.data_dir, ec);
    if (ec)
    {
        return Status(StatusCode::kInternal,
                      "create raft data dir: " + ec.message());
    }

    hard_state_path_ = options_.data_dir + "/hard_state";
    CAIRNKV_RETURN_IF_ERROR(log_.Open(options_.data_dir + "/raft.log"));
    CAIRNKV_RETURN_IF_ERROR(state_.Open(options_.data_dir + "/state"));

    StatusOr<HardState> hard_state = LoadHardState(hard_state_path_);
    if (!hard_state.ok())
    {
        return hard_state.status();
    }
    current_term_ = hard_state->current_term;
    voted_for_ = hard_state->voted_for;
    term_snapshot_.store(current_term_);

    last_applied_ = state_.LastApplied();
    if (last_applied_ > log_.LastIndex())
    {
        return Status(StatusCode::kCorruption,
                      "state machine is ahead of the Raft log");
    }
    commit_index_ = last_applied_;

    role_ = NodeRole::kFollower;
    leader_id_.clear();
    running_.store(true);

    for (const auto& [peer_id, address] : options_.peers)
    {
        auto client = std::make_unique<PeerClient>(
            peer_id, address, [this](const RaftMessage& message) {
                OnPeerMessage(message);
            });
        client->Start();
        peer_clients_[peer_id] = std::move(client);
    }

    last_metrics_log_ = Clock::now();
    thread_ = std::thread([this] { Run(); });
    return Status::Ok();
}

void RaftNode::Stop()
{
    running_.store(false);
    cv_.notify_all();
    if (thread_.joinable())
    {
        thread_.join();
    }

    for (auto& [peer_id, client] : peer_clients_)
    {
        (void)peer_id;
        client->Stop();
    }

    FailPendingWrites(Status(StatusCode::kUnavailable, "raft node stopped"));
    FailReadRequests(Status(StatusCode::kUnavailable, "raft node stopped"));
}

void RaftNode::Enqueue(std::function<void()> task)
{
    if (!running_.load())
    {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back(std::move(task));
    }
    cv_.notify_one();
}

void RaftNode::Run()
{
    ResetElectionDeadline();

    while (running_.load())
    {
        const Clock::time_point metrics_now = Clock::now();
        if (metrics_now - last_metrics_log_ >= std::chrono::seconds(1))
        {
            LogMetrics();
            last_metrics_log_ = metrics_now;
        }

        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (!running_.load())
            {
                break;
            }

            const auto now = Clock::now();
            const auto next = (role_ == NodeRole::kLeader)
                                  ? heartbeat_deadline_
                                  : election_deadline_;
            if (now >= next)
            {
                lock.unlock();
                OnTimeout();
                continue;
            }

            if (!queue_.empty())
            {
                task = std::move(queue_.front());
                queue_.pop_front();
            }
            else
            {
                cv_.wait_until(lock, next);
                continue;
            }
        }
        task();
    }
}

void RaftNode::OnTimeout()
{
    const auto now = Clock::now();
    if (role_ == NodeRole::kLeader)
    {
        if (now >= heartbeat_deadline_)
        {
            BroadcastAppendEntries();
            ResetHeartbeatDeadline();
        }
        return;
    }

    if (now >= election_deadline_)
    {
        StartElection();
    }
}

void RaftNode::LogMetrics()
{
    const uint64_t proposals =
        metrics_.proposals.load(std::memory_order_relaxed);
    const uint64_t sync_calls =
        metrics_.sync_calls.load(std::memory_order_relaxed);
    const uint64_t sync_ns = metrics_.sync_ns.load(std::memory_order_relaxed);
    const uint64_t apply_calls =
        metrics_.apply_calls.load(std::memory_order_relaxed);
    const uint64_t apply_ns =
        metrics_.apply_ns.load(std::memory_order_relaxed);
    const uint64_t appends_sent =
        metrics_.appends_sent.load(std::memory_order_relaxed);
    const uint64_t sends_empty =
        metrics_.sends_empty.load(std::memory_order_relaxed);
    const uint64_t sends_with_entries =
        metrics_.sends_with_entries.load(std::memory_order_relaxed);
    const uint64_t responses =
        metrics_.responses.load(std::memory_order_relaxed);
    const uint64_t responses_success =
        metrics_.responses_success.load(std::memory_order_relaxed);
    const uint64_t responses_failure =
        metrics_.responses_failure.load(std::memory_order_relaxed);
    const uint64_t append_requests =
        metrics_.append_requests.load(std::memory_order_relaxed);
    const uint64_t reject_prev =
        metrics_.reject_prev.load(std::memory_order_relaxed);
    const uint64_t reject_term =
        metrics_.reject_term.load(std::memory_order_relaxed);
    const uint64_t reject_gap =
        metrics_.reject_gap.load(std::memory_order_relaxed);
    const uint64_t appended_entries =
        metrics_.appended_entries.load(std::memory_order_relaxed);

    const uint64_t sync_delta = sync_calls - last_logged_sync_calls_;
    const uint64_t sync_ns_delta = sync_ns - last_logged_sync_ns_;
    const uint64_t apply_delta = apply_calls - last_logged_apply_calls_;
    const uint64_t apply_ns_delta = apply_ns - last_logged_apply_ns_;
    const uint64_t appends_delta = appends_sent - last_logged_appends_sent_;
    const uint64_t sends_empty_delta = sends_empty - last_logged_sends_empty_;
    const uint64_t sends_entries_delta =
        sends_with_entries - last_logged_sends_with_entries_;
    const uint64_t responses_delta = responses - last_logged_responses_;
    const uint64_t responses_success_delta =
        responses_success - last_logged_responses_success_;
    const uint64_t responses_failure_delta =
        responses_failure - last_logged_responses_failure_;
    const uint64_t append_requests_delta =
        append_requests - last_logged_append_requests_;
    const uint64_t reject_prev_delta = reject_prev - last_logged_reject_prev_;
    const uint64_t reject_term_delta = reject_term - last_logged_reject_term_;
    const uint64_t reject_gap_delta = reject_gap - last_logged_reject_gap_;
    const uint64_t appended_entries_delta =
        appended_entries - last_logged_appended_entries_;

    if (proposals == last_logged_proposals_ && sync_delta == 0 &&
        apply_delta == 0 && appends_delta == 0 && responses_delta == 0 &&
        append_requests_delta == 0 && pending_writes_.empty())
    {
        return;
    }

    size_t queue_depth = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_depth = queue_.size();
    }

    std::string peers;
    for (const auto& [peer_id, client] : peer_clients_)
    {
        const auto next_it = next_index_.find(peer_id);
        const auto match_it = match_index_.find(peer_id);
        const uint64_t next =
            next_it == next_index_.end() ? 0 : next_it->second;
        const uint64_t match =
            match_it == match_index_.end() ? 0 : match_it->second;
        if (!peers.empty())
        {
            peers += " ";
        }
        peers += peer_id + " next=" + std::to_string(next) +
                 " match=" + std::to_string(match) +
                 " q=" + std::to_string(client->QueueDepth());
    }

    const double sync_avg_us =
        sync_delta == 0
            ? 0.0
            : static_cast<double>(sync_ns_delta) /
                  static_cast<double>(sync_delta) / 1000.0;
    const double apply_avg_us =
        apply_delta == 0
            ? 0.0
            : static_cast<double>(apply_ns_delta) /
                  static_cast<double>(apply_delta) / 1000.0;

    const char* role = "follower";
    if (role_ == NodeRole::kLeader)
    {
        role = "leader";
    }
    else if (role_ == NodeRole::kCandidate)
    {
        role = "candidate";
    }

    CAIRNKV_LOG_INFO("raft metrics: role="
                     << role << " proposals=" << proposals
                     << " pending=" << pending_writes_.size()
                     << " qdepth=" << queue_depth
                     << " appends=" << appends_delta
                     << " empty=" << sends_empty_delta
                     << " with_entries=" << sends_entries_delta
                     << " responses=" << responses_delta
                     << " ok=" << responses_success_delta
                     << " fail=" << responses_failure_delta
                     << " append_req=" << append_requests_delta
                     << " rej_prev=" << reject_prev_delta
                     << " rej_term=" << reject_term_delta
                     << " rej_gap=" << reject_gap_delta
                     << " appended=" << appended_entries_delta
                     << " sync=" << sync_delta
                     << " sync_avg_us=" << sync_avg_us
                     << " apply=" << apply_delta
                     << " apply_avg_us=" << apply_avg_us
                     << " last_index=" << log_.LastIndex()
                     << " commit=" << commit_index_
                     << " applied=" << last_applied_ << " peers=[" << peers
                     << "]");

    last_logged_proposals_ = proposals;
    last_logged_sync_calls_ = sync_calls;
    last_logged_sync_ns_ = sync_ns;
    last_logged_apply_calls_ = apply_calls;
    last_logged_apply_ns_ = apply_ns;
    last_logged_appends_sent_ = appends_sent;
    last_logged_sends_empty_ = sends_empty;
    last_logged_sends_with_entries_ = sends_with_entries;
    last_logged_responses_ = responses;
    last_logged_responses_success_ = responses_success;
    last_logged_responses_failure_ = responses_failure;
    last_logged_append_requests_ = append_requests;
    last_logged_reject_prev_ = reject_prev;
    last_logged_reject_term_ = reject_term;
    last_logged_reject_gap_ = reject_gap;
    last_logged_appended_entries_ = appended_entries;
}

void RaftNode::ResetElectionDeadline()
{
    const int min_ms = std::max(10, options_.election_timeout_min_ms);
    const int max_ms = std::max(min_ms, options_.election_timeout_max_ms);
    std::uniform_int_distribution<int> distribution(min_ms, max_ms);
    election_deadline_ =
        Clock::now() + std::chrono::milliseconds(distribution(rng_));
}

void RaftNode::ResetHeartbeatDeadline()
{
    const int interval_ms = std::max(5, options_.heartbeat_interval_ms);
    heartbeat_deadline_ = Clock::now() + std::chrono::milliseconds(interval_ms);
}

void RaftNode::StartElection()
{
    role_ = NodeRole::kCandidate;
    ++current_term_;
    voted_for_ = options_.node_id;
    leader_id_.clear();
    leader_ready_ = false;
    leader_noop_index_ = 0;
    term_snapshot_.store(current_term_);

    votes_received_.clear();
    votes_received_.insert(options_.node_id);
    ResetElectionDeadline();
    FailPendingWrites(NotLeaderStatusUnlocked());
    FailReadRequests(NotLeaderStatusUnlocked());

    if (!PersistHardStateUnlocked().ok())
    {
        role_ = NodeRole::kFollower;
        return;
    }

    CAIRNKV_LOG_INFO("node " << options_.node_id << " starts election for term "
                             << current_term_);

    if (Majority() == 1)
    {
        BecomeLeader();
        return;
    }

    RaftMessage request;
    request.type = RaftMessageType::kRequestVote;
    request.term = current_term_;
    request.from_id = options_.node_id;
    request.last_log_index = log_.LastIndex();
    request.last_log_term = log_.LastTerm();

    for (const auto& [peer_id, client] : peer_clients_)
    {
        request.to_id = peer_id;
        client->Send(request);
    }
}

void RaftNode::BecomeFollower(uint64_t term, const std::string& new_leader_id)
{
    if (term < current_term_)
    {
        return;
    }

    if (term > current_term_)
    {
        current_term_ = term;
        voted_for_.clear();
        term_snapshot_.store(current_term_);
        if (!PersistHardStateUnlocked().ok())
        {
            CAIRNKV_LOG_ERROR("failed to persist hard state on term change");
        }
    }

    role_ = NodeRole::kFollower;
    leader_id_ = new_leader_id;
    leader_ready_ = false;
    leader_noop_index_ = 0;
    votes_received_.clear();
    next_index_.clear();
    match_index_.clear();
    ResetElectionDeadline();
    FailPendingWrites(NotLeaderStatusUnlocked());
    FailReadRequests(NotLeaderStatusUnlocked());
}

void RaftNode::BecomeLeader()
{
    role_ = NodeRole::kLeader;
    leader_id_ = options_.node_id;
    leader_ready_ = false;
    leader_noop_index_ = 0;
    votes_received_.clear();

    next_index_.clear();
    match_index_.clear();
    for (const auto& [peer_id, client] : peer_clients_)
    {
        (void)client;
        next_index_[peer_id] = log_.LastIndex() + 1;
        match_index_[peer_id] = 0;
    }

    LogEntry noop;
    noop.index = log_.LastIndex() + 1;
    noop.term = current_term_;
    noop.type = EntryType::kNoop;

    Status status = log_.Append(noop);
    if (status.ok())
    {
        status = log_.Sync();
    }
    if (!status.ok())
    {
        CAIRNKV_LOG_ERROR("failed to append leader no-op: " << status.ToString());
        BecomeFollower(current_term_, "");
        return;
    }

    leader_noop_index_ = noop.index;
    leader_ready_ = false;

    ResetHeartbeatDeadline();
    CAIRNKV_LOG_INFO("node " << options_.node_id << " became leader for term "
                             << current_term_);

    BroadcastAppendEntries();

    if (Majority() == 1)
    {
        commit_index_ = log_.LastIndex();
        ApplyCommitted();
        MaybeBecomeReadReady();
        CompleteReadRounds();
    }
}

void RaftNode::SendAppendEntries(const std::string& peer_id)
{
    if (role_ != NodeRole::kLeader)
    {
        return;
    }

    auto next_it = next_index_.find(peer_id);
    auto client_it = peer_clients_.find(peer_id);
    if (next_it == next_index_.end() || client_it == peer_clients_.end())
    {
        return;
    }

    const uint64_t last_index = log_.LastIndex();
    uint64_t next = next_it->second;
    if (next == 0)
    {
        next = 1;
    }
    if (next > last_index + 1)
    {
        next = last_index + 1;
    }
    next_it->second = next;

    const uint64_t prev_index = next > 0 ? next - 1 : 0;
    uint64_t prev_term = 0;
    if (prev_index > 0)
    {
        StatusOr<LogEntry> prev = log_.Get(prev_index);
        if (!prev.ok())
        {
            return;
        }
        prev_term = prev->term;
    }

    RaftMessage message;
    message.type = RaftMessageType::kAppendEntries;
    message.term = current_term_;
    message.from_id = options_.node_id;
    message.to_id = peer_id;
    message.leader_id = options_.node_id;
    message.prev_log_index = prev_index;
    message.prev_log_term = prev_term;
    message.leader_commit = commit_index_;

    for (uint64_t index = next;
         index <= last_index && message.entries.size() < 64;
         ++index)
    {
        StatusOr<LogEntry> entry = log_.Get(index);
        if (!entry.ok())
        {
            break;
        }
        message.entries.push_back(*entry);
    }

    metrics_.appends_sent.fetch_add(1, std::memory_order_relaxed);
    if (message.entries.empty())
    {
        metrics_.sends_empty.fetch_add(1, std::memory_order_relaxed);
    }
    else
    {
        metrics_.sends_with_entries.fetch_add(1, std::memory_order_relaxed);
    }
    client_it->second->Send(message);
}

void RaftNode::BroadcastAppendEntries()
{
    if (role_ != NodeRole::kLeader)
    {
        return;
    }
    for (const auto& [peer_id, client] : peer_clients_)
    {
        (void)client;
        SendAppendEntries(peer_id);
    }
}

void RaftNode::AdvanceCommitIndex()
{
    if (role_ != NodeRole::kLeader)
    {
        return;
    }

    for (uint64_t index = log_.LastIndex(); index > commit_index_; --index)
    {
        StatusOr<LogEntry> entry = log_.Get(index);
        if (!entry.ok() || entry->term != current_term_)
        {
            continue;
        }

        int match_count = 1;   // the leader itself
        for (const auto& [peer_id, match] : match_index_)
        {
            (void)peer_id;
            if (match >= index)
            {
                ++match_count;
            }
        }

        if (static_cast<uint64_t>(match_count) >= Majority())
        {
            commit_index_ = index;
            ApplyCommitted();
            MaybeBecomeReadReady();
            CompleteReadRounds();
            break;
        }
    }
}

void RaftNode::ApplyCommitted()
{
    while (last_applied_ < commit_index_)
    {
        const uint64_t index = last_applied_ + 1;
        StatusOr<LogEntry> entry = log_.Get(index);
        if (!entry.ok())
        {
            CAIRNKV_LOG_ERROR("missing committed log entry at index " << index);
            return;
        }

        const Clock::time_point apply_start = Clock::now();
        Status status = state_.Apply(*entry);
        const Clock::time_point apply_end = Clock::now();
        metrics_.apply_calls.fetch_add(1, std::memory_order_relaxed);
        metrics_.apply_ns.fetch_add(DurationNs(apply_start, apply_end),
                                    std::memory_order_relaxed);
        if (!status.ok())
        {
            CAIRNKV_LOG_ERROR("state machine apply failed at index "
                              << index << ": " << status.ToString());
            return;
        }
        last_applied_ = index;

        auto pending = pending_writes_.find(index);
        if (pending != pending_writes_.end())
        {
            pending->second->set_value(StatusOr<uint64_t>(index));
            pending_writes_.erase(pending);
        }
    }

    CompleteReadRounds();
}

void RaftNode::FailPendingWrites(const Status& status)
{
    for (auto& [index, promise] : pending_writes_)
    {
        (void)index;
        promise->set_value(StatusOr<uint64_t>(status));
    }
    pending_writes_.clear();
}

void RaftNode::StartReadIndex(
    const std::string& key,
    const std::shared_ptr<std::promise<StatusOr<GetResult>>>& promise)
{
    if (role_ != NodeRole::kLeader)
    {
        promise->set_value(StatusOr<GetResult>(NotLeaderStatusUnlocked()));
        return;
    }
    if (!leader_ready_)
    {
        pending_reads_before_ready_.push_back(PendingRead{key, promise});
        return;
    }

    const uint64_t context = next_read_context_++;
    ReadRound round;
    round.read_index = commit_index_;
    round.key = key;
    round.promise = promise;
    round.acks.insert(options_.node_id);

    auto inserted = read_rounds_.emplace(context, std::move(round));
    ReadRound& stored = inserted.first->second;

    if (stored.acks.size() >= Majority())
    {
        stored.majority_acked = true;
    }
    else
    {
        for (const auto& [peer_id, client] : peer_clients_)
        {
            (void)client;
            SendReadIndexHeartbeat(peer_id, context);
        }
    }
    CompleteReadRounds();
}

void RaftNode::SendReadIndexHeartbeat(const std::string& peer_id,
                                      uint64_t context)
{
    if (role_ != NodeRole::kLeader)
    {
        return;
    }
    auto client_it = peer_clients_.find(peer_id);
    if (client_it == peer_clients_.end())
    {
        return;
    }

    RaftMessage message;
    message.type = RaftMessageType::kAppendEntries;
    message.term = current_term_;
    message.from_id = options_.node_id;
    message.to_id = peer_id;
    message.leader_id = options_.node_id;
    message.prev_log_index = log_.LastIndex();
    message.prev_log_term = log_.LastTerm();
    message.leader_commit = commit_index_;
    message.read_context = context;
    client_it->second->Send(message);
}

void RaftNode::MaybeBecomeReadReady()
{
    if (role_ != NodeRole::kLeader || leader_ready_ ||
        leader_noop_index_ == 0 || commit_index_ < leader_noop_index_)
    {
        return;
    }

    StatusOr<LogEntry> entry = log_.Get(leader_noop_index_);
    if (!entry.ok() || entry->term != current_term_)
    {
        return;
    }

    leader_ready_ = true;
    FlushPendingReads();
}

void RaftNode::FlushPendingReads()
{
    std::deque<PendingRead> pending;
    pending.swap(pending_reads_before_ready_);
    for (auto& read : pending)
    {
        StartReadIndex(read.key, read.promise);
    }
}

void RaftNode::CompleteReadRounds()
{
    for (auto it = read_rounds_.begin(); it != read_rounds_.end();)
    {
        if (!it->second.majority_acked ||
            it->second.read_index > last_applied_)
        {
            ++it;
            continue;
        }

        StatusOr<std::string> value = state_.Get(it->second.key);
        if (!value.ok())
        {
            it->second.promise->set_value(StatusOr<GetResult>(value.status()));
        }
        else
        {
            it->second.promise->set_value(
                StatusOr<GetResult>(GetResult{*value, last_applied_}));
        }
        it = read_rounds_.erase(it);
    }
}

void RaftNode::FailReadRequests(const Status& status)
{
    for (auto& [context, round] : read_rounds_)
    {
        (void)context;
        round.promise->set_value(StatusOr<GetResult>(status));
    }
    read_rounds_.clear();

    for (auto& read : pending_reads_before_ready_)
    {
        read.promise->set_value(StatusOr<GetResult>(status));
    }
    pending_reads_before_ready_.clear();
}

void RaftNode::SetBlockedPeers(const std::set<std::string>& peers)
{
    std::lock_guard<std::mutex> lock(blocked_mutex_);
    blocked_peers_ = peers;
    for (const auto& [peer_id, client] : peer_clients_)
    {
        client->SetBlocked(peers.count(peer_id) != 0);
    }
}

bool RaftNode::IsPeerBlocked(const std::string& peer_id) const
{
    std::lock_guard<std::mutex> lock(blocked_mutex_);
    return blocked_peers_.count(peer_id) != 0;
}

std::string RaftNode::LeaderAddressUnlocked() const
{
    if (leader_id_.empty())
    {
        return "";
    }
    if (leader_id_ == options_.node_id)
    {
        return options_.listen_addr;
    }
    const auto it = options_.peers.find(leader_id_);
    if (it == options_.peers.end())
    {
        return "";
    }
    return it->second;
}

Status RaftNode::NotLeaderStatusUnlocked() const
{
    return Status(StatusCode::kNotLeader, LeaderAddressUnlocked());
}

uint64_t RaftNode::Majority() const
{
    const uint64_t node_count = static_cast<uint64_t>(options_.peers.size()) + 1;
    return node_count / 2 + 1;
}

Status RaftNode::PersistHardStateUnlocked()
{
    const Status status =
        SaveHardState(hard_state_path_, HardState{current_term_, voted_for_});
    if (!status.ok())
    {
        CAIRNKV_LOG_ERROR("failed to persist hard state: " << status.ToString());
    }
    return status;
}

bool RaftNode::IsLogUpToDate(uint64_t last_log_term,
                             uint64_t last_log_index) const
{
    const uint64_t local_term = log_.LastTerm();
    const uint64_t local_index = log_.LastIndex();
    if (last_log_term != local_term)
    {
        return last_log_term > local_term;
    }
    return last_log_index >= local_index;
}

RaftMessage RaftNode::HandleRequestVote(const RaftMessage& request)
{
    RaftMessage response;
    response.type = RaftMessageType::kRequestVoteResp;
    response.term = current_term_;
    response.from_id = options_.node_id;
    response.to_id = request.from_id;
    response.vote_granted = false;

    if (request.term < current_term_)
    {
        return response;
    }
    if (request.term > current_term_)
    {
        BecomeFollower(request.term, "");
    }

    if (voted_for_.empty() || voted_for_ == request.from_id)
    {
        if (IsLogUpToDate(request.last_log_term, request.last_log_index))
        {
            voted_for_ = request.from_id;
            term_snapshot_.store(current_term_);
            const Status status = PersistHardStateUnlocked();
            if (status.ok())
            {
                response.vote_granted = true;
                ResetElectionDeadline();
            }
            else
            {
                voted_for_.clear();
            }
        }
    }

    response.term = current_term_;
    return response;
}

RaftMessage RaftNode::HandleAppendEntries(const RaftMessage& request)
{
    RaftMessage response;
    response.type = RaftMessageType::kAppendEntriesResp;
    response.term = current_term_;
    response.from_id = options_.node_id;
    response.to_id = request.from_id;
    response.success = false;
    response.match_index = log_.LastIndex();
    response.read_context = request.read_context;
    metrics_.append_requests.fetch_add(1, std::memory_order_relaxed);

    if (request.term < current_term_)
    {
        metrics_.reject_term.fetch_add(1, std::memory_order_relaxed);
        return response;
    }

    if (request.term > current_term_)
    {
        BecomeFollower(request.term, request.leader_id);
    }
    else
    {
        leader_id_ = request.leader_id;
        if (role_ != NodeRole::kFollower)
        {
            role_ = NodeRole::kFollower;
            votes_received_.clear();
            next_index_.clear();
            match_index_.clear();
            FailPendingWrites(NotLeaderStatusUnlocked());
        }
        ResetElectionDeadline();
    }
    response.term = current_term_;

    if (request.prev_log_index > log_.LastIndex())
    {
        metrics_.reject_prev.fetch_add(1, std::memory_order_relaxed);
        response.match_index = log_.LastIndex();
        return response;
    }

    if (request.prev_log_index > 0)
    {
        StatusOr<LogEntry> prev = log_.Get(request.prev_log_index);
        if (!prev.ok() || prev->term != request.prev_log_term)
        {
            metrics_.reject_term.fetch_add(1, std::memory_order_relaxed);
            response.match_index = request.prev_log_index - 1;
            return response;
        }
    }

    bool appended = false;
    for (const auto& entry : request.entries)
    {
        if (entry.index <= log_.LastIndex())
        {
            StatusOr<LogEntry> existing = log_.Get(entry.index);
            if (!existing.ok())
            {
                metrics_.reject_gap.fetch_add(1, std::memory_order_relaxed);
                return response;
            }
            if (existing->term == entry.term)
            {
                continue;
            }

            Status status = log_.TruncateSuffix(entry.index);
            if (!status.ok())
            {
                metrics_.reject_gap.fetch_add(1, std::memory_order_relaxed);
                return response;
            }
        }

        if (entry.index != log_.LastIndex() + 1)
        {
            metrics_.reject_gap.fetch_add(1, std::memory_order_relaxed);
            return response;
        }

        Status status = log_.Append(entry);
        if (!status.ok())
        {
            metrics_.reject_gap.fetch_add(1, std::memory_order_relaxed);
            return response;
        }
        metrics_.appended_entries.fetch_add(1, std::memory_order_relaxed);
        appended = true;
    }

    if (appended)
    {
        const Clock::time_point sync_start = Clock::now();
        Status status = log_.Sync();
        const Clock::time_point sync_end = Clock::now();
        metrics_.sync_calls.fetch_add(1, std::memory_order_relaxed);
        metrics_.sync_ns.fetch_add(DurationNs(sync_start, sync_end),
                                   std::memory_order_relaxed);
        if (!status.ok())
        {
            return response;
        }
    }

    if (request.leader_commit > commit_index_)
    {
        commit_index_ = std::min(request.leader_commit, log_.LastIndex());
        ApplyCommitted();
    }

    response.success = true;
    response.match_index = log_.LastIndex();
    response.term = current_term_;
    return response;
}

void RaftNode::HandleRequestVoteResponse(const RaftMessage& response)
{
    if (response.term > current_term_)
    {
        BecomeFollower(response.term, "");
        return;
    }
    if (role_ != NodeRole::kCandidate || response.term != current_term_)
    {
        return;
    }
    if (response.vote_granted)
    {
        votes_received_.insert(response.from_id);
        if (votes_received_.size() >= Majority())
        {
            BecomeLeader();
        }
    }
}

void RaftNode::HandleAppendEntriesResponse(const RaftMessage& response)
{
    if (response.term > current_term_)
    {
        BecomeFollower(response.term, "");
        return;
    }
    if (role_ != NodeRole::kLeader || response.term != current_term_)
    {
        return;
    }

    metrics_.responses.fetch_add(1, std::memory_order_relaxed);

    if (response.read_context != 0)
    {
        auto read_it = read_rounds_.find(response.read_context);
        if (read_it != read_rounds_.end())
        {
            read_it->second.acks.insert(response.from_id);
            if (read_it->second.acks.size() >= Majority())
            {
                read_it->second.majority_acked = true;
            }
            CompleteReadRounds();
        }
    }

    auto next_it = next_index_.find(response.from_id);
    auto match_it = match_index_.find(response.from_id);
    if (next_it == next_index_.end() || match_it == match_index_.end())
    {
        return;
    }

    if (response.success)
    {
        metrics_.responses_success.fetch_add(1, std::memory_order_relaxed);
        const uint64_t match = std::min(response.match_index, log_.LastIndex());
        if (match > match_it->second)
        {
            match_it->second = match;
        }
        next_it->second = match_it->second + 1;
        AdvanceCommitIndex();

        // Only continue when this peer still has entries to catch up on.
        // Replying to an empty heartbeat must not start another
        // AppendEntries, otherwise leader and follower ping-pong forever
        // and starve client proposals behind the peer send queue.
        if (next_it->second <= log_.LastIndex())
        {
            SendAppendEntries(response.from_id);
        }
    }
    else
    {
        metrics_.responses_failure.fetch_add(1, std::memory_order_relaxed);
        uint64_t next = response.match_index + 1;
        if (next < 1)
        {
            next = 1;
        }
        if (next > log_.LastIndex() + 1)
        {
            next = log_.LastIndex() + 1;
        }
        next_it->second = next;
        SendAppendEntries(response.from_id);
    }
}

RaftMessage RaftNode::DispatchRaftMessage(const RaftMessage& request)
{
    switch (request.type)
    {
        case RaftMessageType::kRequestVote:
            return HandleRequestVote(request);
        case RaftMessageType::kAppendEntries:
            return HandleAppendEntries(request);
        case RaftMessageType::kRequestVoteResp:
            HandleRequestVoteResponse(request);
            break;
        case RaftMessageType::kAppendEntriesResp:
            HandleAppendEntriesResponse(request);
            break;
    }

    RaftMessage response;
    response.from_id = options_.node_id;
    response.to_id = request.from_id;
    response.term = current_term_;
    return response;
}

void RaftNode::OnPeerMessage(const RaftMessage& message)
{
    if (!running_.load())
    {
        return;
    }
    Enqueue([this, message] { (void)DispatchRaftMessage(message); });
}

RaftMessage RaftNode::HandleRaftRequest(const RaftMessage& request)
{
    if (!running_.load())
    {
        RaftMessage response;
        response.type = request.type == RaftMessageType::kRequestVote
                            ? RaftMessageType::kRequestVoteResp
                            : RaftMessageType::kAppendEntriesResp;
        response.term = term_snapshot_.load();
        response.from_id = options_.node_id;
        response.to_id = request.from_id;
        return response;
    }

    auto promise = std::make_shared<std::promise<RaftMessage>>();
    auto future = promise->get_future();
    Enqueue([this, request, promise] {
        promise->set_value(DispatchRaftMessage(request));
    });

    if (future.wait_for(kRaftRequestTimeout) != std::future_status::ready)
    {
        RaftMessage response;
        response.type = request.type == RaftMessageType::kRequestVote
                            ? RaftMessageType::kRequestVoteResp
                            : RaftMessageType::kAppendEntriesResp;
        response.term = term_snapshot_.load();
        response.from_id = options_.node_id;
        response.to_id = request.from_id;
        return response;
    }
    return future.get();
}

StatusOr<uint64_t> RaftNode::ProposeEntry(EntryType type,
                                          const std::string& key,
                                          const std::string& value)
{
    if (!running_.load())
    {
        return Status(StatusCode::kUnavailable, "raft node stopped");
    }

    auto promise = std::make_shared<std::promise<StatusOr<uint64_t>>>();
    auto future = promise->get_future();
    Enqueue([this, type, key, value, promise] {
        if (role_ != NodeRole::kLeader)
        {
            promise->set_value(StatusOr<uint64_t>(NotLeaderStatusUnlocked()));
            return;
        }
        if (key.empty())
        {
            promise->set_value(StatusOr<uint64_t>(
                Status(StatusCode::kInvalidArgument, "empty key")));
            return;
        }
        if (key.size() > 64u * 1024u)
        {
            promise->set_value(StatusOr<uint64_t>(
                Status(StatusCode::kInvalidArgument, "key too large")));
            return;
        }
        if (value.size() > 64u * 1024u)
        {
            promise->set_value(StatusOr<uint64_t>(
                Status(StatusCode::kInvalidArgument, "value too large")));
            return;
        }
        if (key.rfind("__meta/", 0) == 0)
        {
            promise->set_value(StatusOr<uint64_t>(
                Status(StatusCode::kInvalidArgument, "reserved key prefix")));
            return;
        }

        const uint64_t index = log_.LastIndex() + 1;
        const LogEntry entry{index, current_term_, type, key, value};

        const Clock::time_point append_start = Clock::now();
        Status status = log_.Append(entry);
        const Clock::time_point append_end = Clock::now();
        metrics_.append_ns.fetch_add(DurationNs(append_start, append_end),
                                     std::memory_order_relaxed);
        if (!status.ok())
        {
            promise->set_value(StatusOr<uint64_t>(status));
            return;
        }

        const Clock::time_point sync_start = Clock::now();
        status = log_.Sync();
        const Clock::time_point sync_end = Clock::now();
        metrics_.sync_calls.fetch_add(1, std::memory_order_relaxed);
        metrics_.sync_ns.fetch_add(DurationNs(sync_start, sync_end),
                                   std::memory_order_relaxed);
        metrics_.proposals.fetch_add(1, std::memory_order_relaxed);
        if (!status.ok())
        {
            promise->set_value(StatusOr<uint64_t>(status));
            return;
        }

        pending_writes_[index] = promise;
        BroadcastAppendEntries();

        if (Majority() == 1)
        {
            commit_index_ = index;
            ApplyCommitted();
        }
    });

    const int timeout_ms =
        options_.client_timeout_ms > 0 ? options_.client_timeout_ms : 5000;
    if (future.wait_for(std::chrono::milliseconds(timeout_ms)) !=
        std::future_status::ready)
    {
        return Status(StatusCode::kTimeout, "raft proposal timeout");
    }
    return future.get();
}

StatusOr<uint64_t> RaftNode::Put(const std::string& key,
                                 const std::string& value)
{
    return ProposeEntry(EntryType::kPut, key, value);
}

Status RaftNode::Delete(const std::string& key)
{
    StatusOr<uint64_t> result = ProposeEntry(EntryType::kDelete, key, "");
    if (!result.ok())
    {
        return result.status();
    }
    return Status::Ok();
}

StatusOr<GetResult> RaftNode::Get(const std::string& key, ReadOptions opts)
{
    if (!running_.load())
    {
        return Status(StatusCode::kUnavailable, "raft node stopped");
    }

    auto promise = std::make_shared<std::promise<StatusOr<GetResult>>>();
    auto future = promise->get_future();
    Enqueue([this, key, opts, promise] {
        if (opts.mode == ReadMode::kStale)
        {
            StatusOr<std::string> value = state_.Get(key);
            if (!value.ok())
            {
                promise->set_value(StatusOr<GetResult>(value.status()));
                return;
            }
            promise->set_value(
                StatusOr<GetResult>(GetResult{*value, last_applied_}));
            return;
        }

        if (role_ != NodeRole::kLeader)
        {
            promise->set_value(StatusOr<GetResult>(NotLeaderStatusUnlocked()));
            return;
        }
        StartReadIndex(key, promise);
    });

    const int timeout_ms =
        options_.client_timeout_ms > 0 ? options_.client_timeout_ms : 5000;
    if (future.wait_for(std::chrono::milliseconds(timeout_ms)) !=
        std::future_status::ready)
    {
        return Status(StatusCode::kTimeout, "raft read timeout");
    }
    return future.get();
}

StatusOr<std::vector<KeyValue>> RaftNode::Scan(const std::string& prefix,
                                                uint32_t limit)
{
    if (!running_.load())
    {
        return Status(StatusCode::kUnavailable, "raft node stopped");
    }

    auto promise =
        std::make_shared<std::promise<StatusOr<std::vector<KeyValue>>>>();
    auto future = promise->get_future();
    Enqueue([this, prefix, limit, promise] {
        if (role_ != NodeRole::kLeader)
        {
            promise->set_value(
                StatusOr<std::vector<KeyValue>>(NotLeaderStatusUnlocked()));
            return;
        }
        StatusOr<std::vector<KeyValue>> result = state_.Scan(prefix, limit);
        promise->set_value(std::move(result));
    });

    const int timeout_ms =
        options_.client_timeout_ms > 0 ? options_.client_timeout_ms : 5000;
    if (future.wait_for(std::chrono::milliseconds(timeout_ms)) !=
        std::future_status::ready)
    {
        return Status(StatusCode::kTimeout, "raft scan timeout");
    }
    return future.get();
}

StatusOr<ClusterStatus> RaftNode::GetStatus()
{
    if (!running_.load())
    {
        return Status(StatusCode::kUnavailable, "raft node stopped");
    }

    auto promise = std::make_shared<std::promise<StatusOr<ClusterStatus>>>();
    auto future = promise->get_future();
    Enqueue([this, promise] {
        ClusterStatus status;
        status.role = role_;
        status.term = current_term_;
        status.leader_id = leader_id_;
        status.commit_index = commit_index_;
        status.last_applied = last_applied_;
        status.log_last_index = log_.LastIndex();
        status.wal_bytes = log_.FileSize();
        promise->set_value(StatusOr<ClusterStatus>(status));
    });

    const int timeout_ms =
        options_.client_timeout_ms > 0 ? options_.client_timeout_ms : 5000;
    if (future.wait_for(std::chrono::milliseconds(timeout_ms)) !=
        std::future_status::ready)
    {
        return Status(StatusCode::kTimeout, "raft status timeout");
    }
    return future.get();
}

}
