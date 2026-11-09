#include "bench.h"

#include <algorithm>
#include <barrier>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cairnkv/client.h"

namespace cairnkv
{

namespace
{

using Clock = std::chrono::steady_clock;

struct ThreadStats
{
    bool opened = false;
    std::string open_error;
    uint64_t success = 0;
    uint64_t not_found = 0;
    uint64_t timeout = 0;
    uint64_t not_leader = 0;
    uint64_t other_error = 0;
    std::vector<uint64_t> latency_ns;
    Clock::time_point measure_start;
    Clock::time_point measure_end;
};

StatusOr<std::unique_ptr<Client>> OpenClient(const BenchOptions& options)
{
    ClientOptions client_options;
    client_options.host = options.host;
    client_options.port = options.port;
    client_options.timeout_ms = options.timeout_ms;
    return Client::Open(client_options);
}

double SecondsBetween(Clock::time_point start, Clock::time_point end)
{
    return std::chrono::duration<double>(end - start).count();
}

std::string MakeKey(uint64_t index)
{
    return "bench:" + std::to_string(index);
}

std::string MakeValue(size_t size)
{
    std::string value(size, 'x');
    for (size_t i = 0; i < value.size(); ++i)
    {
        value[i] = static_cast<char>('a' + (i % 26));
    }
    return value;
}

Status RunOne(Client* client,
              const BenchOptions& options,
              const std::string& key,
              const std::string& value)
{
    if (options.op == "put")
    {
        return client->Put(key, value);
    }

    ReadOptions read_options;
    read_options.mode = options.op == "stale-get" ? ReadMode::kStale
                                                   : ReadMode::kLinearizable;
    StatusOr<GetResult> result = client->Get(key, read_options);
    if (!result.ok())
    {
        return result.status();
    }
    return Status::Ok();
}

void Classify(const Status& status, ThreadStats* stats)
{
    if (status.ok())
    {
        ++stats->success;
        return;
    }

    switch (status.code())
    {
    case StatusCode::kNotFound:
        ++stats->not_found;
        break;
    case StatusCode::kTimeout:
        ++stats->timeout;
        break;
    case StatusCode::kNotLeader:
        ++stats->not_leader;
        break;
    default:
        ++stats->other_error;
        break;
    }
}

void RunClientThread(const BenchOptions& options,
                     int thread_index,
                     const std::string& value,
                     Clock::time_point warmup_end,
                     std::barrier<>& warmup_done,
                     std::barrier<>& measure_go,
                     ThreadStats* stats)
{
    StatusOr<std::unique_ptr<Client>> client = OpenClient(options);
    if (!client.ok())
    {
        stats->open_error = client.status().ToString();
        // Keep the phase barriers balanced even when this client failed.
        warmup_done.arrive_and_wait();
        measure_go.arrive_and_wait();
        return;
    }
    stats->opened = true;
    Client* raw_client = client->get();

    const uint64_t stride = static_cast<uint64_t>(options.threads);
    uint64_t seq = static_cast<uint64_t>(thread_index);

    while (Clock::now() < warmup_end)
    {
        const std::string key = MakeKey(seq % options.keyspace);
        seq += stride;
        (void)RunOne(raw_client, options, key, value);
    }

    // The main thread takes the server snapshot between these two barriers,
    // so commit/apply deltas cover the measurement window only.
    warmup_done.arrive_and_wait();
    measure_go.arrive_and_wait();

    stats->latency_ns.reserve(1u << 16);
    stats->measure_start = Clock::now();
    const Clock::time_point deadline =
        stats->measure_start + std::chrono::seconds(options.duration_s);

    while (Clock::now() < deadline)
    {
        const std::string key = MakeKey(seq % options.keyspace);
        seq += stride;

        const Clock::time_point start = Clock::now();
        const Status status = RunOne(raw_client, options, key, value);
        const Clock::time_point end = Clock::now();

        Classify(status, stats);
        if (status.ok())
        {
            stats->latency_ns.push_back(static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(end - start)
                    .count()));
        }
    }
    stats->measure_end = Clock::now();
}

bool Prefill(const BenchOptions& options,
             const std::string& value,
             double* elapsed_s,
             std::string* error)
{
    const Clock::time_point start = Clock::now();
    StatusOr<std::unique_ptr<Client>> client = OpenClient(options);
    if (!client.ok())
    {
        *error = client.status().ToString();
        return false;
    }

    uint64_t failures = 0;
    for (uint64_t i = 0; i < options.keyspace; ++i)
    {
        const Status status = (*client)->Put(MakeKey(i), value);
        if (!status.ok())
        {
            ++failures;
        }
    }
    *elapsed_s = SecondsBetween(start, Clock::now());

    if (failures > 0)
    {
        *error = "prefill failed for " + std::to_string(failures) + " of " +
                 std::to_string(options.keyspace) + " keys";
        return false;
    }
    return true;
}

ServerSnapshot TakeSnapshot(const BenchOptions& options)
{
    ServerSnapshot snapshot;
    StatusOr<std::unique_ptr<Client>> client = OpenClient(options);
    if (!client.ok())
    {
        snapshot.error = client.status().ToString();
        return snapshot;
    }

    StatusOr<ClusterStatus> status = (*client)->GetStatus();
    if (!status.ok())
    {
        snapshot.error = status.status().ToString();
        return snapshot;
    }

    snapshot.ok = true;
    snapshot.role = status->role;
    snapshot.term = status->term;
    snapshot.leader_id = status->leader_id;
    snapshot.commit_index = status->commit_index;
    snapshot.last_applied = status->last_applied;
    snapshot.log_last_index = status->log_last_index;
    snapshot.wal_bytes = status->wal_bytes;
    return snapshot;
}

double PercentileUs(const std::vector<uint64_t>& sorted, double percentile)
{
    if (sorted.empty())
    {
        return 0.0;
    }
    const double rank =
        std::ceil(percentile / 100.0 * static_cast<double>(sorted.size()));
    size_t index = rank < 1.0 ? 0 : static_cast<size_t>(rank) - 1;
    if (index >= sorted.size())
    {
        index = sorted.size() - 1;
    }
    return static_cast<double>(sorted[index]) / 1000.0;
}

}

bool RunBenchmark(const BenchOptions& options,
                  BenchResult* result,
                  std::string* error)
{
    *result = BenchResult{};
    result->options = options;

    const std::string value = MakeValue(options.value_size);

    if (options.op != "put")
    {
        double prefill_s = 0.0;
        if (!Prefill(options, value, &prefill_s, error))
        {
            result->prefill_s = prefill_s;
            return false;
        }
        result->prefill_s = prefill_s;
    }

    const Clock::time_point warmup_end =
        Clock::now() + std::chrono::seconds(options.warmup_s);

    const std::ptrdiff_t barrier_count =
        static_cast<std::ptrdiff_t>(options.threads) + 1;
    std::barrier<> warmup_done(barrier_count);
    std::barrier<> measure_go(barrier_count);

    std::vector<ThreadStats> stats(static_cast<size_t>(options.threads));
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(options.threads));
    for (int i = 0; i < options.threads; ++i)
    {
        threads.emplace_back(&RunClientThread,
                             std::cref(options),
                             i,
                             std::cref(value),
                             warmup_end,
                             std::ref(warmup_done),
                             std::ref(measure_go),
                             &stats[static_cast<size_t>(i)]);
    }

    warmup_done.arrive_and_wait();
    result->before = TakeSnapshot(options);
    measure_go.arrive_and_wait();

    for (std::thread& thread : threads)
    {
        thread.join();
    }

    result->after = TakeSnapshot(options);

    std::vector<uint64_t> samples;
    Clock::time_point earliest = Clock::time_point::max();
    Clock::time_point latest = Clock::time_point::min();
    bool any_open = false;

    for (ThreadStats& thread_stats : stats)
    {
        result->success += thread_stats.success;
        result->not_found += thread_stats.not_found;
        result->timeout += thread_stats.timeout;
        result->not_leader += thread_stats.not_leader;
        result->other_error += thread_stats.other_error;

        if (!thread_stats.opened)
        {
            ++result->open_failures;
            continue;
        }

        any_open = true;
        samples.insert(samples.end(),
                       thread_stats.latency_ns.begin(),
                       thread_stats.latency_ns.end());
        earliest = std::min(earliest, thread_stats.measure_start);
        latest = std::max(latest, thread_stats.measure_end);
    }

    if (!any_open)
    {
        *error = "all client connections failed to open";
        return false;
    }

    result->elapsed_s = SecondsBetween(earliest, latest);
    if (result->elapsed_s > 0.0)
    {
        result->throughput_ops =
            static_cast<double>(result->success) / result->elapsed_s;
    }

    std::sort(samples.begin(), samples.end());
    result->p50_us = PercentileUs(samples, 50.0);
    result->p90_us = PercentileUs(samples, 90.0);
    result->p99_us = PercentileUs(samples, 99.0);
    result->p999_us = PercentileUs(samples, 99.9);
    result->max_us = samples.empty()
                         ? 0.0
                         : static_cast<double>(samples.back()) / 1000.0;
    return true;
}

}
