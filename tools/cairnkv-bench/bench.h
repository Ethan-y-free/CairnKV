#pragma once

#include <cstdint>
#include <string>

#include "cairnkv/types.h"

namespace cairnkv
{

// Closed-loop benchmark options. Each client thread owns one blocking Client,
// so at most one request per thread is in flight at any time.
struct BenchOptions
{
    std::string host = "127.0.0.1";
    uint16_t port = 6666;
    std::string op = "put";   // put | get | stale-get
    int threads = 8;
    int duration_s = 10;
    int warmup_s = 2;
    int timeout_ms = 5000;
    uint64_t keyspace = 1000;
    size_t value_size = 64;
    std::string label;
};

struct ServerSnapshot
{
    bool ok = false;
    NodeRole role = NodeRole::kFollower;
    uint64_t term = 0;
    std::string leader_id;
    uint64_t commit_index = 0;
    uint64_t last_applied = 0;
    uint64_t log_last_index = 0;
    uint64_t wal_bytes = 0;
    std::string error;
};

struct BenchResult
{
    BenchOptions options;

    uint64_t success = 0;
    uint64_t not_found = 0;
    uint64_t timeout = 0;
    uint64_t not_leader = 0;
    uint64_t other_error = 0;
    uint64_t open_failures = 0;

    double elapsed_s = 0.0;
    double throughput_ops = 0.0;
    double p50_us = 0.0;
    double p90_us = 0.0;
    double p99_us = 0.0;
    double p999_us = 0.0;
    double max_us = 0.0;
    double prefill_s = 0.0;

    ServerSnapshot before;
    ServerSnapshot after;
};

// Runs one benchmark. Returns false on setup failure (prefill or every client
// failing to open); request-level errors are counted in the result instead.
bool RunBenchmark(const BenchOptions& options,
                  BenchResult* result,
                  std::string* error);

}
