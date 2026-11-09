#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>

#include "bench.h"

namespace
{

struct CommandLine
{
    cairnkv::BenchOptions options;
    bool json = false;
};

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

bool ParseInt(const std::string& text, long long* out)
{
    char* end = nullptr;
    errno = 0;
    const long long value = std::strtoll(text.c_str(), &end, 10);
    if (errno != 0 || end == nullptr || *end != '\0')
    {
        return false;
    }
    *out = value;
    return true;
}

bool InRange(long long value, long long min_value, long long max_value)
{
    return value >= min_value && value <= max_value;
}

bool TakeValue(int argc, char** argv, int* index, std::string* value)
{
    if (*index + 1 >= argc)
    {
        return false;
    }
    *value = argv[*index + 1];
    *index += 2;
    return true;
}

void PrintUsage()
{
    std::cout
        << "usage: cairnkv-bench [--addr host:port] [options]\n"
        << "options:\n"
        << "  --op put|get|stale-get   workload (default: put)\n"
        << "  --threads N              client threads (default: 8)\n"
        << "  --duration S             measurement seconds (default: 10)\n"
        << "  --warmup S               warmup seconds (default: 2)\n"
        << "  --timeout-ms N           client RPC timeout (default: 5000)\n"
        << "  --keyspace N             distinct keys (default: 1000)\n"
        << "  --value-size N           value bytes (default: 64)\n"
        << "  --label TEXT             result label\n"
        << "  --json                   print one JSON object\n";
}

const char* RoleName(cairnkv::NodeRole role)
{
    switch (role)
    {
    case cairnkv::NodeRole::kLeader:
        return "leader";
    case cairnkv::NodeRole::kCandidate:
        return "candidate";
    case cairnkv::NodeRole::kFollower:
    default:
        return "follower";
    }
}

std::string JsonEscape(const std::string& text)
{
    std::string escaped;
    escaped.reserve(text.size());
    for (char ch : text)
    {
        switch (ch)
        {
        case '"':
            escaped += "\\\"";
            break;
        case '\\':
            escaped += "\\\\";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            escaped += ch;
            break;
        }
    }
    return escaped;
}

std::string LabelOf(const cairnkv::BenchOptions& options)
{
    return options.label.empty() ? options.op : options.label;
}

void PrintSnapshot(const char* name, const cairnkv::ServerSnapshot& snapshot)
{
    if (!snapshot.ok)
    {
        std::cout << name << " ok=false error=" << snapshot.error << "\n";
        return;
    }
    std::cout << name << " ok=true"
              << " role=" << RoleName(snapshot.role)
              << " term=" << snapshot.term
              << " leader_id="
              << (snapshot.leader_id.empty() ? "-" : snapshot.leader_id)
              << " commit_index=" << snapshot.commit_index
              << " last_applied=" << snapshot.last_applied
              << " log_last_index=" << snapshot.log_last_index
              << " wal_bytes=" << snapshot.wal_bytes << "\n";
}

void PrintJsonSnapshot(const char* name,
                       const cairnkv::ServerSnapshot& snapshot,
                       bool trailing_comma)
{
    std::cout << "  \"" << name << "\": {";
    if (!snapshot.ok)
    {
        std::cout << "\"ok\": false, \"error\": \""
                  << JsonEscape(snapshot.error) << "\"}";
    }
    else
    {
        std::cout << "\"ok\": true"
                  << ", \"role\": \"" << RoleName(snapshot.role) << "\""
                  << ", \"term\": " << snapshot.term
                  << ", \"leader_id\": \"" << JsonEscape(snapshot.leader_id)
                  << "\""
                  << ", \"commit_index\": " << snapshot.commit_index
                  << ", \"last_applied\": " << snapshot.last_applied
                  << ", \"log_last_index\": " << snapshot.log_last_index
                  << ", \"wal_bytes\": " << snapshot.wal_bytes << "}";
    }
    std::cout << (trailing_comma ? "," : "") << "\n";
}

void PrintHuman(const cairnkv::BenchResult& result)
{
    const cairnkv::BenchOptions& options = result.options;
    std::cout << "label=" << LabelOf(options) << "\n"
              << "op=" << options.op << " threads=" << options.threads
              << " duration_s=" << options.duration_s
              << " warmup_s=" << options.warmup_s
              << " value_size=" << options.value_size
              << " keyspace=" << options.keyspace << "\n"
              << "success=" << result.success
              << " timeout=" << result.timeout
              << " not_leader=" << result.not_leader
              << " not_found=" << result.not_found
              << " other_error=" << result.other_error
              << " open_failures=" << result.open_failures << "\n";

    std::cout << std::fixed << std::setprecision(2)
              << "elapsed_s=" << result.elapsed_s
              << " throughput_ops=" << result.throughput_ops << "\n"
              << "latency_us p50=" << result.p50_us
              << " p90=" << result.p90_us
              << " p99=" << result.p99_us
              << " p999=" << result.p999_us
              << " max=" << result.max_us << "\n";

    if (options.op != "put")
    {
        std::cout << "prefill_s=" << result.prefill_s << "\n";
    }

    PrintSnapshot("server_before", result.before);
    PrintSnapshot("server_after", result.after);

    if (result.before.ok && result.after.ok)
    {
        const long long commit_delta =
            static_cast<long long>(result.after.commit_index) -
            static_cast<long long>(result.before.commit_index);
        const long long apply_delta =
            static_cast<long long>(result.after.last_applied) -
            static_cast<long long>(result.before.last_applied);
        const long long wal_delta =
            static_cast<long long>(result.after.wal_bytes) -
            static_cast<long long>(result.before.wal_bytes);
        std::cout << "server_delta commit=" << commit_delta
                  << " apply=" << apply_delta
                  << " wal_bytes=" << wal_delta << "\n";
    }
}

void PrintJson(const cairnkv::BenchResult& result)
{
    const cairnkv::BenchOptions& options = result.options;
    std::cout << "{\n"
              << "  \"label\": \"" << JsonEscape(LabelOf(options)) << "\",\n"
              << "  \"op\": \"" << JsonEscape(options.op) << "\",\n"
              << "  \"threads\": " << options.threads << ",\n"
              << "  \"duration_s\": " << options.duration_s << ",\n"
              << "  \"warmup_s\": " << options.warmup_s << ",\n"
              << "  \"value_size\": " << options.value_size << ",\n"
              << "  \"keyspace\": " << options.keyspace << ",\n"
              << "  \"success\": " << result.success << ",\n"
              << "  \"timeout\": " << result.timeout << ",\n"
              << "  \"not_leader\": " << result.not_leader << ",\n"
              << "  \"not_found\": " << result.not_found << ",\n"
              << "  \"other_error\": " << result.other_error << ",\n"
              << "  \"open_failures\": " << result.open_failures << ",\n"
              << std::fixed << std::setprecision(3)
              << "  \"elapsed_s\": " << result.elapsed_s << ",\n"
              << "  \"throughput_ops\": " << result.throughput_ops << ",\n"
              << "  \"p50_us\": " << result.p50_us << ",\n"
              << "  \"p90_us\": " << result.p90_us << ",\n"
              << "  \"p99_us\": " << result.p99_us << ",\n"
              << "  \"p999_us\": " << result.p999_us << ",\n"
              << "  \"max_us\": " << result.max_us << ",\n"
              << "  \"prefill_s\": " << result.prefill_s << ",\n";

    PrintJsonSnapshot("server_before", result.before, true);
    PrintJsonSnapshot("server_after", result.after, false);
    std::cout << "}\n";
}

}

int main(int argc, char** argv)
{
    CommandLine command_line;
    cairnkv::BenchOptions& options = command_line.options;

    int index = 1;
    while (index < argc)
    {
        const std::string arg = argv[index];
        std::string value;

        if (arg == "--addr")
        {
            if (!TakeValue(argc, argv, &index, &value) ||
                !ParseAddress(value, &options.host, &options.port))
            {
                std::cerr << "invalid --addr\n";
                return 1;
            }
            continue;
        }
        if (arg == "--op")
        {
            if (!TakeValue(argc, argv, &index, &value))
            {
                std::cerr << "missing --op value\n";
                return 1;
            }
            options.op = value;
            continue;
        }
        if (arg == "--threads" || arg == "--duration" || arg == "--warmup" ||
            arg == "--timeout-ms" || arg == "--keyspace" || arg == "--value-size")
        {
            if (!TakeValue(argc, argv, &index, &value))
            {
                std::cerr << "missing value for " << arg << "\n";
                return 1;
            }
            long long parsed = 0;
            if (!ParseInt(value, &parsed))
            {
                std::cerr << "invalid value for " << arg << "\n";
                return 1;
            }
            if (arg == "--threads")
            {
                if (!InRange(parsed, 1, 1024))
                {
                    std::cerr << "--threads must be in [1, 1024]\n";
                    return 1;
                }
                options.threads = static_cast<int>(parsed);
            }
            else if (arg == "--duration")
            {
                if (!InRange(parsed, 1, 3600))
                {
                    std::cerr << "--duration must be in [1, 3600]\n";
                    return 1;
                }
                options.duration_s = static_cast<int>(parsed);
            }
            else if (arg == "--warmup")
            {
                if (!InRange(parsed, 0, 3600))
                {
                    std::cerr << "--warmup must be in [0, 3600]\n";
                    return 1;
                }
                options.warmup_s = static_cast<int>(parsed);
            }
            else if (arg == "--timeout-ms")
            {
                if (!InRange(parsed, 1, 600000))
                {
                    std::cerr << "--timeout-ms must be in [1, 600000]\n";
                    return 1;
                }
                options.timeout_ms = static_cast<int>(parsed);
            }
            else if (arg == "--keyspace")
            {
                if (!InRange(parsed, 1, 1000000000LL))
                {
                    std::cerr << "--keyspace must be in [1, 1000000000]\n";
                    return 1;
                }
                options.keyspace = static_cast<uint64_t>(parsed);
            }
            else
            {
                if (!InRange(parsed, 1, 65536))
                {
                    std::cerr << "--value-size must be in [1, 65536]\n";
                    return 1;
                }
                options.value_size = static_cast<size_t>(parsed);
            }
            continue;
        }
        if (arg == "--label")
        {
            if (!TakeValue(argc, argv, &index, &value))
            {
                std::cerr << "missing --label value\n";
                return 1;
            }
            options.label = value;
            continue;
        }
        if (arg == "--json")
        {
            command_line.json = true;
            ++index;
            continue;
        }
        if (arg == "--help" || arg == "-h")
        {
            PrintUsage();
            return 0;
        }

        std::cerr << "unknown argument: " << arg << "\n";
        PrintUsage();
        return 1;
    }

    if (options.op != "put" && options.op != "get" &&
        options.op != "stale-get")
    {
        std::cerr << "--op must be put, get, or stale-get\n";
        return 1;
    }

    cairnkv::BenchResult result;
    std::string error;
    if (!cairnkv::RunBenchmark(options, &result, &error))
    {
        std::cerr << "benchmark failed: " << error << "\n";
        return 1;
    }

    if (command_line.json)
    {
        PrintJson(result);
    }
    else
    {
        PrintHuman(result);
    }

    return result.open_failures == 0 ? 0 : 2;
}
