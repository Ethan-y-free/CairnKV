#include <cstdlib>
#include <iostream>
#include <string>

#include "cairnkv/client.h"

namespace
{

bool ParseAddress(const std::string& text,
                  std::string* host,
                  uint16_t* port)
{
    const size_t pos = text.find_last_of(':');
    if (pos == std::string::npos || pos + 1 >= text.size())
    {
        return false;
    }

    *host = text.substr(0, pos);
    const std::string port_text = text.substr(pos + 1);
    char* end = nullptr;
    const long value = std::strtol(port_text.c_str(), &end, 10);
    if (end == nullptr || *end != '\0' || value <= 0 || value > 65535)
    {
        return false;
    }
    *port = static_cast<uint16_t>(value);
    return true;
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

void PrintStatus(const std::string& op, const cairnkv::Status& status)
{
    std::cout << op << " -> " << (status.ok() ? "OK" : status.ToString())
              << "\n";
}

}

int main(int argc, char** argv)
{
    cairnkv::ClientOptions options;
    if (argc >= 3 && std::string(argv[1]) == "--addr")
    {
        if (!ParseAddress(argv[2], &options.host, &options.port))
        {
            std::cerr << "invalid --addr, expected host:port\n";
            return 1;
        }
    }

    auto client = cairnkv::Client::Open(options);
    if (!client.ok())
    {
        std::cerr << "connect failed: " << client.status().ToString() << "\n";
        return 1;
    }

    PrintStatus("put user:1", (*client)->Put("user:1", "alice"));
    PrintStatus("put user:2", (*client)->Put("user:2", "bob"));

    auto linear = (*client)->Get("user:1", cairnkv::ReadOptions{});
    if (linear.ok())
    {
        std::cout << "get linearizable user:1 -> " << linear->value
                  << " (applied_index=" << linear->applied_index << ")\n";
    }
    else
    {
        std::cout << "get linearizable user:1 -> " << linear.status().ToString()
                  << "\n";
    }

    cairnkv::ReadOptions stale;
    stale.mode = cairnkv::ReadMode::kStale;
    auto local = (*client)->Get("user:1", stale);
    if (local.ok())
    {
        std::cout << "get stale user:1 -> " << local->value
                  << " (applied_index=" << local->applied_index << ")\n";
    }
    else
    {
        std::cout << "get stale user:1 -> " << local.status().ToString()
                  << "\n";
    }

    auto scan = (*client)->Scan("user:", cairnkv::ScanOptions{10});
    if (scan.ok())
    {
        std::cout << "scan user: ->";
        for (const cairnkv::KeyValue& kv : *scan)
        {
            std::cout << " " << kv.key << "=" << kv.value;
        }
        std::cout << "\n";
    }
    else
    {
        std::cout << "scan user: -> " << scan.status().ToString() << "\n";
    }

    PrintStatus("delete user:2", (*client)->Delete("user:2"));

    auto missing = (*client)->Get("user:2", cairnkv::ReadOptions{});
    if (!missing.ok())
    {
        std::cout << "get user:2 -> " << missing.status().ToString() << "\n";
    }

    auto status = (*client)->GetStatus();
    if (status.ok())
    {
        std::cout << "status: role=" << RoleName(status->role)
                  << " term=" << status->term
                  << " commit=" << status->commit_index
                  << " applied=" << status->last_applied
                  << " wal_bytes=" << status->wal_bytes << "\n";
    }
    else
    {
        std::cout << "status -> " << status.status().ToString() << "\n";
    }

    return 0;
}
