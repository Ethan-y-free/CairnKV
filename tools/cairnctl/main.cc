#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

#include "cairnkv/client.h"

namespace
{

struct Options
{
    std::string host = "127.0.0.1";
    uint16_t port = 6666;
    std::string command;
    std::string key;
    std::string value;
    std::string prefix;
    std::vector<std::string> blocked_peers;
    uint32_t limit = 100;
    bool stale = false;
};

bool ParseAddress(const std::string& address, std::string* host, uint16_t* port)
{
    const size_t pos = address.find_last_of(':');
    if (pos == std::string::npos)
    {
        return false;
    }
    const std::string host_part = address.substr(0, pos);
    const std::string port_part = address.substr(pos + 1);
    char* end = nullptr;
    const long parsed = std::strtol(port_part.c_str(), &end, 10);
    if (end == nullptr || *end != '\0' || parsed <= 0 || parsed > 65535)
    {
        return false;
    }
    *host = host_part.empty() ? "127.0.0.1" : host_part;
    *port = static_cast<uint16_t>(parsed);
    return true;
}

void PrintUsage()
{
    std::cout
        << "usage: cairnctl [--addr host:port] [--stale] <command> [args]\n"
        << "commands:\n"
        << "  put <key> <value>\n"
        << "  get <key>              (--stale: local read, may be old)\n"
        << "  delete <key>\n"
        << "  scan <prefix> [limit]\n"
        << "  status\n"
        << "  partition <id1,id2>    (test hook: drop Raft traffic)\n"
        << "  unpartition";
}

}

int main(int argc, char** argv)
{
    Options options;

    int index = 1;
    while (index < argc)
    {
        const std::string arg = argv[index];
        if (arg == "--addr")
        {
            if (index + 1 >= argc ||
                !ParseAddress(argv[index + 1], &options.host, &options.port))
            {
                std::cerr << "invalid --addr\n";
                return 1;
            }
            index += 2;
            continue;
        }
        if (arg == "--stale")
        {
            options.stale = true;
            ++index;
            continue;
        }
        break;
    }

    if (index >= argc)
    {
        PrintUsage();
        return 1;
    }
    options.command = argv[index++];

    if (options.command == "put")
    {
        if (index + 2 > argc)
        {
            PrintUsage();
            return 1;
        }
        options.key = argv[index++];
        options.value = argv[index++];
    }
    else if (options.command == "get" || options.command == "delete")
    {
        if (index >= argc)
        {
            PrintUsage();
            return 1;
        }
        options.key = argv[index++];
    }
    else if (options.command == "scan")
    {
        if (index >= argc)
        {
            PrintUsage();
            return 1;
        }
        options.prefix = argv[index++];
        if (index < argc)
        {
            options.limit = static_cast<uint32_t>(std::strtoul(argv[index++], nullptr, 10));
        }
    }
    else if (options.command == "partition")
    {
        if (index >= argc)
        {
            PrintUsage();
            return 1;
        }
        const std::string text = argv[index++];
        size_t start = 0;
        while (start < text.size())
        {
            const size_t comma = text.find(',', start);
            const std::string peer_id =
                text.substr(start, comma == std::string::npos
                                       ? std::string::npos
                                       : comma - start);
            if (!peer_id.empty())
            {
                options.blocked_peers.push_back(peer_id);
            }
            if (comma == std::string::npos)
            {
                break;
            }
            start = comma + 1;
        }
        if (options.blocked_peers.empty())
        {
            PrintUsage();
            return 1;
        }
    }
    else if (options.command == "unpartition")
    {
        // Empty blocked_peers clears the partition.
    }
    else if (options.command != "status")
    {
        PrintUsage();
        return 1;
    }

    cairnkv::ClientOptions client_options;
    client_options.host = options.host;
    client_options.port = options.port;

    cairnkv::StatusOr<std::unique_ptr<cairnkv::Client>> client =
        cairnkv::Client::Open(client_options);
    if (!client.ok())
    {
        std::cerr << client.status().ToString() << "\n";
        return 1;
    }

    if (options.command == "partition" || options.command == "unpartition")
    {
        const cairnkv::Status status =
            (*client)->SetBlockedPeers(options.blocked_peers);
        if (!status.ok())
        {
            std::cerr << status.ToString() << "\n";
            return 1;
        }
        std::cout << "OK\n";
        return 0;
    }

    if (options.command == "put")
    {
        const cairnkv::Status status = (*client)->Put(options.key, options.value);
        if (!status.ok())
        {
            std::cerr << status.ToString() << "\n";
            return 1;
        }
        std::cout << "OK\n";
        return 0;
    }

    if (options.command == "get")
    {
        cairnkv::ReadOptions read_options;
        read_options.mode = options.stale ? cairnkv::ReadMode::kStale
                                          : cairnkv::ReadMode::kLinearizable;
        cairnkv::StatusOr<cairnkv::GetResult> result =
            (*client)->Get(options.key, read_options);
        if (!result.ok())
        {
            std::cerr << result.status().ToString() << "\n";
            return 1;
        }
        std::cout << result->value << "\n";
        return 0;
    }

    if (options.command == "delete")
    {
        const cairnkv::Status status = (*client)->Delete(options.key);
        if (!status.ok())
        {
            std::cerr << status.ToString() << "\n";
            return 1;
        }
        std::cout << "OK\n";
        return 0;
    }

    if (options.command == "scan")
    {
        cairnkv::ScanOptions scan_options;
        scan_options.limit = options.limit;
        cairnkv::StatusOr<std::vector<cairnkv::KeyValue>> result =
            (*client)->Scan(options.prefix, scan_options);
        if (!result.ok())
        {
            std::cerr << result.status().ToString() << "\n";
            return 1;
        }
        for (const auto& item : *result)
        {
            std::cout << item.key << "\t" << item.value << "\n";
        }
        return 0;
    }

    cairnkv::StatusOr<cairnkv::ClusterStatus> status = (*client)->GetStatus();
    if (!status.ok())
    {
        std::cerr << status.status().ToString() << "\n";
        return 1;
    }
    const char* role = "follower";
    if (status->role == cairnkv::NodeRole::kLeader)
    {
        role = "leader";
    }
    else if (status->role == cairnkv::NodeRole::kCandidate)
    {
        role = "candidate";
    }
    std::cout << "role=" << role << "\n"
              << "term=" << status->term << "\n"
              << "leader_id=" << status->leader_id << "\n"
              << "commit_index=" << status->commit_index << "\n"
              << "last_applied=" << status->last_applied << "\n"
              << "log_last_index=" << status->log_last_index << "\n"
              << "wal_bytes=" << status->wal_bytes << "\n";
    return 0;
}
