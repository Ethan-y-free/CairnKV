#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "cairnkv/status.h"
#include "cairnkv/types.h"

namespace cairnkv
{

struct ClientOptions
{
    std::string host = "127.0.0.1";
    uint16_t port = 6666;
    int timeout_ms = 1000;
};

class Client
{
public:
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    static StatusOr<std::unique_ptr<Client>> Open(const ClientOptions& opts);

    Status Put(const std::string& key, const std::string& value);
    StatusOr<GetResult> Get(const std::string& key, ReadOptions opts = {});
    Status Delete(const std::string& key);

    StatusOr<std::vector<KeyValue>> Scan(const std::string& prefix,
                                          ScanOptions opts = {});
    StatusOr<ClusterStatus> GetStatus();

    // Opaque implementation; public so out-of-line member definitions compile.
    struct Impl;

private:
    Client();

    std::unique_ptr<Impl> impl_;
};

}
