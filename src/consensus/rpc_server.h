#pragma once

#include <memory>

#include "consensus/request_pool.h"

namespace cairnkv
{

class RaftNode;
struct ClientRequest;
struct ClientResponse;

namespace net
{
class Buffer;
class Connection;
}

class RpcServer
{
public:
    explicit RpcServer(RaftNode* node);

    // Stop accepting client work before the network layer is torn down.
    void Stop();

    void OnMessage(const std::shared_ptr<net::Connection>& connection,
                   net::Buffer* buffer);

private:
    void HandleClientRequest(const ClientRequest& request,
                             ClientResponse* response);
    void SendResponse(const std::shared_ptr<net::Connection>& connection,
                      uint64_t request_id,
                      const ClientResponse& response);

    RaftNode* node_;
    RequestPool pool_;
};

}
