#include "consensus/rpc_server.h"

#include <utility>

#include "common/logging.h"
#include "net/buffer.h"
#include "net/connection.h"
#include "raft/raft_node.h"
#include "rpc/frame.h"
#include "rpc/message.h"

namespace cairnkv
{

namespace
{

void SetError(ClientResponse* response, const Status& status)
{
    response->code = status.code();
    response->message = status.message();
    if (status.code() == StatusCode::kNotLeader)
    {
        response->leader_hint = status.message();
    }
}

}

RpcServer::RpcServer(RaftNode* node)
    : node_(node), pool_(4)
{
}

void RpcServer::Stop()
{
    pool_.Stop();
}

void RpcServer::HandleClientRequest(const ClientRequest& request,
                                    ClientResponse* response)
{
    switch (request.op)
    {
        case Op::kPut:
        {
            StatusOr<uint64_t> result = node_->Put(request.key, request.value);
            if (!result.ok())
            {
                SetError(response, result.status());
            }
            else
            {
                response->applied_index = *result;
            }
            break;
        }
        case Op::kDelete:
        {
            const Status result = node_->Delete(request.key);
            if (!result.ok())
            {
                SetError(response, result);
            }
            break;
        }
        case Op::kGet:
        {
            StatusOr<GetResult> result =
                node_->Get(request.key, ReadOptions{request.read_mode});
            if (!result.ok())
            {
                SetError(response, result.status());
            }
            else
            {
                response->value = result->value;
                response->applied_index = result->applied_index;
            }
            break;
        }
        case Op::kScan:
        {
            StatusOr<std::vector<KeyValue>> result =
                node_->Scan(request.prefix, request.limit);
            if (!result.ok())
            {
                SetError(response, result.status());
            }
            else
            {
                response->items = *result;
            }
            break;
        }
        case Op::kStatus:
        {
            StatusOr<ClusterStatus> result = node_->GetStatus();
            if (!result.ok())
            {
                SetError(response, result.status());
            }
            else
            {
                response->cluster = *result;
            }
            break;
        }
    }
}

void RpcServer::OnMessage(const std::shared_ptr<net::Connection>& connection,
                          net::Buffer* buffer)
{
    while (buffer->ReadableBytes() > 0)
    {
        Frame frame;
        size_t consumed = 0;
        Status status;
        const DecodeResult result = TryDecodeFrame(
            std::string_view(buffer->Peek(), buffer->ReadableBytes()),
            &frame,
            &consumed,
            &status);

        if (result == DecodeResult::kNeedMore)
        {
            return;
        }
        if (result == DecodeResult::kError)
        {
            CAIRNKV_LOG_WARN("invalid RPC frame from fd="
                             << connection->fd() << ": " << status.ToString());
            connection->Close();
            return;
        }

        buffer->Retrieve(consumed);

        if (frame.type == MessageType::kClientRequest)
        {
            StatusOr<ClientRequest> request = DecodeClientRequest(frame.payload);
            if (!request.ok())
            {
                ClientResponse response;
                SetError(&response, request.status());
                SendResponse(connection, frame.request_id, response);
                continue;
            }

            const uint64_t request_id = frame.request_id;
            ClientRequest parsed = std::move(*request);
            const bool submitted = pool_.Submit(
                [this, connection, request_id, parsed]() mutable {
                    ClientResponse response;
                    HandleClientRequest(parsed, &response);
                    SendResponse(connection, request_id, response);
                });
            if (!submitted)
            {
                ClientResponse response;
                response.code = StatusCode::kBusy;
                response.message = "server busy";
                SendResponse(connection, request_id, response);
            }
            continue;
        }

        if (frame.type == MessageType::kRaftRequest)
        {
            StatusOr<RaftMessage> message = DecodeRaftMessage(frame.payload);
            if (!message.ok())
            {
                CAIRNKV_LOG_WARN("invalid Raft message from fd="
                                 << connection->fd());
                connection->Close();
                return;
            }

            const RaftMessage response = node_->HandleRaftRequest(*message);
            connection->Send(EncodeFrame(MessageType::kRaftResponse,
                                         frame.request_id,
                                         EncodeRaftMessage(response)));
            continue;
        }

        CAIRNKV_LOG_WARN("unexpected RPC message type from fd="
                         << connection->fd());
        connection->Close();
        return;
    }
}

void RpcServer::SendResponse(
    const std::shared_ptr<net::Connection>& connection,
    uint64_t request_id,
    const ClientResponse& response)
{
    const std::string payload = EncodeClientResponse(response);
    connection->Send(EncodeFrame(MessageType::kClientResponse,
                                 request_id,
                                 payload));
}

}
