#include "net/tcp_server.h"

#include <vector>

#include "common/logging.h"
#include "net/acceptor.h"
#include "net/connection.h"
#include "net/event_loop.h"
#include "net/event_loop_thread_pool.h"

namespace cairnkv::net
{

TcpServer::TcpServer(EventLoop* base_loop,
                     std::string listen_addr,
                     int io_threads)
    : base_loop_(base_loop),
      pool_(std::make_unique<EventLoopThreadPool>(base_loop, io_threads))
{
    acceptor_ = std::make_unique<Acceptor>(base_loop, std::move(listen_addr));
    acceptor_->SetNewConnectionCallback(
        [this](int fd, const struct sockaddr_in& peer) {
            OnNewConnection(fd, peer);
        });
}

TcpServer::~TcpServer()
{
    // Stop I/O loops first, then destroy connections on this thread.
    // Channels remove themselves from their (now stopped) EventLoop.
    pool_->Stop();
    connections_.clear();
    acceptor_.reset();
    pool_.reset();
}

Status TcpServer::Start()
{
    pool_->Start();
    return acceptor_->Start();
}

void TcpServer::SetConnectionCallback(ConnectionCallback callback)
{
    connection_callback_ = std::move(callback);
}

void TcpServer::SetMessageCallback(MessageCallback callback)
{
    message_callback_ = std::move(callback);
}

void TcpServer::OnNewConnection(int fd, const struct sockaddr_in& peer)
{
    (void)peer;
    EventLoop* io_loop = pool_->GetNextLoop();
    io_loop->RunInLoop([this, io_loop, fd] {
        // Destroy the Connection on its EventLoop thread. RPC worker threads
        // may be the last owner of this shared_ptr; the Channel must not be
        // destroyed concurrently with epoll bookkeeping.
        std::shared_ptr<Connection> connection(
            new Connection(io_loop, fd),
            [io_loop](Connection* raw) {
                if (io_loop->IsInLoopThread() || !io_loop->IsRunning())
                {
                    delete raw;
                    return;
                }
                io_loop->RunInLoop([raw] { delete raw; });
            });
        connection->SetMessageCallback(message_callback_);
        connection->SetCloseCallback(
            [this](const std::shared_ptr<Connection>& closed) {
                RemoveConnection(closed);
            });

        {
            std::lock_guard<std::mutex> lock(mutex_);
            connections_[fd] = connection;
        }

        connection->Start();
        if (connection_callback_)
        {
            connection_callback_(connection);
        }
    });
}

void TcpServer::RemoveConnection(const std::shared_ptr<Connection>& connection)
{
    std::lock_guard<std::mutex> lock(mutex_);
    connections_.erase(connection->fd());
}

}
