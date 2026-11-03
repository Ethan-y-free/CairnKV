#include <csignal>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <pthread.h>
#include <signal.h>
#include <string>
#include <sys/signalfd.h>
#include <unistd.h>

#include "common/logging.h"
#include "consensus/rpc_server.h"
#include "net/channel.h"
#include "net/connection.h"
#include "net/event_loop.h"
#include "net/tcp_server.h"
#include "raft/raft_node.h"

namespace
{

struct Options
{
    std::string node_id = "node-1";
    std::string listen = "127.0.0.1:6666";
    std::string data_dir = "./cairnkv-data";
    std::string peers_text;
    int io_threads = 4;
};

void PrintUsage()
{
    std::cout
        << "usage: cairnkd [options]\n"
        << "  --id <node-id>        default: node-1\n"
        << "  --listen <host:port>  default: 127.0.0.1:6666\n"
        << "  --data-dir <path>     default: ./cairnkv-data\n"
        << "  --peers <id=host:port,...>\n"
        << "  --io-threads <n>      default: 4\n";
}

bool ParsePeers(const std::string& text,
                std::map<std::string, std::string>* peers)
{
    if (text.empty())
    {
        return true;
    }

    size_t start = 0;
    while (start < text.size())
    {
        const size_t comma = text.find(',', start);
        const std::string part =
            text.substr(start, comma == std::string::npos ? std::string::npos
                                                          : comma - start);
        const size_t equal = part.find('=');
        if (equal == std::string::npos || equal == 0 || equal + 1 >= part.size())
        {
            return false;
        }
        (*peers)[part.substr(0, equal)] = part.substr(equal + 1);
        if (comma == std::string::npos)
        {
            break;
        }
        start = comma + 1;
    }
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options)
{
    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        auto next = [&](std::string* value) {
            if (i + 1 >= argc)
            {
                return false;
            }
            *value = argv[++i];
            return true;
        };

        if (arg == "--id")
        {
            if (!next(&options->node_id))
            {
                return false;
            }
        }
        else if (arg == "--listen")
        {
            if (!next(&options->listen))
            {
                return false;
            }
        }
        else if (arg == "--data-dir")
        {
            if (!next(&options->data_dir))
            {
                return false;
            }
        }
        else if (arg == "--peers")
        {
            if (!next(&options->peers_text))
            {
                return false;
            }
        }
        else if (arg == "--io-threads")
        {
            std::string value;
            if (!next(&value))
            {
                return false;
            }
            options->io_threads = std::atoi(value.c_str());
            if (options->io_threads <= 0)
            {
                return false;
            }
        }
        else if (arg == "--help" || arg == "-h")
        {
            PrintUsage();
            std::exit(0);
        }
        else
        {
            std::cerr << "unknown option: " << arg << "\n";
            return false;
        }
    }
    return true;
}

}

int main(int argc, char** argv)
{
    Options options;
    if (!ParseOptions(argc, argv, &options))
    {
        PrintUsage();
        return 1;
    }

    std::map<std::string, std::string> peers;
    if (!ParsePeers(options.peers_text, &peers))
    {
        std::cerr << "invalid --peers\n";
        return 1;
    }
    if (peers.count(options.node_id) != 0)
    {
        std::cerr << "--peers must not contain this node itself\n";
        return 1;
    }

    cairnkv::SetLogLevel(cairnkv::LogLevel::kInfo);

    // Block signals before any I/O thread starts, then consume them via signalfd.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &mask, nullptr);

    cairnkv::RaftNode::Options raft_options;
    raft_options.node_id = options.node_id;
    raft_options.listen_addr = options.listen;
    raft_options.data_dir = options.data_dir;
    raft_options.peers = peers;

    cairnkv::RaftNode raft(raft_options);
    const cairnkv::Status raft_status = raft.Start();
    if (!raft_status.ok())
    {
        CAIRNKV_LOG_ERROR("failed to start RaftNode: " << raft_status.ToString());
        return 1;
    }

    cairnkv::net::EventLoop main_loop;
    // Declared before the TcpServer so the server (and its EventLoops) is
    // destroyed before the RpcServer object that its callbacks reference.
    cairnkv::RpcServer rpc(&raft);
    cairnkv::net::TcpServer server(&main_loop, options.listen, options.io_threads);

    server.SetConnectionCallback(
        [](const std::shared_ptr<cairnkv::net::Connection>& connection) {
            CAIRNKV_LOG_INFO("new connection fd=" << connection->fd());
        });
    server.SetMessageCallback(
        [&rpc](const std::shared_ptr<cairnkv::net::Connection>& connection,
               cairnkv::net::Buffer* buffer) {
            rpc.OnMessage(connection, buffer);
        });

    const cairnkv::Status start_status = server.Start();
    if (!start_status.ok())
    {
        CAIRNKV_LOG_ERROR("failed to start server: " << start_status.ToString());
        return 1;
    }

    const int signal_fd = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    std::unique_ptr<cairnkv::net::Channel> signal_channel;
    if (signal_fd >= 0)
    {
        signal_channel =
            std::make_unique<cairnkv::net::Channel>(&main_loop, signal_fd);
        signal_channel->SetReadCallback([&main_loop, signal_fd] {
            struct signalfd_siginfo info {};
            while (::read(signal_fd, &info, sizeof(info)) > 0)
            {
            }
            main_loop.Stop();
        });
        signal_channel->EnableReading();
    }

    CAIRNKV_LOG_INFO("cairnkd started: id="
                     << options.node_id << " listen=" << options.listen
                     << " data_dir=" << options.data_dir << " peers="
                     << options.peers_text);
    main_loop.Run();

    // Stop client workers while the network layer is still alive, so their
    // final response writes can still be drained by the I/O loops.
    rpc.Stop();

    signal_channel.reset();
    if (signal_fd >= 0)
    {
        ::close(signal_fd);
    }
    CAIRNKV_LOG_INFO("cairnkd stopped");
    return 0;
}
