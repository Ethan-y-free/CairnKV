#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace cairnkv
{

// Keeps client requests off the epoll I/O threads. A client request may block
// until Raft commits, while peer responses must keep flowing on the I/O loops.
class RequestPool
{
public:
    explicit RequestPool(int threads);
    ~RequestPool();

    RequestPool(const RequestPool&) = delete;
    RequestPool& operator=(const RequestPool&) = delete;

    void Stop();
    bool Submit(std::function<void()> task);

private:
    void Run();

    std::vector<std::thread> threads_;
    std::deque<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool running_ = true;
    static constexpr size_t kMaxQueue = 4096;
};

}
