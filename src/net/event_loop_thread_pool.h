#pragma once

#include <memory>
#include <thread>
#include <vector>

namespace cairnkv::net
{

class EventLoop;

class EventLoopThreadPool
{
public:
    EventLoopThreadPool(EventLoop* base_loop, int num_threads);
    ~EventLoopThreadPool();

    EventLoopThreadPool(const EventLoopThreadPool&) = delete;
    EventLoopThreadPool& operator=(const EventLoopThreadPool&) = delete;

    void Start();
    void Stop();
    EventLoop* GetNextLoop();

private:
    EventLoop* base_loop_;
    int num_threads_;
    int next_ = 0;
    bool started_ = false;
    std::vector<std::unique_ptr<EventLoop>> loops_;
    std::vector<std::thread> threads_;
};

}
