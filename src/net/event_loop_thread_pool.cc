#include "net/event_loop_thread_pool.h"

#include <future>
#include <utility>

#include "net/event_loop.h"

namespace cairnkv::net
{

EventLoopThreadPool::EventLoopThreadPool(EventLoop* base_loop, int num_threads)
    : base_loop_(base_loop), num_threads_(num_threads < 0 ? 0 : num_threads)
{
}

EventLoopThreadPool::~EventLoopThreadPool()
{
    Stop();
}

void EventLoopThreadPool::Stop()
{
    for (auto& loop : loops_)
    {
        if (loop)
        {
            loop->Stop();
        }
    }
    for (auto& thread : threads_)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
    threads_.clear();
    started_ = false;
}

void EventLoopThreadPool::Start()
{
    if (started_ || num_threads_ == 0)
    {
        started_ = true;
        return;
    }

    loops_.resize(static_cast<size_t>(num_threads_));
    threads_.reserve(static_cast<size_t>(num_threads_));

    for (int i = 0; i < num_threads_; ++i)
    {
        auto promise = std::make_shared<std::promise<EventLoop*>>();
        auto future = promise->get_future();
        threads_.emplace_back([this, i, promise] {
            auto loop = std::make_unique<EventLoop>();
            EventLoop* raw = loop.get();
            loops_[static_cast<size_t>(i)] = std::move(loop);
            promise->set_value(raw);
            raw->Run();
        });
        future.get();
    }

    started_ = true;
}

EventLoop* EventLoopThreadPool::GetNextLoop()
{
    if (num_threads_ == 0 || loops_.empty())
    {
        return base_loop_;
    }
    EventLoop* loop = loops_[static_cast<size_t>(next_)].get();
    next_ = (next_ + 1) % num_threads_;
    return loop;
}

}
