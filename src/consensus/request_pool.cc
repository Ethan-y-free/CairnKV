#include "consensus/request_pool.h"

#include <utility>

namespace cairnkv
{

RequestPool::RequestPool(int threads)
{
    if (threads <= 0)
    {
        threads = 1;
    }
    threads_.reserve(static_cast<size_t>(threads));
    for (int i = 0; i < threads; ++i)
    {
        threads_.emplace_back([this] { Run(); });
    }
}

RequestPool::~RequestPool()
{
    Stop();
}

void RequestPool::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_)
        {
            return;
        }
        running_ = false;
    }
    cv_.notify_all();
    for (auto& thread : threads_)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
    threads_.clear();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.clear();
    }
}

bool RequestPool::Submit(std::function<void()> task)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || tasks_.size() >= kMaxQueue)
        {
            return false;
        }
        tasks_.push_back(std::move(task));
    }
    cv_.notify_one();
    return true;
}

void RequestPool::Run()
{
    while (true)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this] { return !running_ || !tasks_.empty(); });
            if (!running_)
            {
                return;
            }
            task = std::move(tasks_.front());
            tasks_.pop_front();
        }
        task();
    }
}

}
