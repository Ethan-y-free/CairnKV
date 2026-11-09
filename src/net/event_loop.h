#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace cairnkv::net
{

class Channel;

class EventLoop
{
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void Run();
    void Stop();

    void RunInLoop(std::function<void()> callback);
    void QueueInLoop(std::function<void()> callback);
    bool IsInLoopThread() const;
    bool IsRunning() const
    {
        return running_.load();
    }

    void UpdateChannel(Channel* channel);
    void RemoveChannel(Channel* channel);

private:
    void Wakeup();
    void HandleWakeup();
    void DoPending();

    int epoll_fd_ = -1;
    int wakeup_fd_ = -1;
    std::atomic<bool> running_{true};
    // Captured in the constructor. EventLoopThreadPool constructs each loop
    // inside the thread that runs it and waits on a promise before handing
    // the pointer out, so concurrent readers are safe.
    const std::thread::id thread_id_;
    std::mutex mutex_;
    std::vector<std::function<void()>> pending_;
    std::unordered_map<int, Channel*> channels_;
    std::unique_ptr<Channel> wakeup_channel_;
};

}
