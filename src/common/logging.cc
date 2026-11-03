#include "common/logging.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>
#include <functional>
#include <mutex>
#include <thread>

namespace cairnkv
{

namespace
{

std::atomic<LogLevel> g_log_level{LogLevel::kInfo};
std::mutex g_log_mutex;

const char* LevelName(LogLevel level)
{
    switch (level)
    {
        case LogLevel::kDebug:
            return "DEBUG";
        case LogLevel::kInfo:
            return "INFO";
        case LogLevel::kWarn:
            return "WARN";
        case LogLevel::kError:
            return "ERROR";
    }
    return "UNKNOWN";
}

}

void SetLogLevel(LogLevel level)
{
    g_log_level.store(level);
}

LogLevel GetLogLevel()
{
    return g_log_level.load();
}

void LogLine(LogLevel level,
             const char* file,
             int line,
             const std::string& message)
{
    if (level < g_log_level.load())
    {
        return;
    }

    const auto now = std::chrono::system_clock::now();
    const auto time = std::chrono::system_clock::to_time_t(now);
    const auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now.time_since_epoch()) %
                        1000;

    std::tm tm{};
    localtime_r(&time, &tm);

    char timestamp[64];
    std::snprintf(timestamp,
                  sizeof(timestamp),
                  "%04d-%02d-%02d %02d:%02d:%02d.%03lld",
                  tm.tm_year + 1900,
                  tm.tm_mon + 1,
                  tm.tm_mday,
                  tm.tm_hour,
                  tm.tm_min,
                  tm.tm_sec,
                  static_cast<long long>(millis.count()));

    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::fprintf(stderr,
                 "%s %-5s [%s:%d] [tid=%llu] %s\n",
                 timestamp,
                 LevelName(level),
                 file,
                 line,
                 static_cast<unsigned long long>(
                     std::hash<std::thread::id>{}(std::this_thread::get_id())),
                 message.c_str());
    std::fflush(stderr);
}

}
