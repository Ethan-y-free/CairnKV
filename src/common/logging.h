#pragma once

#include <sstream>
#include <string>

namespace cairnkv
{

enum class LogLevel
{
    kDebug = 0,
    kInfo = 1,
    kWarn = 2,
    kError = 3
};

void SetLogLevel(LogLevel level);
LogLevel GetLogLevel();

void LogLine(LogLevel level,
             const char* file,
             int line,
             const std::string& message);

}

#define CAIRNKV_LOG_DEBUG(expr)                                  \
    do                                                           \
    {                                                            \
        std::ostringstream _cairnkv_oss;                         \
        _cairnkv_oss << expr;                                    \
        ::cairnkv::LogLine(::cairnkv::LogLevel::kDebug,          \
                           __FILE__,                             \
                           __LINE__,                             \
                           _cairnkv_oss.str());                  \
    } while (0)

#define CAIRNKV_LOG_INFO(expr)                                   \
    do                                                           \
    {                                                            \
        std::ostringstream _cairnkv_oss;                         \
        _cairnkv_oss << expr;                                    \
        ::cairnkv::LogLine(::cairnkv::LogLevel::kInfo,           \
                           __FILE__,                             \
                           __LINE__,                             \
                           _cairnkv_oss.str());                  \
    } while (0)

#define CAIRNKV_LOG_WARN(expr)                                   \
    do                                                           \
    {                                                            \
        std::ostringstream _cairnkv_oss;                         \
        _cairnkv_oss << expr;                                    \
        ::cairnkv::LogLine(::cairnkv::LogLevel::kWarn,           \
                           __FILE__,                             \
                           __LINE__,                             \
                           _cairnkv_oss.str());                  \
    } while (0)

#define CAIRNKV_LOG_ERROR(expr)                                  \
    do                                                           \
    {                                                            \
        std::ostringstream _cairnkv_oss;                         \
        _cairnkv_oss << expr;                                    \
        ::cairnkv::LogLine(::cairnkv::LogLevel::kError,          \
                           __FILE__,                             \
                           __LINE__,                             \
                           _cairnkv_oss.str());                  \
    } while (0)
