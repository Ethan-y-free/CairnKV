#include "raft/log_store.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/encoding.h"

namespace cairnkv
{

namespace
{

std::string ErrnoMessage(const std::string& action)
{
    return action + ": " + std::strerror(errno);
}

}

LogStore::~LogStore()
{
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

Status LogStore::Open(const std::string& path)
{
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }

    path_ = path;
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd_ < 0)
    {
        return Status(StatusCode::kInternal, ErrnoMessage("open WAL"));
    }

    struct stat st {};
    if (::fstat(fd_, &st) != 0)
    {
        return Status(StatusCode::kInternal, ErrnoMessage("fstat WAL"));
    }

    const size_t size = static_cast<size_t>(st.st_size);
    std::string data(size, '\0');
    size_t read_total = 0;
    while (read_total < size)
    {
        const ssize_t n = ::pread(fd_,
                                  data.data() + read_total,
                                  size - read_total,
                                  static_cast<off_t>(read_total));
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return Status(StatusCode::kInternal, ErrnoMessage("read WAL"));
        }
        if (n == 0)
        {
            break;
        }
        read_total += static_cast<size_t>(n);
    }
    data.resize(read_total);

    entries_.clear();
    offsets_.clear();
    file_size_ = 0;
    return Recover(data);
}

Status LogStore::Recover(const std::string& data)
{
    size_t pos = 0;
    uint64_t expected_index = 1;
    bool torn_tail = false;

    while (pos < data.size())
    {
        if (data.size() - pos < 8)
        {
            torn_tail = true;
            break;
        }

        const std::string_view view(data);
        size_t header_pos = pos;
        uint32_t payload_len = 0;
        uint32_t stored_crc = 0;
        if (!encoding::ReadU32(view, &header_pos, &payload_len) ||
            !encoding::ReadU32(view, &header_pos, &stored_crc))
        {
            torn_tail = true;
            break;
        }

        if (payload_len > kMaxLogEntryPayload)
        {
            return Status(StatusCode::kCorruption, "WAL record too large");
        }
        if (pos + 8 + payload_len > data.size())
        {
            torn_tail = true;
            break;
        }

        const uint32_t actual_crc =
            encoding::Crc32(data.data() + pos + 8, payload_len);
        if (actual_crc != stored_crc)
        {
            // A bad CRC at the last record is a normal torn write.
            if (pos + 8 + payload_len == data.size())
            {
                torn_tail = true;
                break;
            }
            return Status(StatusCode::kCorruption,
                          "WAL CRC mismatch in the middle of the log");
        }

        const std::string_view payload(data.data() + pos + 8, payload_len);
        StatusOr<LogEntry> entry = DecodeLogEntry(payload);
        if (!entry.ok())
        {
            return entry.status();
        }
        if (entry->index != expected_index)
        {
            return Status(StatusCode::kCorruption,
                          "WAL log index is not contiguous");
        }

        entries_.push_back(*entry);
        offsets_.push_back(pos);
        ++expected_index;
        pos += 8 + payload_len;
    }

    if (torn_tail)
    {
        if (::ftruncate(fd_, static_cast<off_t>(pos)) != 0)
        {
            return Status(StatusCode::kInternal,
                          ErrnoMessage("truncate torn WAL tail"));
        }
        if (::fsync(fd_) != 0)
        {
            return Status(StatusCode::kInternal,
                          ErrnoMessage("fsync truncated WAL"));
        }
    }

    file_size_ = pos;
    return Status::Ok();
}

Status LogStore::Append(const LogEntry& entry)
{
    if (fd_ < 0)
    {
        return Status(StatusCode::kInternal, "WAL is not open");
    }
    if (entry.index != entries_.size() + 1)
    {
        return Status(StatusCode::kInvalidArgument,
                      "log index must be the next contiguous index");
    }

    const std::string payload = EncodeLogEntry(entry);
    if (payload.size() > kMaxLogEntryPayload)
    {
        return Status(StatusCode::kInvalidArgument, "log entry too large");
    }

    std::string record;
    record.reserve(8 + payload.size());
    encoding::AppendU32(&record, static_cast<uint32_t>(payload.size()));
    encoding::AppendU32(&record,
                        encoding::Crc32(payload.data(), payload.size()));
    record.append(payload);

    const uint64_t record_offset = file_size_;
    size_t written = 0;
    while (written < record.size())
    {
        const ssize_t n = ::write(fd_,
                                  record.data() + written,
                                  record.size() - written);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            // Roll back a partial record so the WAL stays parseable.
            (void)::ftruncate(fd_, static_cast<off_t>(record_offset));
            return Status(StatusCode::kInternal, ErrnoMessage("append WAL"));
        }
        written += static_cast<size_t>(n);
    }

    entries_.push_back(entry);
    offsets_.push_back(record_offset);
    file_size_ += record.size();
    return Status::Ok();
}

Status LogStore::Sync()
{
    if (fd_ < 0)
    {
        return Status(StatusCode::kInternal, "WAL is not open");
    }
    if (::fdatasync(fd_) != 0)
    {
        return Status(StatusCode::kInternal, ErrnoMessage("fsync WAL"));
    }
    return Status::Ok();
}

StatusOr<LogEntry> LogStore::Get(uint64_t index) const
{
    if (index == 0 || index > entries_.size())
    {
        return Status(StatusCode::kNotFound, "log entry is not available");
    }
    return entries_[static_cast<size_t>(index - 1)];
}

uint64_t LogStore::LastIndex() const
{
    return entries_.size();
}

uint64_t LogStore::LastTerm() const
{
    if (entries_.empty())
    {
        return 0;
    }
    return entries_.back().term;
}

uint64_t LogStore::FileSize() const
{
    return file_size_;
}

Status LogStore::TruncateSuffix(uint64_t from_index)
{
    if (fd_ < 0)
    {
        return Status(StatusCode::kInternal, "WAL is not open");
    }
    if (from_index == 0 || from_index > entries_.size() + 1)
    {
        return Status(StatusCode::kInvalidArgument, "invalid truncate index");
    }
    if (from_index == entries_.size() + 1)
    {
        return Status::Ok();
    }

    const uint64_t offset = offsets_[static_cast<size_t>(from_index - 1)];
    if (::ftruncate(fd_, static_cast<off_t>(offset)) != 0)
    {
        return Status(StatusCode::kInternal, ErrnoMessage("truncate WAL"));
    }
    if (::fsync(fd_) != 0)
    {
        return Status(StatusCode::kInternal, ErrnoMessage("fsync WAL"));
    }

    entries_.resize(static_cast<size_t>(from_index - 1));
    offsets_.resize(static_cast<size_t>(from_index - 1));
    file_size_ = offset;
    return Status::Ok();
}

}
