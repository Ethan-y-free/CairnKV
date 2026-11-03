#include "raft/hard_state.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/encoding.h"

namespace cairnkv
{

namespace
{

constexpr char kHardStateMagic[4] = {'R', 'H', 'S', '1'};
constexpr uint8_t kHardStateVersion = 1;

std::string ErrnoMessage(const std::string& action)
{
    return action + ": " + std::strerror(errno);
}

Status FsyncParentDirectory(const std::string& path)
{
    const size_t slash = path.find_last_of('/');
    const std::string dir = (slash == std::string::npos) ? "." : path.substr(0, slash);
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0)
    {
        return Status(StatusCode::kInternal,
                      ErrnoMessage("open hard state directory"));
    }
    const int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0)
    {
        return Status(StatusCode::kInternal,
                      ErrnoMessage("fsync hard state directory"));
    }
    return Status::Ok();
}

Status WriteAll(int fd, const std::string& data)
{
    size_t written = 0;
    while (written < data.size())
    {
        const ssize_t n = ::write(fd, data.data() + written, data.size() - written);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return Status(StatusCode::kInternal, ErrnoMessage("write hard state"));
        }
        written += static_cast<size_t>(n);
    }
    return Status::Ok();
}

}

Status SaveHardState(const std::string& path, const HardState& state)
{
    std::string data;
    encoding::AppendBytes(&data, kHardStateMagic, sizeof(kHardStateMagic));
    encoding::AppendU8(&data, kHardStateVersion);
    encoding::AppendU64(&data, state.current_term);
    encoding::AppendString(&data, state.voted_for);
    encoding::AppendU32(&data, encoding::Crc32(data.data(), data.size()));

    const std::string tmp_path = path + ".tmp";
    const int fd = ::open(tmp_path.c_str(),
                          O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC,
                          0644);
    if (fd < 0)
    {
        return Status(StatusCode::kInternal, ErrnoMessage("open hard state tmp"));
    }

    Status status = WriteAll(fd, data);
    if (status.ok() && ::fsync(fd) != 0)
    {
        status = Status(StatusCode::kInternal, ErrnoMessage("fsync hard state tmp"));
    }
    ::close(fd);

    if (!status.ok())
    {
        (void)::unlink(tmp_path.c_str());
        return status;
    }

    if (::rename(tmp_path.c_str(), path.c_str()) != 0)
    {
        (void)::unlink(tmp_path.c_str());
        return Status(StatusCode::kInternal, ErrnoMessage("rename hard state"));
    }
    return FsyncParentDirectory(path);
}

StatusOr<HardState> LoadHardState(const std::string& path)
{
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
    {
        if (errno == ENOENT)
        {
            return HardState{};
        }
        return Status(StatusCode::kInternal, ErrnoMessage("open hard state"));
    }

    struct stat st {};
    if (::fstat(fd, &st) != 0)
    {
        ::close(fd);
        return Status(StatusCode::kInternal, ErrnoMessage("fstat hard state"));
    }

    const size_t size = static_cast<size_t>(st.st_size);
    std::string data(size, '\0');
    size_t read_total = 0;
    while (read_total < size)
    {
        const ssize_t n = ::read(fd, data.data() + read_total, size - read_total);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            ::close(fd);
            return Status(StatusCode::kInternal, ErrnoMessage("read hard state"));
        }
        if (n == 0)
        {
            break;
        }
        read_total += static_cast<size_t>(n);
    }
    ::close(fd);
    data.resize(read_total);

    if (data.size() < sizeof(kHardStateMagic) + 1 + 8 + 4 + 4)
    {
        return Status(StatusCode::kCorruption, "hard state file too short");
    }
    if (std::string_view(data).substr(0, sizeof(kHardStateMagic)) !=
        std::string_view(kHardStateMagic, sizeof(kHardStateMagic)))
    {
        return Status(StatusCode::kCorruption, "bad hard state magic");
    }

    const size_t crc_offset = data.size() - 4;
    size_t pos = crc_offset;
    uint32_t stored_crc = 0;
    if (!encoding::ReadU32(data, &pos, &stored_crc))
    {
        return Status(StatusCode::kCorruption, "truncated hard state CRC");
    }
    const uint32_t actual_crc = encoding::Crc32(data.data(), crc_offset);
    if (stored_crc != actual_crc)
    {
        return Status(StatusCode::kCorruption, "hard state CRC mismatch");
    }

    pos = sizeof(kHardStateMagic);
    uint8_t version = 0;
    HardState state;
    if (!encoding::ReadU8(data, &pos, &version) ||
        !encoding::ReadU64(data, &pos, &state.current_term) ||
        !encoding::ReadString(data, &pos, &state.voted_for))
    {
        return Status(StatusCode::kCorruption, "truncated hard state body");
    }
    if (version != kHardStateVersion)
    {
        return Status(StatusCode::kCorruption, "unsupported hard state version");
    }
    if (pos != crc_offset)
    {
        return Status(StatusCode::kCorruption, "trailing bytes in hard state");
    }
    return state;
}

}
