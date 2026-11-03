#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

#include "raft/log_store.h"

namespace cairnkv
{

namespace
{

std::filesystem::path MakeTempDir()
{
    static std::atomic<int> counter{0};
    const auto dir = std::filesystem::temp_directory_path() /
                     ("cairnkv_wal_test_" + std::to_string(::getpid()) + "_" +
                      std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

LogEntry MakePut(uint64_t index, const std::string& key, const std::string& value)
{
    LogEntry entry;
    entry.index = index;
    entry.term = 1;
    entry.type = EntryType::kPut;
    entry.key = key;
    entry.value = value;
    return entry;
}

}

TEST(LogStoreTest, AppendGetAndReopen)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "raft.log").string();

    {
        LogStore store;
        ASSERT_TRUE(store.Open(path).ok());
        ASSERT_TRUE(store.Append(MakePut(1, "a", "1")).ok());
        ASSERT_TRUE(store.Append(MakePut(2, "b", "2")).ok());
        ASSERT_TRUE(store.Sync().ok());

        EXPECT_EQ(store.LastIndex(), 2u);
        EXPECT_EQ(store.LastTerm(), 1u);

        StatusOr<LogEntry> entry = store.Get(2);
        ASSERT_TRUE(entry.ok());
        EXPECT_EQ(entry->key, "b");
        EXPECT_EQ(entry->value, "2");
    }

    {
        LogStore store;
        ASSERT_TRUE(store.Open(path).ok());
        EXPECT_EQ(store.LastIndex(), 2u);
        StatusOr<LogEntry> entry = store.Get(1);
        ASSERT_TRUE(entry.ok());
        EXPECT_EQ(entry->key, "a");
    }

    std::filesystem::remove_all(dir);
}

TEST(LogStoreTest, TruncatesTornTail)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "raft.log").string();

    {
        LogStore store;
        ASSERT_TRUE(store.Open(path).ok());
        ASSERT_TRUE(store.Append(MakePut(1, "a", "1")).ok());
        ASSERT_TRUE(store.Sync().ok());
    }

    const auto valid_size = std::filesystem::file_size(path);
    {
        std::ofstream out(path, std::ios::binary | std::ios::app);
        // payload_len = 100, crc = 0, but no payload follows: a torn write.
        const char header[8] = {100, 0, 0, 0, 0, 0, 0, 0};
        out.write(header, sizeof(header));
    }

    {
        LogStore store;
        ASSERT_TRUE(store.Open(path).ok());
        EXPECT_EQ(store.LastIndex(), 1u);
    }
    EXPECT_EQ(std::filesystem::file_size(path), valid_size);

    std::filesystem::remove_all(dir);
}

TEST(LogStoreTest, TruncateSuffix)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "raft.log").string();

    {
        LogStore store;
        ASSERT_TRUE(store.Open(path).ok());
        ASSERT_TRUE(store.Append(MakePut(1, "a", "1")).ok());
        ASSERT_TRUE(store.Append(MakePut(2, "b", "2")).ok());
        ASSERT_TRUE(store.Append(MakePut(3, "c", "3")).ok());
        ASSERT_TRUE(store.Sync().ok());
        ASSERT_TRUE(store.TruncateSuffix(2).ok());
        EXPECT_EQ(store.LastIndex(), 1u);
    }

    {
        LogStore store;
        ASSERT_TRUE(store.Open(path).ok());
        EXPECT_EQ(store.LastIndex(), 1u);
        EXPECT_FALSE(store.Get(2).ok());
    }

    std::filesystem::remove_all(dir);
}

TEST(LogStoreTest, RejectsNonContiguousIndex)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "raft.log").string();

    LogStore store;
    ASSERT_TRUE(store.Open(path).ok());
    EXPECT_FALSE(store.Append(MakePut(2, "a", "1")).ok());

    std::filesystem::remove_all(dir);
}

}
