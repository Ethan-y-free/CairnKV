#include <atomic>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

#include "raft/log_entry.h"
#include "storage/state_machine.h"

namespace cairnkv
{

namespace
{

std::filesystem::path MakeTempDir()
{
    static std::atomic<int> counter{0};
    const auto dir = std::filesystem::temp_directory_path() /
                     ("cairnkv_sm_test_" + std::to_string(::getpid()) + "_" +
                      std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

LogEntry MakeEntry(uint64_t index, EntryType type, const std::string& key,
                   const std::string& value)
{
    LogEntry entry;
    entry.index = index;
    entry.term = 1;
    entry.type = type;
    entry.key = key;
    entry.value = value;
    return entry;
}

}

TEST(StateMachineTest, ApplyGetDeleteAndReopen)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "state").string();

    {
        StateMachine machine;
        ASSERT_TRUE(machine.Open(path).ok());
        ASSERT_TRUE(machine.Apply(MakeEntry(1, EntryType::kPut, "a", "1")).ok());
        ASSERT_TRUE(machine.Apply(MakeEntry(2, EntryType::kPut, "b", "2")).ok());
        ASSERT_TRUE(machine.Apply(MakeEntry(3, EntryType::kDelete, "a", "")).ok());

        EXPECT_EQ(machine.LastApplied(), 3u);

        StatusOr<std::string> a = machine.Get("a");
        EXPECT_FALSE(a.ok());
        EXPECT_EQ(a.status().code(), StatusCode::kNotFound);

        StatusOr<std::string> b = machine.Get("b");
        ASSERT_TRUE(b.ok());
        EXPECT_EQ(*b, "2");
    }

    {
        StateMachine machine;
        ASSERT_TRUE(machine.Open(path).ok());
        EXPECT_EQ(machine.LastApplied(), 3u);

        StatusOr<std::string> b = machine.Get("b");
        ASSERT_TRUE(b.ok());
        EXPECT_EQ(*b, "2");

        StatusOr<std::vector<KeyValue>> items = machine.Scan("", 10);
        ASSERT_TRUE(items.ok());
        ASSERT_EQ(items->size(), 1u);
        EXPECT_EQ((*items)[0].key, "b");
    }

    std::filesystem::remove_all(dir);
}

TEST(StateMachineTest, RejectsOutOfOrderApply)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "state").string();

    StateMachine machine;
    ASSERT_TRUE(machine.Open(path).ok());
    EXPECT_FALSE(machine.Apply(MakeEntry(2, EntryType::kPut, "a", "1")).ok());
    EXPECT_TRUE(machine.Apply(MakeEntry(1, EntryType::kPut, "a", "1")).ok());

    std::filesystem::remove_all(dir);
}

TEST(StateMachineTest, RejectsReservedPrefix)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "state").string();

    StateMachine machine;
    ASSERT_TRUE(machine.Open(path).ok());
    EXPECT_FALSE(
        machine.Apply(MakeEntry(1, EntryType::kPut, "__meta/x", "1")).ok());

    std::filesystem::remove_all(dir);
}

}
