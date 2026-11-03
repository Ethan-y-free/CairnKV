#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

#include "raft/hard_state.h"

namespace cairnkv
{

namespace
{

std::filesystem::path MakeTempDir()
{
    static std::atomic<int> counter{0};
    const auto dir = std::filesystem::temp_directory_path() /
                     ("cairnkv_hs_test_" + std::to_string(::getpid()) + "_" +
                      std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

}

TEST(HardStateTest, MissingFileReturnsDefault)
{
    const auto dir = MakeTempDir();
    StatusOr<HardState> state = LoadHardState((dir / "hard_state").string());
    ASSERT_TRUE(state.ok());
    EXPECT_EQ(state->current_term, 0u);
    EXPECT_TRUE(state->voted_for.empty());
    std::filesystem::remove_all(dir);
}

TEST(HardStateTest, SaveLoadAndOverwrite)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "hard_state").string();

    HardState first;
    first.current_term = 7;
    first.voted_for = "node-2";
    ASSERT_TRUE(SaveHardState(path, first).ok());

    StatusOr<HardState> loaded = LoadHardState(path);
    ASSERT_TRUE(loaded.ok());
    EXPECT_EQ(loaded->current_term, 7u);
    EXPECT_EQ(loaded->voted_for, "node-2");

    HardState second;
    second.current_term = 8;
    ASSERT_TRUE(SaveHardState(path, second).ok());

    loaded = LoadHardState(path);
    ASSERT_TRUE(loaded.ok());
    EXPECT_EQ(loaded->current_term, 8u);
    EXPECT_TRUE(loaded->voted_for.empty());

    std::filesystem::remove_all(dir);
}

TEST(HardStateTest, RejectsCorruption)
{
    const auto dir = MakeTempDir();
    const std::string path = (dir / "hard_state").string();

    {
        std::ofstream out(path, std::ios::binary);
        out << "not-a-hard-state";
    }

    StatusOr<HardState> loaded = LoadHardState(path);
    EXPECT_FALSE(loaded.ok());
    EXPECT_EQ(loaded.status().code(), StatusCode::kCorruption);

    std::filesystem::remove_all(dir);
}

}
