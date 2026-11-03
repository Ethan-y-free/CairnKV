#include <atomic>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>
#include <unistd.h>

#include "server/kv_service.h"

namespace cairnkv
{

namespace
{

std::filesystem::path MakeTempDir()
{
    static std::atomic<int> counter{0};
    const auto dir = std::filesystem::temp_directory_path() /
                     ("cairnkv_svc_test_" + std::to_string(::getpid()) + "_" +
                      std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

}

TEST(KVServiceTest, PutGetDeleteAndReopen)
{
    const auto dir = MakeTempDir();
    const std::string data_dir = (dir / "data").string();

    {
        KVService service("node-1", data_dir);
        ASSERT_TRUE(service.Open().ok());

        StatusOr<uint64_t> put = service.Put("k", "v");
        ASSERT_TRUE(put.ok());
        EXPECT_EQ(*put, 1u);

        StatusOr<GetResult> get = service.Get("k", ReadOptions{});
        ASSERT_TRUE(get.ok());
        EXPECT_EQ(get->value, "v");
        EXPECT_EQ(get->applied_index, 1u);

        StatusOr<std::vector<KeyValue>> scan = service.Scan("k", 10);
        ASSERT_TRUE(scan.ok());
        ASSERT_EQ(scan->size(), 1u);
    }

    {
        KVService service("node-1", data_dir);
        ASSERT_TRUE(service.Open().ok());

        StatusOr<GetResult> get = service.Get("k", ReadOptions{});
        ASSERT_TRUE(get.ok());
        EXPECT_EQ(get->value, "v");

        ASSERT_TRUE(service.Delete("k").ok());
        EXPECT_FALSE(service.Get("k", ReadOptions{}).ok());
    }

    std::filesystem::remove_all(dir);
}

TEST(KVServiceTest, ReplaysLogOnOpen)
{
    const auto dir = MakeTempDir();
    const std::string data_dir = (dir / "data").string();

    {
        KVService service("node-1", data_dir);
        ASSERT_TRUE(service.Open().ok());
        ASSERT_TRUE(service.Put("a", "1").ok());
        ASSERT_TRUE(service.Put("b", "2").ok());
    }

    // Remove the RocksDB directory: the Raft log must replay into a fresh state machine.
    std::filesystem::remove_all(std::filesystem::path(data_dir) / "state");

    {
        KVService service("node-1", data_dir);
        ASSERT_TRUE(service.Open().ok());

        StatusOr<GetResult> a = service.Get("a", ReadOptions{});
        ASSERT_TRUE(a.ok());
        EXPECT_EQ(a->value, "1");

        StatusOr<GetResult> b = service.Get("b", ReadOptions{});
        ASSERT_TRUE(b.ok());
        EXPECT_EQ(b->value, "2");
    }

    std::filesystem::remove_all(dir);
}

}
