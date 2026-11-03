#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#include <unistd.h>

#include "raft/raft_node.h"

namespace cairnkv
{

namespace
{

std::filesystem::path MakeTempDir()
{
    static std::atomic<int> counter{0};
    const auto dir = std::filesystem::temp_directory_path() /
                     ("cairnkv_raft_test_" + std::to_string(::getpid()) + "_" +
                      std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

bool WaitForLeader(RaftNode* node)
{
    for (int i = 0; i < 200; ++i)
    {
        StatusOr<ClusterStatus> status = node->GetStatus();
        if (status.ok() && status->role == NodeRole::kLeader)
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

RaftNode::Options SingleNodeOptions(const std::string& data_dir)
{
    RaftNode::Options options;
    options.node_id = "node-1";
    options.listen_addr = "127.0.0.1:0";
    options.data_dir = data_dir;
    options.election_timeout_min_ms = 30;
    options.election_timeout_max_ms = 60;
    options.heartbeat_interval_ms = 10;
    options.client_timeout_ms = 2000;
    return options;
}

}

TEST(RaftNodeTest, SingleNodeElectsAndServes)
{
    const auto dir = MakeTempDir();
    const std::string data_dir = (dir / "node-1").string();
    const RaftNode::Options options = SingleNodeOptions(data_dir);

    {
        RaftNode node(options);
        ASSERT_TRUE(node.Start().ok());
        ASSERT_TRUE(WaitForLeader(&node));

        StatusOr<uint64_t> put = node.Put("k", "v");
        ASSERT_TRUE(put.ok());

        StatusOr<GetResult> get = node.Get("k", ReadOptions{});
        ASSERT_TRUE(get.ok());
        EXPECT_EQ(get->value, "v");

        node.Stop();
    }

    {
        RaftNode node(options);
        ASSERT_TRUE(node.Start().ok());
        ASSERT_TRUE(WaitForLeader(&node));

        StatusOr<GetResult> get = node.Get("k", ReadOptions{});
        ASSERT_TRUE(get.ok());
        EXPECT_EQ(get->value, "v");

        node.Stop();
    }

    std::filesystem::remove_all(dir);
}

TEST(RaftNodeTest, DeleteIsReplicatedThroughTheLog)
{
    const auto dir = MakeTempDir();
    const std::string data_dir = (dir / "node-1").string();
    const RaftNode::Options options = SingleNodeOptions(data_dir);

    {
        RaftNode node(options);
        ASSERT_TRUE(node.Start().ok());
        ASSERT_TRUE(WaitForLeader(&node));

        ASSERT_TRUE(node.Put("k", "v").ok());
        ASSERT_TRUE(node.Delete("k").ok());
        EXPECT_FALSE(node.Get("k", ReadOptions{}).ok());

        node.Stop();
    }
    std::filesystem::remove_all(dir);
}

}
