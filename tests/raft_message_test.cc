#include <gtest/gtest.h>

#include "raft/raft_message.h"

namespace cairnkv
{

TEST(RaftMessageTest, RequestVoteRoundTrip)
{
    RaftMessage message;
    message.type = RaftMessageType::kRequestVote;
    message.term = 7;
    message.from_id = "node-1";
    message.to_id = "node-2";
    message.last_log_index = 5;
    message.last_log_term = 6;

    const std::string encoded = EncodeRaftMessage(message);
    StatusOr<RaftMessage> decoded = DecodeRaftMessage(encoded);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded->type, RaftMessageType::kRequestVote);
    EXPECT_EQ(decoded->term, 7u);
    EXPECT_EQ(decoded->from_id, "node-1");
    EXPECT_EQ(decoded->to_id, "node-2");
    EXPECT_EQ(decoded->last_log_index, 5u);
    EXPECT_EQ(decoded->last_log_term, 6u);
}

TEST(RaftMessageTest, AppendEntriesRoundTrip)
{
    RaftMessage message;
    message.type = RaftMessageType::kAppendEntries;
    message.term = 3;
    message.from_id = "leader";
    message.to_id = "follower";
    message.leader_id = "leader";
    message.prev_log_index = 4;
    message.prev_log_term = 2;
    message.leader_commit = 4;
    message.read_context = 42;

    LogEntry put;
    put.index = 5;
    put.term = 3;
    put.type = EntryType::kPut;
    put.key = "k";
    put.value = "v";
    message.entries.push_back(put);

    LogEntry noop;
    noop.index = 6;
    noop.term = 3;
    noop.type = EntryType::kNoop;
    message.entries.push_back(noop);

    const std::string encoded = EncodeRaftMessage(message);
    StatusOr<RaftMessage> decoded = DecodeRaftMessage(encoded);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded->type, RaftMessageType::kAppendEntries);
    EXPECT_EQ(decoded->leader_id, "leader");
    EXPECT_EQ(decoded->prev_log_index, 4u);
    EXPECT_EQ(decoded->prev_log_term, 2u);
    EXPECT_EQ(decoded->leader_commit, 4u);
    EXPECT_EQ(decoded->read_context, 42u);
    ASSERT_EQ(decoded->entries.size(), 2u);
    EXPECT_EQ(decoded->entries[0].key, "k");
    EXPECT_EQ(decoded->entries[0].value, "v");
    EXPECT_EQ(decoded->entries[1].type, EntryType::kNoop);
}

TEST(RaftMessageTest, RejectsBadMagic)
{
    std::string payload = "BAD!" + std::string(32, '\0');
    StatusOr<RaftMessage> decoded = DecodeRaftMessage(payload);
    EXPECT_FALSE(decoded.ok());
    EXPECT_EQ(decoded.status().code(), StatusCode::kCorruption);
}

}
