#include <gtest/gtest.h>

#include "common/encoding.h"
#include "rpc/frame.h"
#include "rpc/message.h"

namespace cairnkv
{

TEST(EncodingTest, Crc32KnownVector)
{
    const char* input = "123456789";
    EXPECT_EQ(encoding::Crc32(input, 9), 0xCBF43926u);
}

TEST(EncodingTest, U32RoundTrip)
{
    std::string buffer;
    encoding::AppendU32(&buffer, 0x12345678u);

    size_t pos = 0;
    uint32_t value = 0;
    ASSERT_TRUE(encoding::ReadU32(buffer, &pos, &value));
    EXPECT_EQ(value, 0x12345678u);
    EXPECT_EQ(pos, buffer.size());
}

TEST(EncodingTest, StringRoundTrip)
{
    std::string buffer;
    encoding::AppendString(&buffer, "cairnkv");

    size_t pos = 0;
    std::string value;
    ASSERT_TRUE(encoding::ReadString(buffer, &pos, &value));
    EXPECT_EQ(value, "cairnkv");
    EXPECT_EQ(pos, buffer.size());
}

TEST(FrameTest, RoundTrip)
{
    const std::string payload = "hello-cairn";
    const std::string encoded =
        EncodeFrame(MessageType::kClientRequest, 42, payload);

    Frame frame;
    size_t consumed = 0;
    Status status;
    EXPECT_EQ(TryDecodeFrame(encoded, &frame, &consumed, &status),
              DecodeResult::kOk);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(frame.type, MessageType::kClientRequest);
    EXPECT_EQ(frame.request_id, 42u);
    EXPECT_EQ(frame.payload, payload);
    EXPECT_EQ(consumed, encoded.size());
}

TEST(FrameTest, NeedMore)
{
    const std::string encoded =
        EncodeFrame(MessageType::kClientRequest, 1, "payload");

    Frame frame;
    size_t consumed = 0;
    Status status;
    EXPECT_EQ(TryDecodeFrame(encoded.substr(0, encoded.size() - 1),
                             &frame,
                             &consumed,
                             &status),
              DecodeResult::kNeedMore);
}

TEST(MessageTest, ClientRequestRoundTrip)
{
    ClientRequest request;
    request.op = Op::kPut;
    request.read_mode = ReadMode::kStale;
    request.key = "k";
    request.value = "v";
    request.prefix = "p";
    request.limit = 7;

    const std::string encoded = EncodeClientRequest(request);
    StatusOr<ClientRequest> decoded = DecodeClientRequest(encoded);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded->op, Op::kPut);
    EXPECT_EQ(decoded->read_mode, ReadMode::kStale);
    EXPECT_EQ(decoded->key, "k");
    EXPECT_EQ(decoded->value, "v");
    EXPECT_EQ(decoded->prefix, "p");
    EXPECT_EQ(decoded->limit, 7u);
}

TEST(MessageTest, ClientResponseRoundTrip)
{
    ClientResponse response;
    response.code = StatusCode::kOk;
    response.message = "ok";
    response.value = "value";
    response.applied_index = 99;
    response.items.push_back(KeyValue{"a", "1"});
    response.items.push_back(KeyValue{"b", "2"});
    response.cluster.role = NodeRole::kLeader;
    response.cluster.term = 3;
    response.cluster.leader_id = "node-1";
    response.cluster.commit_index = 10;
    response.cluster.last_applied = 9;
    response.cluster.log_last_index = 10;
    response.cluster.wal_bytes = 1024;

    const std::string encoded = EncodeClientResponse(response);
    StatusOr<ClientResponse> decoded = DecodeClientResponse(encoded);
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded->code, StatusCode::kOk);
    EXPECT_EQ(decoded->message, "ok");
    EXPECT_EQ(decoded->value, "value");
    EXPECT_EQ(decoded->applied_index, 99u);
    ASSERT_EQ(decoded->items.size(), 2u);
    EXPECT_EQ(decoded->items[0].key, "a");
    EXPECT_EQ(decoded->items[1].value, "2");
    EXPECT_EQ(decoded->cluster.leader_id, "node-1");
    EXPECT_EQ(decoded->cluster.wal_bytes, 1024u);
}

}
