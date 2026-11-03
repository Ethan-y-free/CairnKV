#include "storage/state_machine.h"

#include <memory>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "common/encoding.h"

namespace cairnkv
{

namespace
{

constexpr const char* kMetaLastApplied = "__meta/last_applied";
constexpr uint32_t kDefaultScanLimit = 100;
constexpr uint32_t kMaxScanLimit = 1000;

bool StartsWith(std::string_view value, std::string_view prefix)
{
    return value.size() >= prefix.size() &&
           value.substr(0, prefix.size()) == prefix;
}

}

StateMachine::~StateMachine()
{
    delete db_;
    db_ = nullptr;
}

Status StateMachine::Open(const std::string& path)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::kNoCompression;

    rocksdb::DB* db = nullptr;
    const rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
    if (!status.ok())
    {
        return Status(StatusCode::kInternal,
                      "open RocksDB: " + status.ToString());
    }

    delete db_;
    db_ = db;

    std::string value;
    const rocksdb::Status read_status =
        db_->Get(rocksdb::ReadOptions(), kMetaLastApplied, &value);
    if (read_status.IsNotFound())
    {
        last_applied_.store(0);
        return Status::Ok();
    }
    if (!read_status.ok())
    {
        return Status(StatusCode::kInternal,
                      "read last_applied: " + read_status.ToString());
    }

    size_t pos = 0;
    uint64_t applied = 0;
    if (!encoding::ReadU64(value, &pos, &applied) || pos != value.size())
    {
        return Status(StatusCode::kCorruption, "bad last_applied value");
    }
    last_applied_.store(applied);
    return Status::Ok();
}

Status StateMachine::Apply(const LogEntry& entry)
{
    if (db_ == nullptr)
    {
        return Status(StatusCode::kInternal, "state machine is not open");
    }

    const uint64_t current = last_applied_.load();
    if (entry.index <= current)
    {
        // Replaying an already-applied entry is a no-op.
        return Status::Ok();
    }
    if (entry.index != current + 1)
    {
        return Status(StatusCode::kInvalidArgument,
                      "log index must be last_applied + 1");
    }

    rocksdb::WriteBatch batch;
    switch (entry.type)
    {
        case EntryType::kNoop:
            break;
        case EntryType::kPut:
            if (StartsWith(entry.key, "__meta/"))
            {
                return Status(StatusCode::kInvalidArgument,
                              "user key uses reserved prefix");
            }
            batch.Put(entry.key, entry.value);
            break;
        case EntryType::kDelete:
            if (StartsWith(entry.key, "__meta/"))
            {
                return Status(StatusCode::kInvalidArgument,
                              "user key uses reserved prefix");
            }
            batch.Delete(entry.key);
            break;
    }

    std::string applied;
    encoding::AppendU64(&applied, entry.index);
    batch.Put(kMetaLastApplied, applied);

    const rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
    {
        return Status(StatusCode::kInternal,
                      "apply to RocksDB: " + status.ToString());
    }

    last_applied_.store(entry.index);
    return Status::Ok();
}

StatusOr<std::string> StateMachine::Get(const std::string& key) const
{
    if (db_ == nullptr)
    {
        return Status(StatusCode::kInternal, "state machine is not open");
    }
    if (StartsWith(key, "__meta/"))
    {
        return Status(StatusCode::kInvalidArgument,
                      "user key uses reserved prefix");
    }

    std::string value;
    const rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (status.IsNotFound())
    {
        return Status(StatusCode::kNotFound, "key not found");
    }
    if (!status.ok())
    {
        return Status(StatusCode::kInternal,
                      "read RocksDB: " + status.ToString());
    }
    return value;
}

StatusOr<std::vector<KeyValue>> StateMachine::Scan(const std::string& prefix,
                                                    uint32_t limit) const
{
    if (db_ == nullptr)
    {
        return Status(StatusCode::kInternal, "state machine is not open");
    }
    if (limit == 0)
    {
        limit = kDefaultScanLimit;
    }
    if (limit > kMaxScanLimit)
    {
        limit = kMaxScanLimit;
    }

    std::vector<KeyValue> result;
    std::unique_ptr<rocksdb::Iterator> iter(
        db_->NewIterator(rocksdb::ReadOptions()));

    if (prefix.empty())
    {
        iter->SeekToFirst();
    }
    else
    {
        iter->Seek(prefix);
    }

    for (; iter->Valid(); iter->Next())
    {
        const std::string key = iter->key().ToString();
        if (!StartsWith(key, prefix))
        {
            break;
        }
        if (StartsWith(key, "__meta/"))
        {
            continue;
        }
        result.push_back(KeyValue{key, iter->value().ToString()});
        if (result.size() >= limit)
        {
            break;
        }
    }

    if (!iter->status().ok())
    {
        return Status(StatusCode::kInternal,
                      "scan RocksDB: " + iter->status().ToString());
    }
    return result;
}

uint64_t StateMachine::LastApplied() const
{
    return last_applied_.load();
}

}
