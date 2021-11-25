#include "redis_strings.h"
#include "scope_record_lock.h"
#include "scope_snapshot.h"
#include "strings_filter.h"

namespace blackwidow {

RedisStrings::RedisStrings(BlackWidow* const bw) : Redis(bw, kStrings) {}

Status RedisStrings::Open(const BlackWidowOptions& bw_options,
                          const std::string& dbpath) {
  rocksdb::Options ops(bw_options.options);

  // CompactionFilter中删除ttl过期的string
  ops.compaction_filter_factory.reset(new StringsFilterFactory());

  // 使用缓存提高查询效率 布隆过滤器减少无效的磁盘seek
  rocksdb::BlockBasedTableOptions table_ops(bw_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  if (bw_options.share_block_cache == false &&
      bw_options.block_cache_size > 0) {
    table_ops.block_cache = rocksdb::NewLRUCache(bw_options.block_cache_size);
  }
  ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_ops));
  return rocksdb::DB::Open(ops, dbpath, &db_);
}

Status RedisStrings::CompactRange(const rocksdb::Slice* begin,
                                  const rocksdb::Slice* end,
                                  const ColumnFamilyType& type) {
  // strings只有1个默认的列族
  return db_->CompactRange(default_compact_range_options_, begin, end);
}

Status RedisStrings::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  auto s = db_->GetProperty(db_->DefaultColumnFamily(), property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

Status RedisStrings::ScanKeyNum(KeyInfo* key_info) {
  // TODO
  return Status::OK();
}
Status RedisStrings::ScanKeys(const std::string& pattern,
                              std::vector<std::string>* keys) {
  // TODO
  return Status::OK();
}
Status RedisStrings::PKPatternMatchDel(const std::string& pattern,
                                       int32_t* ret) {
  // TODO
  return Status::OK();
}


Status RedisStrings::Del(const Slice& key) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    return db_->Delete(default_write_options_, key);
  }
  return s;
}

Status RedisStrings::Expire(const Slice& key, int32_t ttl) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (ttl > 0) {
      parsed_strings_value.SetRelativeTimestamp(ttl);
      return db_->Put(default_write_options_, key, value);
    } else {
      return db_->Delete(default_write_options_, key);
    }
  }
  return s;
}

static int32_t GetCurrentUnixTime() {
  int64_t unix_time;
  rocksdb::Env::Default()->GetCurrentTime(&unix_time);
  return static_cast<int32_t>(unix_time);
}

Status RedisStrings::ExpireAt(const Slice& key, int32_t timestamp) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (timestamp >= GetCurrentUnixTime()) {
      parsed_strings_value.set_timestamp(timestamp);
      return db_->Put(default_write_options_, key, value);
    } else {
      return db_->Delete(default_write_options_, key);
    }
  }
  return s;
}
Status RedisStrings::Persist(const Slice& key) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    } else {
      int32_t timestamp = parsed_strings_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      } else {
        parsed_strings_value.set_timestamp(0);
        return db_->Put(default_write_options_, key, value);
      }
    }
  }
  return s;
}
Status RedisStrings::TTL(const Slice& key, int64_t* timestamp) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (parsed_strings_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else {
      *timestamp = parsed_strings_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime >= 0 ? *timestamp - curtime : -2;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

void RedisStrings::ScanDatabase() {
  // TODO
}

Status RedisStrings::IncrBy(const Slice& key, int64_t value, int64_t* ret) {
  std::string internal_val;
  RecordLockGuard g(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &internal_val);
  if (s.ok()) {
    ParsedStringsValue psv(&internal_val);
    if (psv.IsStale()) {
      // 初始化为0再incrBy

    } else {
      
    }
  } else if (s.IsNotFound()) {
    // 初始化为0再incrBy
    std::string tmp = std::to_string(value);
    StringsValue sv(tmp);
    s = db_->Put(default_write_options_, key, sv.Encode());
    if (s.ok()) {
      *ret = value;
    }
  }
  return s;
}

Status RedisStrings::MSet(const std::vector<KeyValue>& kvlist) {
  std::vector<std::string> keys;
  for(const auto & kv : kvlist) {
    keys.push_back(kv.key);
  }

  MultiScopedRecordLock l(lock_mgr_, keys);
  rocksdb::WriteBatch batch;
  for(const auto & kv : kvlist ){
    StringsValue sv(kv.value);
    batch.Put(kv.key, sv.Encode());
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisStrings::Set(const Slice& key, const Slice& value) {
  StringsValue strings_value(value);
  ScopeRecordLock l(lock_mgr_, key);
  return db_->Put(default_write_options_, key, strings_value.Encode());
}

Status RedisStrings::Get(const Slice& key, std::string* value) {
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, value);
  if (s.ok()) {
    ParsedStringsValue psv(value);
    if (psv.IsStale()) {
      value->clear();
      return Status::NotFound("Stale");
    } else {
      psv.StripSuffix();
    }
  } else if (s.IsNotFound()) {
    value->clear();
  }
  return s;
}

Status RedisStrings::Strlen(const Slice &key, uint64_t *length) {
  std::string value;
  auto s = this->Get(key, &value);
  if(s.ok()) {
    *length = value.length();
  } else {
    *length = 0;
  }
  return s;
}


}  // namespace blackwidow
