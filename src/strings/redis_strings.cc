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


Status RedisStrings::Append(const Slice& key,
                            const Slice& value,
                            int32_t* ret) {
  *ret = 0;
  std::string old_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_value(&old_value);
    if (parsed_value.IsStale()) {
      StringsValue sv(value);
      s = db_->Put(default_write_options_, key, sv.Encode());
      if (s.ok()) {
        *ret = value.size();
      }
    } else {
      // append to old_value
      int32_t timestamp = parsed_value.timestamp();
      parsed_value.StripSuffix();
      old_value.append(value.data(), value.size());
      StringsValue sv(old_value);
      sv.set_timestamp(timestamp);
      s = db_->Put(default_write_options_, key, sv.Encode());
      if (s.ok()) {
        *ret = old_value.length();
      }
    }
  } else if (s.IsNotFound()) {
    StringsValue sv(value);
    s = db_->Put(default_write_options_, key, sv.Encode());
    if (s.ok()) {
      *ret = value.size();
    }
  }
  return s;
}

static uint64_t GetBitCount(const char* data, size_t length) {
  uint64_t bitcount = 0;
  for (auto i = 0; i < length; i++) {
    unsigned char c = static_cast<unsigned char>(data[i]);
    for (auto j = 0; j < 8; j++) {
      if ((c >> j) & (unsigned char)0x1) {
        bitcount++;
      }
    }
  }
  return bitcount;
}

Status RedisStrings::BitCount(const Slice& key, uint64_t* ret) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  *ret = 0;
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    Slice user_value = parsed_strings_value.user_value();
    if (!parsed_strings_value.IsStale() && user_value.size() > 0) {
      *ret = GetBitCount(user_value.data(), user_value.size());
    }
  }
  return s;
}

Status RedisStrings::SetBit(const Slice& key,
                            uint64_t offset,
                            uint32_t newbit,
                            uint32_t* oldbit) {
  if (newbit != 0 && newbit != 1)
    return Status::InvalidArgument("bit out of range");

  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  const char mask = (0x1 << (7 - (offset % 8)));
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if (!parsed_strings_value.IsStale()) {
      int32_t timestamp = parsed_strings_value.timestamp();
      Slice user_value = parsed_strings_value.user_value();
      char* data = const_cast<char*>(user_value.data());
      size_t old_length = user_value.size();

      parsed_strings_value.StripSuffix();
      if (old_length * 8 <= offset) {
        size_t new_length = ((offset + (8 - 1)) & (~(8 - 1))) / 8;
        value.resize(new_length);
      }

      *oldbit = ((data[offset/8] & mask) != 0);
      if (newbit == 0) {
          data[offset / 8] &= (~mask);
      } else {
          data[offset / 8] |= mask;
      }

      StringsValue sv(Slice(value.data(), value.size()));
      sv.set_timestamp(timestamp);
      return db_->Put(default_write_options_, key, sv.Encode());
    }
  } else if (!s.IsNotFound()) {
    return s;
  }

  // NotFound or Stale Already.
  *oldbit = 0;
  uint64_t alloc_bytes = offset / 8 + 1;
  value.assign(alloc_bytes, 0);
  char* const data = value.data();
  if (newbit == 0) {
    data[offset / 8] &= (~mask);
  } else {
    data[offset / 8] |= mask;
  }
  StringsValue sv(value);
  return db_->Put(default_write_options_, key, sv.Encode());
}

Status RedisStrings::GetBit(const Slice& key, uint64_t offset, uint32_t* ret) {
  std::string value;
  ScopeRecordLock l(lock_mgr_, key);
  *ret = 0;
  Status s = db_->Get(default_read_options_, key, &value);
  if (s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    Slice user_value = parsed_strings_value.user_value();
    if (!parsed_strings_value.IsStale() && (user_value.size() * 8) > offset) {
      const char* data = user_value.data();
      const uint64_t pos = offset / 8;
      const uint64_t sft = offset % 8;
      *ret = (static_cast<unsigned char>(data[pos]) &
              ((unsigned char)0x1 << (7 - sft))) != 0;
    }
  }
  return s;
}

Status RedisStrings::Aux_Incr(const Slice& key, int64_t delta, int64_t* ret) {
  RecordLockGuard g(lock_mgr_, key);
  std::string old_value;
  int32_t timestamp = 0;
  int64_t old_num = 0;
  Status s;
  do {
    s = db_->Get(default_read_options_, key, &old_value);
    if (s.ok()) {
      ParsedStringsValue parsed_strings_value(&old_value);
      if (parsed_strings_value.IsStale()) {
        break;
      } else {
        timestamp = parsed_strings_value.timestamp();
        std::string old_num_str = parsed_strings_value.value().ToString();
        char* endptr = nullptr;
        old_num = std::strtoll(old_num_str.c_str(), &endptr, 10);
        if (*endptr != 0) {
          return Status::InvalidArgument(
            "Cannot incy or decy againest a non-number string");
        }
      }
    } else if (s.IsNotFound()) {
      break;
    } else {
      // Error on DB::Get
      *ret = 0;
      return s;
    }
  } while (false);

  if ((old_num > 0 && delta > 0 && (old_num + delta) < old_num) ||
      (old_num < 0 && delta < 0 && (old_num + delta) > old_num)) {
    return Status::InvalidArgument("Overflow");
  }

  int64_t new_num = old_num + delta;
  StringsValue strings_value(std::to_string(new_num));
  strings_value.set_timestamp(timestamp);
  s = db_->Put(default_write_options_, key, strings_value.Encode());
  if (s.ok()) {
    *ret = new_num;
  }
  return s;
}

Status RedisStrings::Incr(const Slice& key, int64_t* ret) {
  return this->Aux_Incr(key, 1, ret);
}

Status RedisStrings::IncrBy(const Slice& key, int64_t delta, int64_t* ret) {
  return this->Aux_Incr(key, delta, ret);
}

Status RedisStrings::Decr(const Slice& key, int64_t* ret) {
  return this->Aux_Incr(key, -1, ret);
}

Status RedisStrings::DecrBy(const Slice& key, int64_t delta, int64_t* ret) {
  return this->Aux_Incr(key, delta * (-1), ret);
}

Status RedisStrings::MSet(const std::vector<KeyValue>& kvlist) {
  std::vector<std::string> keys;
  for (const auto& kv : kvlist) {
    keys.push_back(kv.key);
  }

  MultiScopedRecordLock l(lock_mgr_, keys);
  rocksdb::WriteBatch batch;
  for (const auto& kv : kvlist) {
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

Status RedisStrings::GetSet(const Slice& key,
                            const Slice& value,
                            std::string* old) {
  ScopeRecordLock l(lock_mgr_, key);
  auto s = db_->Get(default_read_options_, key, old);
  if (s.ok()) {
    ParsedStringsValue parsed_old_value(old);
    if (parsed_old_value.IsStale()) {
      old->clear();
    } else {
      parsed_old_value.StripSuffix();
    }
  } else if (!s.IsNotFound()) {
    return s;
  }
  StringsValue sv(value);
  return db_->Put(default_write_options_, key, sv.Encode());
}

Status RedisStrings::Strlen(const Slice& key, uint64_t* length) {
  std::string value;
  auto s = this->Get(key, &value);
  if (s.ok()) {
    *length = value.length();
  } else {
    *length = 0;
  }
  return s;
}

Status RedisStrings::SetNx(const Slice& key,
                           const Slice& value,
                           int32_t* ret,
                           const int32_t ttl) {
  *ret = 0;
  std::string old_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &old_value);
  if (s.ok()) {
    ParsedStringsValue parsed_value(&old_value);
    if (parsed_value.IsStale()) {
      StringsValue sv(value);
      if (ttl > 0) {
        sv.SetRelativeTimestamp(ttl);
      }
      s = db_->Put(default_write_options_, key, sv.Encode());
      if (s.ok()) {
        *ret = 1;
      }
    }
  } else if (s.IsNotFound()) {
    StringsValue sv(value);
    if (ttl > 0) {
      sv.SetRelativeTimestamp(ttl);
    }
    s = db_->Put(default_write_options_, key, sv.Encode());
    if (s.ok()) {
      *ret = 1;
    }
  }
  return s;
}

Status RedisStrings::SetEx(const Slice& key,
                           const Slice& value,
                           const int32_t ttl) {
  if (ttl <= 0) {
    return Status::InvalidArgument("invalid expire time");
  }
  StringsValue sv(value);
  sv.SetRelativeTimestamp(ttl);
  return db_->Put(default_write_options_, key, sv.Encode());
}


}  // namespace blackwidow
