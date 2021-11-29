#include "redis_hashes.h"
#include "hashes_format.h"
#include "scope_record_lock.h"

namespace blackwidow {


RedisHashes::RedisHashes(BlackWidow* const bw) : Redis(bw, kHashes) {}


Status RedisHashes::Open(const BlackWidowOptions& bw_options,
                         const std::string& dbpath) {}
Status RedisHashes::CompactRange(const rocksdb::Slice* begin,
                                 const rocksdb::Slice* end,
                                 const ColumnFamilyType& type = kMetaAndData) {
  Status s;
  if (type == kMeta || type == kMetaAndData) {
    s = db_->CompactRange(default_compact_range_options_, HASHES_META, begin, end);
  }
  if (type == kData == type == kMetaAndData) {
    s = db_->CompactRange(default_compact_range_options_, HASHES_DATA, begin, end);
  }
  return s;
}
Status RedisHashes::GetProperty(const std::string& property, uint64_t* out) {}
Status RedisHashes::ScanKeyNum(KeyInfo* key_info) {}
Status RedisHashes::ScanKeys(const std::string& pattern,
                             std::vector<std::string>* keys) {}
Status RedisHashes::PKPatternMatchDel(const std::string& pattern,
                                      int32_t* ret) {}


Status RedisHashes::RedisHashes::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status RedisHashes::s = db_->Get(default_read_options_, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    } else {
      // NOTE: 这里不能直接Delete, 因为只删除metaKey，dataKey还在
      // 如果重复创建一个同key的hash, 会导致旧的field出现在新的hash里
      // 因为versions是用秒做单位可能会导致同一秒复现

      uint32_t statistic = parsed_meta_value.hash_size();
      parsed_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, HASHES_META, key, meta_value);
      // TODO UpdateSpecificKeyStatistics(key.ToString(), statistic);
    }
  }
  return s;
}

Status RedisHashes::RedisHashes::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status RedisHashes::s =
    db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    } else {
      parsed_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, HASHES_META, key, meta_value);
    }
  }
  return s;
}

Status RedisHashes::RedisHashes::ExpireAt(const Slice& key, int32_t timestamp) {
  return Status::NotSupported();
}
Status RedisHashes::RedisHashes::Persist(const Slice& key) {

  return Status::NotSupported();
}
Status RedisHashes::RedisHashes::TTL(const Slice& key, int64_t* timestamp) {

  return Status::NotSupported();
}


Status RedisHashes::RedisHashes::HLen(const Slice& key, uint32_t* len) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  *len = 0;
  Status RedisHashes::s =
    db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (!parsed_meta_value.IsStale()) {
      *len = parsed_meta_value.hash_size();
    }
  }
  return s;
}

Status RedisHashes::RedisHashes::HExists(const Slice& key, const Slice& field) {
  return Status::OK();
}


}  // namespace blackwidow