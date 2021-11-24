#include "redis_strings.h"
#include "scope_snapshot.h"
#include "scope_record_lock.h"
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
  ScopeRecordLock(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if(s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if(parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    return db_->Delete(default_write_options_, key);
  }
  return s;
}

Status RedisStrings::Expire(const Slice& key, int32_t ttl) {
  std::string value;
  ScopeRecordLock(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &value);
  if(s.ok()) {
    ParsedStringsValue parsed_strings_value(&value);
    if(parsed_strings_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if(ttl > 0) {
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
   ScopeRecordLock(lock_mgr_, key);
   Status s = db_->Get(default_read_options_, key, &value);
   if(s.ok()) {
     ParsedStringsValue parsed_strings_value(&value);
     if(parsed_strings_value.IsStale()) {
       return Status::NotFound("Stale");
     }
     if(timestamp >= GetCurrentUnixTime()) {
         parsed_strings_value.set_timestamp(timestamp);
        return db_->Put(default_write_options_, key, value);
     } else {
       return db_->Delete(default_write_options_, key);
     }
   }
   return s;
 }
  Status RedisStrings::Persist(const Slice& key) {}
  Status RedisStrings::TTL(const Slice& key, int64_t* timestamp){

  }


}  // namespace blackwidow
