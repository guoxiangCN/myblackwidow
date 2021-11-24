#include "redis_strings.h"
#include "strings_filter.h"
#include "scope_snapshot.h"

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

Status RedisStrings::GetProperty(const std::string& property,
                             uint64_t* out) {
  std::string value;
  auto s= db_->GetProperty(db_->DefaultColumnFamily(),property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

}  // namespace blackwidow
