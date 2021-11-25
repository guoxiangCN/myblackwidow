#include "redis_lists.h"
#include "lists_comparator.h"
#include "lists_filter.h"
#include "scope_record_lock.h"

#include <vector>
#include <string>

namespace blackwidow {

const rocksdb::Comparator* ListsDataKeyComparator() {
  static ListDataKeyComparatorImpl c;
  return &c;
}

RedisLists::RedisLists(BlackWidow* const bw) : Redis(bw, kLists) {}

Status RedisLists::Open(const BlackWidowOptions& bw_options,
                        const std::string& dbpath) {
  rocksdb::Options opts = bw_options.options;
  Status status = rocksdb::DB::Open(opts, dbpath, &db_);
  if (status.ok()) {
    rocksdb::ColumnFamilyHandle* cf;
    rocksdb::ColumnFamilyOptions cfo;
    cfo.comparator = ListsDataKeyComparator();
    status = db_->CreateColumnFamily(cfo, "data_cf", &cf);
    if (!status.ok()) {
      return status;
    }

    // Close DB
    delete cf;
    delete db_;
  }

  // ReOpen with column families
  rocksdb::DBOptions db_opts(bw_options.options);
  rocksdb::ColumnFamilyOptions meta_cf_opts(bw_options.options);
  rocksdb::ColumnFamilyOptions data_cf_opts(bw_options.options);

  // Create a base opts for easy to be copied by meta and data.
  rocksdb::BlockBasedTableOptions base_block_opts(bw_options.table_options);
  base_block_opts.filter_policy =
    std::shared_ptr<const rocksdb::FilterPolicy>(rocksdb::NewBloomFilterPolicy(10, true));

  rocksdb::BlockBasedTableOptions meta_block_opts(base_block_opts);
  rocksdb::BlockBasedTableOptions data_block_opts(base_block_opts);

  if (!bw_options.share_block_cache && bw_options.block_cache_size > 0) {
    meta_block_opts.block_cache = std::make_shared<rocksdb::Cache>(
      rocksdb::NewLRUCache(bw_options.block_cache_size));
    data_block_opts.block_cache = std::make_shared<rocksdb::Cache>(
      rocksdb::NewLRUCache(bw_options.block_cache_size));
  }

  /* Setup Meta column family */
  meta_cf_opts.comparator = rocksdb::BytewiseComparator();
  meta_cf_opts.compaction_filter_factory =
    std::make_shared<ListsMetaFilterFactory>();
  meta_cf_opts.table_factory = std::make_shared<rocksdb::TableFactory>(
    rocksdb::NewBlockBasedTableFactory(meta_block_opts));

  /* Setup Data column family */
  data_cf_opts.comparator = ListsDataKeyComparator();
  data_cf_opts.compaction_filter_factory =
    std::make_shared<ListsDataFilterFactory>();
  data_cf_opts.table_factory = std::make_shared<rocksdb::TableFactory>(
    rocksdb::NewBlockBasedTableFactory(meta_block_opts));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
    rocksdb::kDefaultColumnFamilyName, meta_cf_opts));
  column_families.push_back(rocksdb::ColumnFamilyDescriptor("data_cf"),
                            data_cf_opts));

  return rocksdb::DB::Open(db_opts, dbpath, column_families, &handles_, &db_);
}

Status RedisLists::CompactRange(const rocksdb::Slice* begin,
                                const rocksdb::Slice* end,
                                const ColumnFamilyType& type) {
  if (type == kMeta || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[0], begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[1], begin, end);
  }
  return Status::OK();
}

Status RedisLists::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(LISTS_META_CF_HANDLE, property, &value);
  *out = std::strtoull(value.c_str(), nullptr, 10);
  db_->GetProperty(LISTS_DATA_CF_HANDLE, property, &value);
  *out += std::strtoull(value.c_str(), nullptr, 10);
  return Status::OK();
}

Status RedisLists::ScanKeyNum(KeyInfo* key_info) {
  // TODO
  return Status::OK();
}

Status RedisLists::ScanKeys(const std::string& pattern,
                            std::vector<std::string>* keys) {
  // TODO
  return Status::OK();
}

Status RedisLists::PKPatternMatchDel(const std::string& pattern, int32_t* ret) {
  // TODO
  return Status::OK();
}



Status RedisLists::LPushX(const Slice& key, const Slice& value, uint64_t *len) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  *len = 0;
  Status s = db_->Get(default_read_options_, LISTS_META_CF_HANDLE, key, &meta_value);
  if(s.ok()) {
    // TODO
  }
  return s;
}

Status RedisLists::RPushX(const Slice& key, const Slice& value, uint64_t *len){
  return Status::OK();
}

}  // namespace blackwidow