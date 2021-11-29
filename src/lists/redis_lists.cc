#include "redis_lists.h"
#include "lists_comparator.h"
#include "lists_data_format.h"
#include "lists_filter.h"
#include "lists_meta_format.h"
#include "scope_record_lock.h"

#include <string>
#include <vector>

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
  base_block_opts.filter_policy = std::shared_ptr<const rocksdb::FilterPolicy>(
    rocksdb::NewBloomFilterPolicy(10, true));

  rocksdb::BlockBasedTableOptions meta_block_opts(base_block_opts);
  rocksdb::BlockBasedTableOptions data_block_opts(base_block_opts);

  if (!bw_options.share_block_cache && bw_options.block_cache_size > 0) {
    meta_block_opts.block_cache = std::shared_ptr<rocksdb::Cache>(
      rocksdb::NewLRUCache(bw_options.block_cache_size));
    data_block_opts.block_cache = std::shared_ptr<rocksdb::Cache>(
      rocksdb::NewLRUCache(bw_options.block_cache_size));
  }

  /* Setup Meta column family */
  meta_cf_opts.comparator = rocksdb::BytewiseComparator();
  meta_cf_opts.compaction_filter_factory =
    std::make_shared<ListsMetaFilterFactory>();
  meta_cf_opts.table_factory = std::shared_ptr<rocksdb::TableFactory>(
    rocksdb::NewBlockBasedTableFactory(meta_block_opts));

  /* Setup Data column family */
  data_cf_opts.comparator = ListsDataKeyComparator();
  data_cf_opts.compaction_filter_factory =
    std::make_shared<ListsDataFilterFactory>();
  data_cf_opts.table_factory = std::shared_ptr<rocksdb::TableFactory>(
    rocksdb::NewBlockBasedTableFactory(meta_block_opts));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
    rocksdb::kDefaultColumnFamilyName, meta_cf_opts));
  column_families.push_back(
    rocksdb::ColumnFamilyDescriptor("data_cf", data_cf_opts));
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


Status RedisLists::Del(const Slice& key) {
  // TODO
  return Status::OK();
}
Status RedisLists::Expire(const Slice& key, int32_t ttl) {
  return Status::OK();
}
Status RedisLists::ExpireAt(const Slice& key, int32_t timestamp) {
  return Status::OK();
}
Status RedisLists::Persist(const Slice& key) {
  return Status::OK();
}
Status RedisLists::TTL(const Slice& key, int64_t* timestamp) {
  return Status::OK();
}

Status RedisLists::LLen(const Slice& key, uint64_t* len) {
  *len = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, LISTS_META_CF_HANDLE, key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    }
    if (parsed_meta_value.count() == 0) {
      return Status::NotFound();
    }
    *len = parsed_meta_value.count();
  }
  return s;
}

Status RedisLists::LPushX(const Slice& key, const Slice& value, uint64_t* len) {
  *len = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s =
    db_->Get(default_read_options_, LISTS_META_CF_HANDLE, key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t index = parsed_meta_value.left_index();
      int32_t version = parsed_meta_value.version();
      parsed_meta_value.ModifyLeftIndex(1);
      parsed_meta_value.ModifyCount(1);
      ListsDataKey data_key(key, version, index);
      batch.Put(LISTS_META_CF_HANDLE, key, meta_value);
      batch.Put(LISTS_DATA_CF_HANDLE, data_key.Encode(), value);
      *len = parsed_meta_value.count();
      return db_->Write(default_write_options_, &batch);
    }
  }
  return s;
}

// Right push iff list exists
Status RedisLists::RPushX(const Slice& key, const Slice& value, uint64_t* len) {
  *len = 0;
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s =
    db_->Get(default_read_options_, LISTS_META_CF_HANDLE, key, &meta_value);
  if (s.ok()) {
    ParsedListsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint64_t index = parsed_meta_value.right_index();
      int32_t version = parsed_meta_value.version();
      parsed_meta_value.ModifyRightIndex(1);
      parsed_meta_value.IncrCount(1);
      ListsDataKey data_key(key, version, index);
      batch.Put(LISTS_META_CF_HANDLE, key, meta_value);
      batch.Put(LISTS_DATA_CF_HANDLE, data_key.Encode(), value);
      return db_->Write(default_write_options_, &batch);
    }
  }
  return s;
}

Status RedisLists::LPush(const Slice& key,
                         const std::vector<std::string>& values,
                         uint64_t* ret) {
  if(values.size()==0) {
    *ret = 0;
    return Status::OK();
  }

  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, LISTS_META_CF_HANDLE, key, &meta_value);
  if(s.ok()) {
      ParsedListsMetaValue parsed_meta_value(&meta_value);
      if(parsed_meta_value.IsStale() || parsed_meta_value.count()==0) {
        // 数据还在但是过期了或者count==0
        // 由于list的del操作不能通过遍历所有数据添加墓碑标记
        // 或者过期的list还没被compaction清理掉
        // 直接插入新数据会插入meta? 会使之前的list数据重新出现??
        // 需要使用version?

      } else {
        // list存在
      }
  } else if(s.IsNotFound()) {
        // list压根不存在
        char len[8] = {0};
        EncodeFixed64(len, values.size());
        ListsMetaValue raw_meta_val(Slice(len, sizeof(len)));
        for(const std::string& member : values) {
          int32_t version = raw_meta_val.UpdateVersion();
          uint64_t index = raw_meta_val.left_index();
          raw_meta_val.ModifyLeftIndex(1);

          ListsDataKey data_key(key, version, index);
          batch.Put(LISTS_DATA_CF_HANDLE, data_key.Encode(), member);
        }
        batch.Put(LISTS_META_CF_HANDLE, key, raw_meta_val.Encode());
        *ret = values.size();
        return db_->Write(default_write_options_, &batch);
  } else {
    return s;
  }
}

Status RedisLists::LPop(const Slice& key, std::string* element) {

  return Status::NotSupported();
}


}  // namespace blackwidow