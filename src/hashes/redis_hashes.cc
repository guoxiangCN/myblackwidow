#include "redis_hashes.h"
#include "hashes_filter.h"
#include "hashes_format.h"
#include "scope_record_lock.h"

namespace blackwidow {

RedisHashes::RedisHashes(BlackWidow* const bw) : Redis(bw, kHashes) {
  // DO NOTHING
}


Status RedisHashes::Open(const BlackWidowOptions& bw_options,
                         const std::string& dbpath) {
  // TODO FIXME.
  // statistics_store_->SetCapacity(bw_options.statistics_max_size);
  // small_compaction_threshold_ = bw_options.small_compaction_threshold;
  rocksdb::Options opts(bw_options.options);
  rocksdb::Status s = rocksdb::DB::Open(opts, dbpath, &db_);
  if(s.ok()) {
    // Create data column family
    rocksdb::ColumnFamilyOptions cf_opts;
    rocksdb::ColumnFamilyHandle* cf_handle = nullptr;
    s = db_->CreateColumnFamily(cf_opts, "data_cf", &cf_handle);
    if (!s.ok()) {
      return s;
    }
    delete cf_handle;
    delete db_;
  }

  // Reopen
  rocksdb::DBOptions db_opt(bw_options.options);
  rocksdb::ColumnFamilyOptions meta_cf_opt(bw_options.options);
  meta_cf_opt.compaction_filter_factory.reset(new HashesMetaFilterFactory());

  rocksdb::ColumnFamilyOptions data_cf_opt(bw_options.options);
  data_cf_opt.compaction_filter_factory.reset(new HashesDataFilterFactory(&db_, &handles_));

  std::vector<rocksdb::ColumnFamilyOptions> cf_option_list;
  // cf_option_list.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName,cf_opt));
  // cf_option_list.push_back(rocksdb::ColumnFamilyDescriptor("data_cf", cf_opt));

  // return rocksdb::DB::Open(db_opt, dbpath, cf_option_list, handles_, &db_);
}


Status RedisHashes::CompactRange(const Slice* begin,
                                 const Slice* end,
                                 const ColumnFamilyType& type) {
  Status s;
  if (type == kMeta || type == kMetaAndData) {
    s = db_->CompactRange(
      default_compact_range_options_, HASHES_META, begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    s = db_->CompactRange(
      default_compact_range_options_, HASHES_DATA, begin, end);
  }
  return s;
}

Status RedisHashes::GetProperty(const std::string& property, uint64_t* out) {}
Status RedisHashes::ScanKeyNum(KeyInfo* key_info) {}
Status RedisHashes::ScanKeys(const std::string& pattern,
                             std::vector<std::string>* keys) {}
Status RedisHashes::PKPatternMatchDel(const std::string& pattern,
                                      int32_t* ret) {}


Status RedisHashes::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, key, &meta_value);
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

Status RedisHashes::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s =
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

Status RedisHashes::ExpireAt(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s =
    db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    } else {
      int64_t now;
      rocksdb::Env::Default()->GetCurrentTime(&now);
      if (timestamp < now) {
        // NOTE: 直接删除整个hash 但是不直接del hash的metaKey
        parsed_meta_value.InitialMetaValue();
      } else {
        parsed_meta_value.set_timestamp(timestamp);
      }
      s = db_->Put(default_write_options_, HASHES_META, key, meta_value);
    }
  }
  return s;
}

Status RedisHashes::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s =
    db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    } else {
      parsed_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, HASHES_META, key, meta_value);
    }
  }
  return s;
}

Status RedisHashes::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s =
    db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else if(parsed_meta_value.IsPermanentSurvival()) {
      *timestamp = -1;
    } else {
      int64_t ttl = parsed_meta_value.timestamp();
      int64_t now = 0;
      rocksdb::Env::Default()->GetCurrentTime(&now);
      if(ttl <= now) {
        *timestamp = -2;
      } else {
        *timestamp = (ttl - now);
      }
    }
  } else if (s.IsNotFound()){
    *timestamp = -2;
  }
  return s;
}


Status RedisHashes::HLen(const Slice& key, uint32_t* len) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  *len = 0;
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (!parsed_meta_value.IsStale()) {
      *len = parsed_meta_value.hash_size();
    }
  }
  return s;
}

Status RedisHashes::HExists(const Slice& key, const Slice& field) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s =
    db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    } else {
      std::string field_value;
      HashesDataKey data_key(key, field, parsed_meta_value.version());
      s = db_->Get(default_read_options_, HASHES_DATA, data_key.Encode(), &field_value);
    }
  }
  return s;
}


Status RedisHashes::HSet(const Slice& key, const Slice& filed, const Slice& value) {
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if(s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if(parsed_meta_value.IsStale() || parsed_meta_value.hash_size() == 0) {
      parsed_meta_value.InitialMetaValue();
      parsed_meta_value.set_hash_size(1);
      HashesDataKey data_key(key, filed, parsed_meta_value.version());
      batch.Put(HASHES_META, key, meta_value);
      batch.Put(HASHES_DATA, data_key.Encode(), value);
      s = db_->Write(default_write_options_, &batch);
    } else {
      // 需要查询一次 如果存在同filed则替换 不是同filed则hashsize+1
      std::string field_value;
      HashesDataKey data_key(key, filed, parsed_meta_value.version());
      s = db_->Get(
        default_read_options_, HASHES_META, data_key.Encode(), &field_value);
        if(s.ok()) {
          if(value.ToString()==field_value) {
            return Status::OK();
          }
          // 替换filed value即可, meta不需要更新
          s = db_->Put(
            default_write_options_, HASHES_DATA, data_key.Encode(), value);
        } else if (s.IsNotFound()) {
            // field不存在，插入filed同时meta+1
            // parsed_meta_value.sethas
        } else {
          return s;
        }
    }
  } else if (s.IsNotFound()) {
    HashesMetaValue meta(1);
    meta.UpdateVersion();
    HashesDataKey data_key(key, filed, meta.version());
    batch.Put(HASHES_META, key, meta.Encode());
    batch.Put(HASHES_DATA, data_key.Encode(), value);
    s = db_->Write(default_write_options_, &batch);
  }
  return s;
}

}  // namespace blackwidow