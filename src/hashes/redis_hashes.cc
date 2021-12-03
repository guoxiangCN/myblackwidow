#include "redis_hashes.h"
#include "hashes_filter.h"
#include "hashes_format.h"
#include "scope_iterator.h"
#include "scope_record_lock.h"
#include "scope_snapshot.h"

#include <unordered_set>
#include <vector>

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
  if (s.ok()) {
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

  rocksdb::BlockBasedTableOptions base_table_opts(bw_options.table_options);
  base_table_opts.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));

  rocksdb::BlockBasedTableOptions meta_cf_table_opts(base_table_opts);
  rocksdb::BlockBasedTableOptions data_cf_table_opts(base_table_opts);
  if (bw_options.share_block_cache == false &&
      bw_options.block_cache_size > 0) {
    meta_cf_table_opts.block_cache =
      rocksdb::NewLRUCache(bw_options.block_cache_size);
    data_cf_table_opts.block_cache =
      rocksdb::NewLRUCache(bw_options.block_cache_size);
  }

  rocksdb::ColumnFamilyOptions meta_cf_opt(bw_options.options);
  meta_cf_opt.compaction_filter_factory.reset(new HashesMetaFilterFactory());
  meta_cf_opt.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(meta_cf_table_opts));

  rocksdb::ColumnFamilyOptions data_cf_opt(bw_options.options);
  data_cf_opt.compaction_filter_factory.reset(
    new HashesDataFilterFactory(&db_, &handles_));
  data_cf_opt.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(data_cf_table_opts));

  std::vector<rocksdb::ColumnFamilyDescriptor> cfds;
  // metaCf must be the first
  cfds.push_back(rocksdb::ColumnFamilyDescriptor(
    rocksdb::kDefaultColumnFamilyName, meta_cf_opt));
  // dataCf must be the second
  cfds.push_back(rocksdb::ColumnFamilyDescriptor("data_cf", data_cf_opt));
  return rocksdb::DB::Open(db_opt, dbpath, cfds, &handles_, &db_);
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
      // 如果同一秒重复创建一个同key的hash, 会导致旧的field出现在新的hash里
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
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
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
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
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
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
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
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else if (parsed_meta_value.IsPermanentSurvival()) {
      *timestamp = -1;
    } else {
      int64_t ttl = parsed_meta_value.timestamp();
      int64_t now = 0;
      rocksdb::Env::Default()->GetCurrentTime(&now);
      if (ttl <= now) {
        *timestamp = -2;
      } else {
        *timestamp = (ttl - now);
      }
    }
  } else if (s.IsNotFound()) {
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
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    }
    if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    }
    *len = parsed_meta_value.hash_size();
  }
  return s;
}

Status RedisHashes::HExists(const Slice& key, const Slice& field) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    } else {
      std::string field_value;
      HashesDataKey data_key(key, field, parsed_meta_value.version());
      s = db_->Get(
        default_read_options_, HASHES_DATA, data_key.Encode(), &field_value);
    }
  }
  return s;
}


Status RedisHashes::HSet(const Slice& key,
                         const Slice& field,
                         const Slice& value,
                         int32_t* ret) {
  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale() || parsed_meta_value.hash_size() == 0) {
      parsed_meta_value.InitialMetaValue();
      parsed_meta_value.set_hash_size(1);
      HashesDataKey data_key(key, field, parsed_meta_value.version());
      batch.Put(HASHES_META, key, meta_value);
      batch.Put(HASHES_DATA, data_key.Encode(), value);
      s = db_->Write(default_write_options_, &batch);
      if (s.ok() && ret) {
        *ret = 1;
      }
    } else {
      std::string field_value;
      HashesDataKey data_key(key, field, parsed_meta_value.version());
      s = db_->Get(
        default_read_options_, HASHES_DATA, data_key.Encode(), &field_value);
      if (s.ok()) {
        // If field exists and field_value was equals to the newer, nothing
        // todo.
        if (value.ToString() == field_value) {
          if (ret) {
            *ret = 0;
          }
          return Status::OK();
        }
        // Replace the old value.
        s = db_->Put(
          default_write_options_, HASHES_DATA, data_key.Encode(), value);
        if (s.ok() && ret) {
          *ret = 0;
        }
      } else if (s.IsNotFound()) {
        // Put the filed-value pair and update the hash_size + 1
        parsed_meta_value.set_hash_size(parsed_meta_value.hash_size() + 1);
        batch.Put(HASHES_META, key, meta_value);
        batch.Put(HASHES_DATA, data_key.Encode(), value);
        s = db_->Write(default_write_options_, &batch);
        if (s.ok() && ret) {
          *ret = 1;
        }
      } else {
        // error on query field.
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    HashesMetaValue meta_value(1);
    meta_value.UpdateVersion();
    HashesDataKey data_key(key, field, meta_value.version());
    batch.Put(HASHES_META, key, meta_value.Encode());
    batch.Put(HASHES_DATA, data_key.Encode(), value);
    s = db_->Write(default_write_options_, &batch);
    if (s.ok() && ret) {
      *ret = 1;
    }
  }
  return s;
}


Status RedisHashes::HGet(const Slice& key,
                         const Slice& field,
                         std::string* value) {
  std::string meta_value;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot guard(db_, &snapshot);
  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  Status s = db_->Get(read_opts, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale() || parsed_meta_value.hash_size() == 0) {
      value->clear();
      return Status::NotFound();
    } else {
      HashesDataKey data_key(key, field, parsed_meta_value.version());
      return db_->Get(read_opts, HASHES_DATA, data_key.Encode(), value);
    }
  } else if (s.IsNotFound()) {
    value->clear();
  }
  return s;
}

Status RedisHashes::HGetAll(const Slice& key, std::vector<FieldValue>* fvs) {
  std::string meta_value;
  rocksdb::ReadOptions read_opts;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot guard(db_, &snapshot);
  read_opts.snapshot = snapshot;
  Status s = db_->Get(read_opts, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      fvs->clear();
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      fvs->clear();
      return Status::NotFound();
    } else {
      fvs->clear();
      // <keysize><key><version><field>
      HashesDataKey data_key(key, "", parsed_meta_value.version());
      const Slice prefix = data_key.Encode();
      rocksdb::Iterator* it = db_->NewIterator(read_opts, HASHES_DATA);
      for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
           it->Next()) {
        ParsedHashesDataKey parsed_data_key(it->key());
        fvs->push_back(
          {parsed_data_key.field().ToString(), it->value().ToString()});
      }
      delete it;
    }
  } else if (s.IsNotFound()) {
    fvs->clear();
  }
  return s;
}


Status RedisHashes::HVals(const Slice& key, std::vector<std::string>* vals) {
  std::string meta_value;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  Status s = db_->Get(read_opts, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      vals->clear();
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      vals->clear();
      return Status::NotFound();
    } else {
      HashesDataKey data_key(key, "", parsed_meta_value.version());
      Slice prefix = data_key.Encode();
      rocksdb::Iterator* it = db_->NewIterator(read_opts, HASHES_DATA);
      for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
           it->Next()) {
        vals->push_back(it->value().ToString());
      }
      delete it;
    }
  } else if (s.IsNotFound()) {
    vals->clear();
  }
  return s;
}

Status RedisHashes::HDel(const Slice& key,
                         const std::vector<std::string>& fields,
                         int32_t* ret) {
  if (fields.size() == 0) {
    *ret = 0;
    return Status::OK();
  }

  // remove duplicated
  std::unordered_set<std::string> filters(fields.begin(), fields.end());
  std::vector<std::string> filted_fields(filters.begin(), filters.end());

  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  rocksdb::WriteBatch batch;
  Status s = db_->Get(default_read_options_, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale() || parsed_meta_value.hash_size() == 0) {
      *ret = 0;
      return Status::OK();
    } else {
      *ret = 0;
      std::string field_value;
      for (const auto& f : filted_fields) {
        HashesDataKey data_key(key, f, parsed_meta_value.version());
        s = db_->Get(
          default_read_options_, HASHES_DATA, data_key.Encode(), &field_value);
        if (s.ok()) {
          (*ret)++;
          batch.Delete(HASHES_DATA, data_key.Encode());
        } else if (!s.IsNotFound()) {
          *ret = 0;
          return s;
        }
      }
      if (*ret > 0) {
        parsed_meta_value.set_hash_size(parsed_meta_value.hash_size() - (*ret));
        batch.Put(HASHES_META, key, meta_value);
        s = db_->Write(default_write_options_, &batch);
        if (!s.ok()) {
          *ret = 0;
        }
      } else {
        return Status::OK();
      }
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return Status::OK();
  }
  return s;
}

Status RedisHashes::HStrlen(const Slice& key,
                            const Slice& field,
                            int32_t* len) {
  std::string meta_value;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  *len = 0;
  Status s = db_->Get(read_opts, HASHES_META, key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.hash_size() == 0) {
      return Status::NotFound();
    } else {
      std::string field_value;
      HashesDataKey data_key(key, field, parsed_meta_value.version());
      s = db_->Get(read_opts, HASHES_DATA, data_key.Encode(), &field_value);
      if (s.ok()) {
        *len = field_value.size();
      }
    }
  }
  return s;
}


void RedisHashes::ScanDatabase() {
  // TODO
}


}  // namespace blackwidow
