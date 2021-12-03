#include "redis_zsets.h"
#include "scope_record_lock.h"
#include "scope_snapshot.h"
#include "zsets_format.h"

namespace blackwidow {

RedisZsets::RedisZsets(BlackWidow* const bw) : Redis(bw, kZSets) {}

// Common Commands
Status RedisZsets::Open(const BlackWidowOptions& bw_options,
                        const std::string& dbpath) {
  return Status::OK();
}

Status RedisZsets::CompactRange(const Slice* begin,
                                const Slice* end,
                                const ColumnFamilyType& type) {
  if (type == kMeta || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, ZSETS_META, begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, ZSETS_MEMBER, begin, end);
    db_->CompactRange(default_compact_range_options_, ZSETS_SCORE, begin, end);
  }
  return Status::OK();
}

Status RedisZsets::GetProperty(const std::string& property, uint64_t* out) {
  return Status::OK();
}

Status RedisZsets::ScanKeyNum(KeyInfo* key_info) {
  return Status::OK();
}

Status RedisZsets::ScanKeys(const std::string& pattern,
                            std::vector<std::string>* keys) {
  return Status::OK();
}

Status RedisZsets::PKPatternMatchDel(const std::string& pattern, int32_t* ret) {
  return Status::OK();
}

// Keys Commands
Status RedisZsets::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);

    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    }

    if (parsed_meta_value.zset_size() == 0) {
      return Status::NotFound();
    }

    parsed_meta_value.InitialMetaValue();
    s = db_->Put(default_write_options_, ZSETS_META, key, meta_value);
    return s;
  }
}

Status RedisZsets::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.zset_size() == 0) {
      return Status::NotFound();
    } else {
      parsed_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, ZSETS_META, key, meta_value);
    }
  }
  return s;
}

Status RedisZsets::ExpireAt(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsStale()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.zset_size() == 0) {
      return Status::NotFound();
    } else {
      int64_t unixtime_now;
      rocksdb::Env::Default()->GetCurrentTime(&unixtime_now);
      if (timestamp < unixtime_now) {
        parsed_meta_value.InitialMetaValue();
      } else {
        parsed_meta_value.set_timestamp(timestamp);
      }
      s = db_->Put(default_write_options_, ZSETS_META, key, meta_value);
    }
  }
  return s;
}

Status RedisZsets::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsExpired()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.zset_size() == 0) {
      return Status::NotFound();
    } else {
      parsed_meta_value.set_timestamp(0);
      s = db_->Put(default_write_options_, ZSETS_META, key, meta_value);
    }
  }
  return s;
}

Status RedisZsets::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsExpired()) {
      *timestamp = -2;
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.zset_size() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      int64_t zset_timestamp = parsed_meta_value.timestamp();
      int64_t unixtime_now;
      rocksdb::Env::Default()->GetCurrentTime(&unixtime_now);
      if (zset_timestamp == 0) {
        *timestamp = -1;
      } else {
        if (zset_timestamp <= unixtime_now) {
          *timestamp = -2;
        } else {
          *timestamp = zset_timestamp - unixtime_now;
        }
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

Status RedisZsets::ZAdd(const Slice& key,
                        const std::vector<ScoreMember>& members,
                        int32_t* ret) {
  if (members.size()) {
    if (ret)
      *ret = 0;
    return Status::OK();
  }

  std::unordered_map<std::string, double> unique_members;
  for (const auto& mem : members) {
    unique_members.emplace(mem.member, mem.score);
  }

  std::string meta_value;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_zset_meta_value(&meta_value);
    if (parsed_zset_meta_value.IsStale() ||
        parsed_zset_meta_value.zset_size() == 0) {

      // Reinit and update version REQUIRED.
      parsed_zset_meta_value.InitialMetaValue();
      parsed_zset_meta_value.set_zset_size(unique_members.size());

      for (const auto& pair : unique_members) {
        const auto& member = pair.first;
        const auto& score = pair.second;
        ZsetsMemberKey member_key(
          key, parsed_zset_meta_value.version(), member);
        ZsetsScoreKey score_key(
          key, parsed_zset_meta_value.version(), score, member);
        batch.Put(
          ZSETS_MEMBER, member_key.Encode(), score_key.GetScoreAsString());
        batch.Put(ZSETS_SCORE, score_key.Encode(), EMPTY_SLICE);
      }

      batch.Put(ZSETS_META, key, meta_value);
      s = db_->Write(default_write_options_, &batch);
      if (s.ok() && ret) {
        *ret = unique_members.size();
      }
    } else {
      // A exsiting zset. TODO.
    }
  } else if (s.IsNotFound()) {
    ZsetsMetaValue zset_meta_value(unique_members.size());
    zset_meta_value.UpdateVersion();
    for (const auto& pair : unique_members) {
      const auto& member = pair.first;
      const auto& score = pair.second;
      ZsetsMemberKey member_key(key, zset_meta_value.version(), member);
      ZsetsScoreKey score_key(key, zset_meta_value.version(), score, member);
      batch.Put(
        ZSETS_MEMBER, member_key.Encode(), score_key.GetScoreAsString());
      batch.Put(ZSETS_SCORE, score_key.Encode(), EMPTY_SLICE);
    }
    batch.Put(ZSETS_META, key, zset_meta_value.Encode());
    s = db_->Write(default_write_options_, &batch);
    if (s.ok() && ret) {
      *ret = unique_members.size();
    }
  }

  return s;
}


Status RedisZsets::ZCard(const Slice& key, int32_t* len) {
  std::string meta_value;
  *len = 0;
  Status s = db_->Get(default_read_options_, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsExpired()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.zset_size() == 0) {
      return Status::NotFound();
    } else {
      *len = parsed_meta_value.zset_size();
    }
  }
  return s;
}

Status RedisZsets::ZScore(const Slice& key, const Slice& member, double* score) {
  std::string meta_value;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = snapshot;

  Status s = db_->Get(read_opts, ZSETS_META, key, &meta_value);
  if(s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if(parsed_meta_value.IsExpired()) {
      return Status::NotFound("Expired");
    } else if(parsed_meta_value.zset_size() == 0) {
      return Status::NotFound();
    } else {
      std::string scorestr;
      ZsetsMemberKey member_key(key, parsed_meta_value.version(), member);
      s = db_->Get(read_opts, ZSETS_MEMBER, member_key.Encode(), &scorestr);
      if(s.ok()) {
        assert(scorestr.size() == 8);
        uint64_t x = DecodeFixed64(&scorestr[0]);
        const double* scoreptr = reinterpret_cast<const double*>(&x);
        *score = *scoreptr;
      }
    }
  }
  return s;
}

Status RedisZsets::ZRem(const Slice& key, const std::vector<std::string>& members) {
  return Status::OK();
}

Status RedisZsets::ZCount(const Slice& key,
                          double min,
                          double max,
                          int32_t* count) {
  if (min < max) {
    *count = 0;
    return Status::OK();
  }

  std::string meta_value;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = snapshot;

  Status s = db_->Get(read_opts, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsExpired()) {
      *count = 0;
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.zset_size() == 0) {
      *count = 0;
      return Status::NotFound();
    } else {
      int32_t cnt = 0;
      // <keySize><key><version><score><member>
      int32_t version = parsed_meta_value.version();
      ZsetsScoreKey min_key(key, version, min, Slice());

      // 这里不推荐前缀，如果zset很大, min-max在很后面则需要大量无效的seek
      rocksdb::Iterator* it = db_->NewIterator(read_opts);
      for (it->Seek(min_key.Encode()); it->Valid(); it->Next()) {
        ParsedZsetsScoreKey score_key(it->key());
        if (score_key.key() != key || score_key.version() != version) {
          break;
        }

        assert(min <= score_key.score());
        if (min <= score_key.score() && score_key.score() <= max) {
          cnt++;
        } else if (score_key.score() > max) {
          break;
        }
      }
      delete it;
      *count = cnt;
    }
  } else if (s.IsNotFound()) {
    *count = 0;
  }
  return s;
}

Status RedisZsets::ZRank(const Slice& key, const Slice& member, int32_t* rank) {
  std::string meta_value;
  const rocksdb::Snapshot* snapshot = nullptr;
  ScopeSnapshot ss(db_, &snapshot);
  rocksdb::ReadOptions read_opts;
  read_opts.snapshot = snapshot;

  Status s = db_->Get(read_opts, ZSETS_META, key, &meta_value);
  if (s.ok()) {
    ParsedZsetsMetaValue parsed_meta_value(&meta_value);
    if (parsed_meta_value.IsExpired()) {
      return Status::NotFound("Expired");
    } else if (parsed_meta_value.zset_size() == 0) {
      return Status::NotFound();
    } else {
      bool found = false;
      int32_t index = 0;
      int32_t version = parsed_meta_value.version();
      // <keysz><key><version> | <score><member>
      std::string prefix = ZsetsScoreKey::GetKeyAndVersionPrefix(key, version);
      rocksdb::Iterator* it = db_->NewIterator(read_opts);
      for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
           it->Next(), index++) {
        ParsedZsetsScoreKey parsed_score_key(it->key());
        if (parsed_score_key.member() == member) {
          found = true;
          break;
        }
      }
      delete it;
      if (found) {
        *rank = index;
        return Status::OK();
      } else {
        return Status::NotFound();
      }
    }
  }
  return s;
}

}  // namespace blackwidow