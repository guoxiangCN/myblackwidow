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
Status Del(const Slice& key) {
  return Status::OK();
}

Status Expire(const Slice& key, int32_t ttl) {
  return Status::OK();
}

Status ExpireAt(const Slice& key, int32_t timestamp) {
  return Status::OK();
}

Status Persist(const Slice& key) {
  return Status::OK();
}

Status TTL(const Slice& key, int64_t* timestamp) {
  return Status::OK();
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
    if (parsed_zset_meta_value.IsStale() || parsed_zset_meta_value.zset_szie() == 0) {
      
      // Reinit and update version REQUIRED.
      parsed_zset_meta_value.InitialMetaValue();
      parsed_zset_meta_value.set_zset_size(unique_members.size());

      for (const auto& pair : unique_members) {
        const auto& member = pair.first;
        const auto& score = pair.second;
        ZsetsMemberKey member_key(key, member, zset_meta_value.version());
        ZsetsScoreKey score_key(key, member, score, zset_meta_value.version());
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
      ZsetsMemberKey member_key(key, member, zset_meta_value.version());
      ZsetsScoreKey score_key(key, member, score, zset_meta_value.version());
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


}  // namespace blackwidow