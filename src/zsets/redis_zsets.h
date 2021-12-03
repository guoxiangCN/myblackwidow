#pragma once

#include "redis.h"

namespace blackwidow {

#define ZSETS_META (handles_[0])
#define ZSETS_MEMBER (handles_[1])
#define ZSETS_SCORE (handles_[2])
#define EMPTY_SLICE rocksdb::Slice()

class RedisZsets : public Redis {
 public:
  RedisZsets(BlackWidow* const bw);
  ~RedisZsets() override = default;

  // Common Commands
  Status Open(const BlackWidowOptions& bw_options,
              const std::string& dbpath) override;
  Status CompactRange(const Slice* begin,
                      const Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;
  Status GetProperty(const std::string& property, uint64_t* out) override;
  Status ScanKeyNum(KeyInfo* key_info) override;
  Status ScanKeys(const std::string& pattern,
                  std::vector<std::string>* keys) override;
  Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) override;

  // Keys Commands
  Status Del(const Slice& key) override;
  Status Expire(const Slice& key, int32_t ttl) override;
  Status ExpireAt(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;
  // TODO Scan
  // TODO: PKExpireScan


  // Zset Commands
  Status ZAdd(const Slice& key, const std::vector<ScoreMember>& members, int32_t *ret);

};  // class RedisZsets


}  // namespace blackwidow