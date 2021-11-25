#pragma once

#include "redis.h"

namespace blackwidow {

#define LISTS_META_CF_HANDLE (handles_[0])
#define LISTS_DATA_CF_HANDLE (handles_[1])

class RedisLists : public Redis {
 public:
  explicit RedisLists(BlackWidow* const bw);
  ~RedisLists() override = default;

  // Common Commands defined in Redis
  Status Open(const BlackWidowOptions& bw_options,
              const std::string& dbpath) override;
  Status CompactRange(const rocksdb::Slice* begin,
                      const rocksdb::Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;
  Status GetProperty(const std::string& property, uint64_t* out) override;
  Status ScanKeyNum(KeyInfo* key_info) override;
  Status ScanKeys(const std::string& pattern,
                  std::vector<std::string>* keys) override;
  Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) override;

  // Keys Commands defined in redis
  Status Del(const Slice& key) override;
  Status Expire(const Slice& key, int32_t ttl) override;
  Status ExpireAt(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;
  // TODO Scan
  // TODO: PKExpireScan

  // Self List Commands
  Status LLen(const Slice& key, uint64_t* len);
  Status LPushX(const Slice& key, const Slice& value, uint64_t* len);
  Status RPushX(const Slice &key, const Slice &value, uint64_t *len);

  Status LPush(const Slice& key,
               const std::vector<std::string>& values,
               uint64_t* ret);
  Status LPop(const Slice &key, std::string *element);
};

}  // namespace blackwidow