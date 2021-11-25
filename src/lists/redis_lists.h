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


  // Self List Commands
  Status LLen(const Slice& key, uint64_t* len);
  Status LPushX(const Slice& key, const Slice& value, uint64_t* len);
  Status RPushX(const Slice &key, const Slice &value, uint64_t *len);
};

}  // namespace blackwidow