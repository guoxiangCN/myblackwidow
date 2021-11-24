#pragma once

#include "redis.h"

namespace blackwidow {

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
};

}  // namespace blackwidow