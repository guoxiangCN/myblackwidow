#pragma once

#include "redis.h"

namespace blackwidow {

class RedisZsets : public Redis {
public:
     // Common Commands
  virtual Status Open(const BlackWidowOptions& bw_options,
                      const std::string& dbpath) = 0;
  virtual Status CompactRange(const Slice* begin,
                              const Slice* end,
                              const ColumnFamilyType& type = kMetaAndData) = 0;
  virtual Status GetProperty(const std::string& property, uint64_t* out) = 0;
  virtual Status ScanKeyNum(KeyInfo* key_info) = 0;
  virtual Status ScanKeys(const std::string& pattern,
                          std::vector<std::string>* keys) = 0;
  virtual Status PKPatternMatchDel(const std::string& pattern,
                                   int32_t* ret) = 0;
  // Keys Commands
  virtual Status Del(const Slice& key) = 0;
  virtual Status Expire(const Slice& key, int32_t ttl) = 0;
  virtual Status ExpireAt(const Slice& key, int32_t timestamp) = 0;
  virtual Status Persist(const Slice& key) = 0;
  virtual Status TTL(const Slice& key, int64_t* timestamp) = 0;
  // TODO Scan
  // TODO: PKExpireScan


    // Zset Commands
  Status ZAdd(const Slice &key, const std::vector<ScoreMember> &members);

}; // class RedisZsets


}   // namespace blackwidow