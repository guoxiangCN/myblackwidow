#pragma once
#include "../redis.h"

namespace blackwidow {

#define HASHES_META (handles_[0])
#define HASHES_DATA (handles_[0])

class RedisHashes : public Redis {
 public:
  RedisHashes(BlackWidow* const bw);
  ~RedisHashes() override = default;

  // Common Commands
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

  // Keys Commands
  Status Del(const Slice& key) override;
  Status Expire(const Slice& key, int32_t ttl) override;
  Status ExpireAt(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;
  // TODO Scan
  // TODO: PKExpireScan


  // Hash Commands
  Status HLen(const Slice& key, uint32_t* len);
  Status HExists(const Slice& key, const Slice& field);
};


}  // namespace blackwidow