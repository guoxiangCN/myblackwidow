#include "redis.h"
#include <algorithm>
#include <string>
#include <vector>

namespace blackwidow {

class RedisStrings : public Redis {
 public:
  RedisStrings(BlackWidow* const bw);
  ~RedisStrings() = default;

  // Command Commands Define in ::Redis
  Status Open(const BlackWidowOptions& bw_options,
              const std::string& dbpath) override;

  Status CompactRange(const rocksdb::Slice* begin,
                      const rocksdb::Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;

  Status GetProperty(const std::string& property, uint64_t* out) override;

  Status ScanKeyNum(KeyInfo* key_info) override;
  Status ScanKeys(const std::string& pattern,
                  std::vector<std::string>* keys) override;
  Status PKPatternMatchDel(const std::string& pattern,
                                   int32_t* ret) override;

  // Keys Commands Define in ::Redis
  Status Del(const Slice& key) override;
  Status Expire(const Slice& key, int32_t ttl) override;
  Status ExpireAt(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;


  // Self String Commands

  // Special Iterate all data
  void ScanDatabase();
};

}  // namespace blackwidow