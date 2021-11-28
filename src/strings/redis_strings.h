#include "redis.h"
#include <algorithm>
#include <string>
#include <vector>

namespace blackwidow {

class RedisStrings : public Redis {
 public:
  RedisStrings(BlackWidow* const bw);
  ~RedisStrings() override = default;

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
  Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) override;

  // Keys Commands Define in ::Redis
  Status Del(const Slice& key) override;
  Status Expire(const Slice& key, int32_t ttl) override;
  Status ExpireAt(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;

  // Special Iterate all data
  void ScanDatabase();

  // String Commands
  Status Append(const Slice& key, const Slice& value, int32_t* ret);
  Status BitCount(const Slice& key, uint64_t* ret);
  Status SetBit(const Slice& key, uint64_t offset, uint32_t newbit, uint32_t *oldbit);
  Status GetBit(const Slice& key, uint64_t offset, uint32_t* ret);
  Status Incr(const Slice& key, int64_t* ret);
  Status IncrBy(const Slice& key, int64_t delta, int64_t* ret);
  Status IncrByFloat(const Slice &key, )
  Status Decr(const Slice& key, int64_t* ret);
  Status DecrBy(const Slice& key, int64_t delta, int64_t* ret);
  Status MSet(const std::vector<KeyValue>& kvlist);
  Status Set(const Slice& key, const Slice& value);
  Status Strlen(const Slice& key, uint64_t* strlen);
  Status SetNx(const Slice& key,
               const Slice& value,
               int32_t* ret,
               const int32_t ttl = 0);
  Status SetEx(const Slice& key,
               const Slice& value,
               const int32_t ttl);
  Status Get(const Slice& key, std::string* value);
  Status GetSet(const Slice& key, const Slice& value, std::string* old);

 private:
  // AUX Utils
  Status Aux_Incr(const Slice& key, int64_t delta, int64_t* ret);
};

}  // namespace blackwidow