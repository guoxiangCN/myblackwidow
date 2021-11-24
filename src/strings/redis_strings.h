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

  Status GetProperty(const std::string& property,
                             uint64_t* out) override;

  // Keys Commands Define in ::Redis

  // Self String Commands

  // Special Iterate all data
  void ScanDatabase();
};

}  // namespace blackwidow