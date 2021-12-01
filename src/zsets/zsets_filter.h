#pragma once

#include "debug.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "zsets_format.h"

namespace blackwidow {

class ZsetsMetaFilter : public rocksdb::CompactionFilter {
 public:
  const char* Name() const override {
    return "blackwidow.ZsetsMetaFilter";
  }

  bool Filter(int level,
              const Slice& key,
              const Slice& existing_value,
              std::string* new_value,
              bool* value_changed) const override {
    int64_t unix_time_now;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time_now);

    bool shoudFilter = false;
    std::string filterReason = "None";

    ParsedZsetsMetaValue parsed_meta_value(existing_value);
    int32_t version = parsed_meta_value.version();
    int32_t timestamp = parsed_meta_value.timestamp();
    uint32_t zset_size = parsed_meta_value.zset_szie();

    // Note: we need also the check the version.
    if (timestamp != 0 && timestamp < unix_time_now &&
        version < unix_time_now) {
      shoudFilter = true;
      filterReason = "Expired";
    }

    // Not set expire time or setted but not expired
    // but the elements was empty by hdel or del directly.
    if (zset_size == 0 && version < unix_time_now) {
      shoudFilter = true;
      filterReason = "NoElements";
    }

    Trace(
      "[ZsetMetaFilter]-level:%d, key:%s, zset_size:%d, version:%d, "
      "timestamp:%d, now_timestamp:%d, shouldFilter:%d, filterReason:%s\n",
      level,
      key.ToString().c_str(),
      zset_size,
      version,
      timestamp,
      unix_time_now,
      shoudFilter,
      filterReason);

    return shoudFilter;
  }
};


class ZsetsMetaFilterFactory : public rocksdb::CompactionFilterFactory {
public:
    
};

}  // namespace blackwidow