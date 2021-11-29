#pragma once

#include "debug.h"
#include "strings_format.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"

namespace blackwidow {

class StringsFilter : public rocksdb::CompactionFilter {
 public:
  StringsFilter() = default;

  const char* Name() const override {
    return "blackwidow.StringsFilter";
  }

  bool Filter(int level,
              const rocksdb::Slice& key,
              const rocksdb::Slice& existing_value,
              std::string* new_value,
              bool* value_changed) const override {

    bool should_filter = false;
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);

    ParsedStringsValue parsed_value(existing_value);
    if(parsed_value.timestamp() != 0 && parsed_value.timestamp() < unix_time) {
      should_filter = true;
    }

    Trace(
      "[StringsCompactionFilter] Level-%d, UserKey: %s, UserValue: %s, Timestamp: %d, CurrentTime: %ld, "
      "ShouldFilter: %d", 
      level,
      key.ToString().c_str(), 
      // parsed_value.value().ToString().c_str(), 
      "*****",
      parsed_value.timestamp(), 
      unix_time, 
      should_filter);

    return should_filter;
  }
};

class StringsFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  StringsFilterFactory() = default;

  const char* Name() const override {
    return "blackwidow.StringsFilterFactory";
  }

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new StringsFilter());
  }
};


}  // namespace blackwidow