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

    // 如果设置了过期时间且已经过期.
    if(parsed_value.timestamp() != 0 && parsed_value.timestamp() < unix_time) {
      should_filter = true;
    }

    // NOTE: 能否传入db_进来查询最新的key对应的数据, 如果key被删除或者key的值和自己不相等， 说明
    // 自己的key是旧版本的key, 则这里可以直接删除？
    // 前提是不影响lruCache. 可以read_options.fill_cache=false
    // 不然如果一个经常更新的key,可能存在N个版本, 都没设置过期时间， 那么低层的旧数据compaction是
    // 删不掉的， 一直下沉到更下层。
    // example:
    // set user 1 当前位于level4
    // set user 2 当前位于level3
    // set user 3 当前位于level2
    // set uesr 4 当前位于level0
    // set user 5 当前位于memtable
    // ... 更新版本...

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