#ifndef __HASHES_FILTER_H__
#define __HASHES_FILTER_H__

#include "debug.h"
#include "hashes_format.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"

namespace blackwidow {

class HashesMetaFilter : public rocksdb::CompactionFilter {
 public:
  const char* Name() const override {
    return "blackwidow.HashesMetaFilter";
  }

  bool Filter(int level,
              const Slice& key,
              const Slice& existing_value,
              std::string* new_value,
              bool* value_changed) const override {
    int64_t unix_time_now;
    rocksdb::Env::GetCurrentTime(&unix_time_now);

    bool should_filter = false;
    std::string filter_reason;
    ParsedHashesMetaValue parsed_meta_value(existing_value);
    int32_t version = parsed_meta_value.version();
    uint32_t hash_size = parsed_meta_value.hash_size();
    int64_t timestamp = static_cast<int64_t>(parsed_meta_value.timestamp());

    // 这里除了判断整个hash是否过期外，还需要额外判断version是否小于当前的时间戳(秒)
    // 如果过期了，但是version不小于当前时间戳，删除当前metaKey是不安全的！！
    // 因为这里只删除hash的metaKey， 但是hash的filed是没有被删除的. 如果这里直接删除
    // metaKey, 同一秒用户又针对同1个key设置了一个新的hash, 新hash的version会和这里的
    // version相同, 则会导致旧hash的data数据归属到新的hash里~
    if (timestamp != 0 && timestamp < unix_time_now && version < unix_time_now) {
      should_filter = true;
      filter_reason = "Expired";
    }

    // 这里仍然也要额外判断version才能绝对是否定夺。
    // 如果hash_size==0完全是由于hdel导致的，这里不用判定version, 因为这种情况所有的field的
    // 都是打了删除的墓碑标记, 不会导致旧hash的数据出现在新hash的数据里。
    // 但是还有一种情况hash_size==0,即del命令导致的，这种情况, field还是存在的, 因此必须判断
    // version才能安全删除。但是并发插入同key的hash情况比较少, 因此这里这里绝大部分情况是可以
    // 正常删除的。
    if (hash_size == 0 && version < unix_time_now) {
      should_filter = true;
      filter_reason = "NoElements";
    }

    Trace("[HashesMetaFilter] level-%d, key: %s, value:%s, timestamp:%ld, currentTime: %ld, shouldFilter:%d\n",
        level, key.ToString(), existing_value.ToString(), timestamp, unix_time_now, should_filter);

    return should_filter;
  }
};

class HashesMetaFilterFactory : public rocksdb::CompactionFilterFactory {};

}  // namespace blackwidow


#endif