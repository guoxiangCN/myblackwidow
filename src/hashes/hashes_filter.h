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
    rocksdb::Env::Default()->GetCurrentTime(&unix_time_now);

    bool should_filter = false;
    std::string filter_reason = "None";
    ParsedHashesMetaValue parsed_meta_value(existing_value);
    int32_t version = parsed_meta_value.version();
    uint32_t hash_size = parsed_meta_value.hash_size();
    int64_t timestamp = static_cast<int64_t>(parsed_meta_value.timestamp());

    // 这里除了判断整个hash是否过期外，还需要额外判断version是否小于当前的时间戳(秒)
    // 如果过期了，但是version不小于当前时间戳，删除当前metaKey是不安全的！！
    // 因为这里只删除hash的metaKey， 但是hash的filed是没有被删除的.
    // 如果这里直接删除 metaKey, 同一秒用户又针对同1个key设置了一个新的hash,
    // 新hash的version会和这里的 version相同,
    // 则会导致旧hash的data数据归属到新的hash里~
    if (timestamp != 0 && timestamp < unix_time_now &&
        version < unix_time_now) {
      should_filter = true;
      filter_reason = "Expired";
    }

    // 这里仍然也要额外判断version才能绝对是否定夺。
    // 如果hash_size==0完全是由于hdel导致的，这里不用判定version,
    // 因为这种情况所有的field的 都是打了删除的墓碑标记,
    // 不会导致旧hash的数据出现在新hash的数据里。
    // 但是还有一种情况hash_size==0,即del命令导致的，这种情况, field还是存在的,
    // 因此必须判断 version才能安全删除。但是并发插入同key的hash情况比较少,
    // 因此这里这里绝大部分情况是可以 正常删除的。
    if (hash_size == 0 && version < unix_time_now) {
      should_filter = true;
      filter_reason = "NoElements";
    }

    Trace(
      "[HashesMetaFilter] level-%d, key: %s, value:%s, timestamp:%ld, "
      "currentTime: %ld, shouldFilter:%d, filterReason:%s\n",
      level,
      key.ToString().c_str(),
      existing_value.ToString().c_str(),
      timestamp,
      unix_time_now,
      should_filter,
      filter_reason.c_str());

    return should_filter;
  }
};

class HashesMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<HashesMetaFilter>(new HashesMetaFilter());
  }

  const char* Name() const override {
    return "blackwidow.HashesMetaFilterFactory";
  }
};

class HashesDataFilter : public rocksdb::CompactionFilter {
 public:
  HashesDataFilter(rocksdb::DB* dbptr, std::vector<rocksdb::ColumnFamilyHandle*>* handles)
    : db_(dbptr), handles_(handles),
      cur_key_(""),
      meta_not_found_(false),
      cur_meta_version_(0),
      cur_meta_timestamp_(0) {
    // read_opts_.fill_cache = false;
  }

  const char* Name() const override {
    return "blackwidow.HashesDataFilter";
  }

  bool Filter(int level,
              const Slice& key,
              const Slice& existing_value,
              std::string* new_value,
              bool* value_changed) const {

    bool should_filter = false;
    std::string filter_reason = "None";
    ParsedHashesDataKey parsed_data_key(key);

    if(parsed_data_key.user_key().ToString() != cur_key_) {
      cur_key_ = parsed_data_key.user_key().ToString();
      std::string meta_value;
      if(handles_->size() == 0) {
          // destroyed when close the database, Reserve the kv
        return false;
      }

      Status s = db_->Get(read_opts_, (*handles_)[0], cur_key_, &meta_value);
      if(s.ok()) {
        meta_not_found_ = false;
        ParsedHashesMetaValue parsed_meta_value(&meta_value);
        cur_meta_version_ = parsed_meta_value.version();
        cur_meta_timestamp_ = parsed_meta_value.timestamp();
      } else if (s.IsNotFound()) {
        meta_not_found_ = true;
      } else {
        cur_key_ = "";
        Trace("Get MetaKey failed, reserve.");
        return false;
      }
    }

    if(meta_not_found_) {
      // meta deleted by compactionfilter too.
      should_filter = true;
      filter_reason = "MetaNotFound";
    } else {
      int64_t unix_time_now;
      rocksdb::Env::Default()->GetCurrentTime(&unix_time_now);
      if(cur_meta_timestamp_ != 0 &&
        cur_meta_timestamp_ < static_cast<int32_t>(unix_time_now)) {
        should_filter = true;
        filter_reason = "MetaExpired";
      }

      if(cur_meta_version_ > parsed_data_key.version()) {
        should_filter = true;
        filter_reason = "DeprecatedVersion";
      } else {
        assert(cur_meta_version_ == parsed_data_key.version());
      }
    }

    Trace(
      "[HashesDataFilter]-level-%d, key:%s, version:%d, field:%s, "
      "filedValue:%s, "
      "metaVersion:%d, shouldFilter:%d, filterReason:%s\n",
      level,
      cur_key_.c_str(),
      parsed_data_key.version(),
      parsed_data_key.field().ToString().c_str(),
      existing_value.ToString().c_str(),
      cur_meta_version_,
      should_filter,
      filter_reason.c_str());

    return should_filter;
  }

 private:
  rocksdb::DB* db_;
  std::vector<rocksdb::ColumnFamilyHandle*>* handles_;
  rocksdb::ReadOptions read_opts_;
  // cached meta infos
  mutable std::string cur_key_;
  mutable bool meta_not_found_;
  mutable int32_t cur_meta_version_;
  mutable int32_t cur_meta_timestamp_;
};

class HashesDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  HashesDataFilterFactory(rocksdb::DB** db_ptr,
                        std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr) {
  }

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
           new HashesDataFilter(*db_ptr_, cf_handles_ptr_));
  }
  const char* Name() const override {
    return "blackwidow.HashesDataFilterFactory";
  }

 private:
  rocksdb::DB** db_ptr_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
};

}  // namespace blackwidow


#endif