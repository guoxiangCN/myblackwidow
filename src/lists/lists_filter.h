#pragma once

#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"

namespace blackwidow {

class ListsMetaFilter : public rocksdb::CompactionFilter {

 public:
  const char* Name() const override {
    return "blackwidow.ListsMetaFilter";
  }

  bool Filter(int level,
              const rocksdb::Slice& key,
              const rocksdb::Slice& existing_value,
              std::string* new_value,
              bool* value_changed) const override {
    // TODO
    return false;
  }
};

class ListsMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  const char* Name() const override {
    return "blackwidow.ListsMetaFilterFactory";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
    const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(new ListsMetaFilter()));
  }
};



class ListsDataFilter : public rocksdb::CompactionFilter {

 public:
  const char* Name() const override {
    return "blackwidow.ListsDataFilter";
  }

  bool Filter(int level,
              const rocksdb::Slice& key,
              const rocksdb::Slice& existing_value,
              std::string* new_value,
              bool* value_changed) const override {
    // TODO
    return false;
  }
};

class ListsDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  const char* Name() const override {
    return "blackwidow.ListsDataFilterFactory";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
    const CompactionFilter::Context& context) override {
    return std::unique_ptr<CompactionFilter>(new ListsDataFilter());
  }
};


}  // namespace blackwidow