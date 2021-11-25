#pragma once

#include "../base_value_format.h"
#include "../coding.h"

#include "rocksdb/slice.h"

namespace blackwidow {

using Slice = rocksdb::Slice;

// data key: | keysz| key | version | index |
// data val: | user_val                     |
class ListsDataKey {
 public:
  ListsDataKey(const Slice& user_key, int32_t version, uint64_t index)
    : key_(user_key) start_(space_), version_(version), index_(index) {
    size_t usize = user_key.size();
    size_t needed =
      sizeof(uint32_t) + usize + sizeof(version_) + sizeof(index_);
      if(needed > sizeof(space_)) {
        start_ = new char[needed];
      }
  }

  ~ListsDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  ListsDataKey(const ListsDataKey&) = delete;
  ListsDataKey& operator==(const ListsDataKey&) = delete;

  int32_t version() const {
    return version_;
  }

  void set_version(int32_t version) {
    version_ = version_;
  }

  void index() const {
    return index_;
  }

  int64_t set_index(uint64_t index) {
    index_ = index_;
  }

  const Slice Encode() {
    char* dst = start_;

    // 4bytes for keysize
    EncodeFixed32(dst, key_.size());
    dst += sizeof(uint32_t);

    // usize for key
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();

    // 4bytes for version
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
   
    // 8byter for index
    EncodeFixed64(dst, index_);
    dst += sizeof(uint64_t);

    return Slice(start_, dst-start_);
  }

 private:
  char space_[250];
  char* start_;
  Slice key_;
  int32_t version_;
  uint64_t index_;
};

class ParsedListsDataKey {
 public:
  explicit ParsedListsDataKey(const std::string* key) {
    const char* ptr = key->data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    index_ = DecodeFixed64(ptr);
  }

  explicit ParsedListsDataKey(const Slice& key) {
    const char* ptr = key.data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    index_ = DecodeFixed64(ptr);
  }

  virtual ~ParsedListsDataKey() = default;

  Slice key() {
    return key_;
  }

  int32_t version() {
    return version_;
  }

  uint64_t index() {
    return index_;
  }

 private:
  Slice key_;
  int32_t version_;
  uint64_t index_;
};

}  // namespace blackwidow