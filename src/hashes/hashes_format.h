#pragma once

#include "../base_value_format.h"
#include "rocksdb/env.h"
#include <cassert>

// MetaKey:  |UserKey|
// MetaVal:  |HashSize(4bytes)|Version(4byte)|Timestamp(4byte)|

// FieldKey: |KeySize|UserKey|Version|Field|
// FieldVal: |FieldValue|
namespace blackwidow {

class HashesMetaValue : InternalValue {
 public:
  explicit HashesMetaValue(uint32_t hash_size)
    : InternalValue(Slice("****")), hash_size_(hash_size) {
    // **** 仅占位4字节 让InternalValue的user_value占位4字节去分配空间.
  }

  void set_hash_size(uint32_t hash_size) {
    hash_size_ = hash_size_;
  }

  size_t AppendTimestampAndVersion() override {
    // 4 bytes for hash size
    char* dst = start_;
    EncodeFixed32(dst, hash_size_);
    dst += sizeof(uint32_t);

    // 4 bytes for version
    EncodeFixed32(dst, version_);
    dst += sizeof(uint32_t);

    // 4 bytes for timestamp
    EncodeFixed32(dst, timestamp_);
    dst += sizeof(uint32_t);

    return dst - start_;
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    return version_;
  }

 private:
  uint32_t hash_size_;
};


class ParsedHashesMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get
  explicit ParsedHashesMetaValue(std::string* value)
    : ParsedInternalValue(value) {
    assert(value->size() == (sizeof(uint32_t) * 3));
    char* ptr = value->data();

    // Decode hash_size
    hash_size_ = DecodeFixed32(ptr);
    ptr += sizeof(uint32_t);

    // Decode version
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    // Decode timestamp
    timestamp_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
  }

  // Use this constructor in rocksdb::CompactionFilter
  explicit ParsedHashesMetaValue(const Slice& value)
    : ParsedInternalValue(value) {
    // TODO
    assert(false);
  }

  uint32_t hash_size() const {
    return hash_size_;
  }

  void set_hash_size(uint32_t hash_size) {
    hash_size_ = hash_size;
  }

  void InitialMetaValue() {
    this->set_hash_size(0);
    this->set_timestamp(0);
    this->UpdateVersion();
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(value_->begin() + sizeof(uint32_t), value_->end());
    }
  }

  void SetVersionToValue() override {
    if(value_) {
      char* ptr = const_cast<char*>(value_->data());
      ptr += sizeof(uint32_t);
      EncodeFixed32(ptr, version_);
    }
  }
  void SetTimestampToValue() override {
    if(value_) {
      char* ptr = const_cast<char*>(value_->data());
      ptr += sizeof(uint32_t) + sizeof(int32_t);
      EncodeFixed32(ptr, timestamp_);
    }
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

 private:
  uint32_t hash_size_;
};


}  // namespace blackwidow
