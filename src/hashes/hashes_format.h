#pragma once

#include "base_value_format.h"
#include "coding.h"
#include "rocksdb/env.h"
#include <cassert>

// MetaKey:  |UserKey|
// MetaVal:  |HashSize(4bytes)|Version(4byte)|Timestamp(4byte)|

// FieldKey: |KeySize(4bytes)|UserKey|Version(4bytes)|Field|
// FieldVal: |FieldValue|
namespace blackwidow {

class HashesMetaValue : public InternalValue {
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
    SetHashSizeToValue();
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

  void SetHashSizeToValue() {
    if(value_) {
      char* ptr = value_->data();
      EncodeFixed32(ptr, hash_size_);
    }
  }

  void SetVersionToValue() override {
    if (value_) {
      char* ptr = const_cast<char*>(value_->data());
      ptr += sizeof(uint32_t);
      EncodeFixed32(ptr, version_);
    }
  }
  void SetTimestampToValue() override {
    if (value_) {
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

class HashesDataKey {
 public:
  explicit HashesDataKey(const Slice& key, const Slice& field, int32_t version)
    : key_(key), field_(field), version_(version), start_(space_) {}

  virtual ~HashesDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice Encode() {
    size_t needed =
      sizeof(uint32_t) + key_.size() + sizeof(int32_t) + field_.size();
    if (needed > sizeof(space_)) {
      start_ = new char[needed];
    }
    char* ptr = start_;

    // Key size
    EncodeFixed32(ptr, static_cast<uint32_t>(key_.size()));
    ptr += sizeof(uint32_t);

    // key (这里兼容空key)
    if (key_.size() > 0) {
      memcpy(ptr, key_.data(), key_.size());
      ptr += key_.size();
    }

    // version
    EncodeFixed32(ptr, static_cast<uint32_t>(version_));
    ptr += sizeof(int32_t);

    // field (这里兼容空field)
    if (field_.size() > 0) {
      memcpy(ptr, field_.data(), field_.size());
      ptr += field_.size();
    }

    return Slice(start_, ptr - start_);
  }

 private:
  char space_[250];  // reversed for short keys
  char* start_;
  const Slice key_;
  const Slice field_;
  const int32_t version_;
};

// FieldKey: |KeySize(4bytes)|UserKey|Version(4bytes)|Field|
// FieldVal: |FieldValue|
class ParsedHashesDataKey {
 public:
  // use this constructor in CompactionFilter
  explicit ParsedHashesDataKey(const Slice& raw_key) {
    assert(raw_key.size() >= (sizeof(uint32_t) + sizeof(int32_t)));
    const char* ptr = raw_key.data();
    key_size_ = DecodeFixed32(ptr);
    ptr += sizeof(uint32_t);

    // 兼容key为空
    if (key_size_ > 0) {
      user_key_ = Slice(ptr, key_size_);
      ptr += key_size_;
    }

    version_ = static_cast<int32_t>(DecodeFixed32(ptr));
    ptr += sizeof(int32_t);

    // 兼容field为空
    if(raw_key.size() > (ptr - raw_key.data())) {
      field_ = Slice(ptr, raw_key.size() - (ptr-raw_key.data()));
    }
  }

  const uint32_t key_size() const {
    return key_size_;
  }

  const Slice user_key() const {
    return user_key_;
  }

  int32_t version() const {
    return version_;
  }

  const Slice field() const {
    return field_;
  }

 private:
  uint32_t key_size_;
  Slice user_key_;
  int32_t version_;
  Slice field_;
};
}  // namespace blackwidow
