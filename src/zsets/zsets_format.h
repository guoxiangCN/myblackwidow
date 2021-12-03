#pragma once

#include "rocksdb/slice.h"

// meta_cf:
//
// data_cf:
//
// score_cf:

namespace blackwidow {

using Slice = rocksdb::Slice;

class ZsetsMetaValue : public InternalValue {
 public:
  explicit ZsetsMetaValue(uint32_t zset_size)
    : InternalValue(Slice("****")), zset_size_(zset_size) {
    // **** 仅占位4字节 让InternalValue的user_value占位4字节去分配空间.
  }

  void set_zset_size(uint32_t zset_size) {
    zset_size_ = zset_size;
  }

  size_t AppendTimestampAndVersion() override {
    // 4 bytes for zset size
    char* dst = start_;
    EncodeFixed32(dst, zset_size_);
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
  uint32_t zset_size_;
};

class ParsedZsetsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get
  explicit ParsedZsetsMetaValue(std::string* value)
    : ParsedInternalValue(value) {
    assert(value->size() == (sizeof(uint32_t) * 3));
    char* ptr = value->data();

    // Decode hash_size
    zset_size_ = DecodeFixed32(ptr);
    ptr += sizeof(uint32_t);

    // Decode version
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    // Decode timestamp
    timestamp_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
  }

  // Use this constructor in rocksdb::CompactionFilter
  explicit ParsedZsetsMetaValue(const Slice& value)
    : ParsedInternalValue(value) {
    // TODO
    assert(false);
  }

  uint32_t zset_szie() const {
    return zset_size_;
  }

  void set_zset_size(uint32_t zset_size) {
    zset_size_ = zset_size;
    SetZSetSizeToValue();
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

  void SetZSetSizeToValue() {
    if (value_) {
      char* ptr = value_->data();
      EncodeFixed32(ptr, zset_size_);
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
  uint32_t zset_size_;
};

class ZsetsDataKey {
 public:
  explicit ZsetsDataKey(const Slice& key, const Slice& member, int32_t version)
    : key_(key), member_(member), version_(version), start_(space_) {}

  virtual ~ZsetsDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice Encode() {
    size_t needed =
      sizeof(uint32_t) + key_.size() + sizeof(int32_t) + member_.size();
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

    // member (这里兼容空member)
    if (member_.size() > 0) {
      memcpy(ptr, member_.data(), member_.size());
      ptr += member_.size();
    }

    return Slice(start_, ptr - start_);
  }

 private:
  char space_[250];  // reversed for short keys
  char* start_;
  const Slice key_;
  const Slice member_;
  const int32_t version_;
};

// MemberKey: |KeySize(4bytes)|UserKey|Version(4bytes)|Member|
// MemberVal: |Score|
class ParsedZsetsDataKey {
 public:
  // use this constructor in CompactionFilter
  explicit ParsedZsetsDataKey(const Slice& raw_key) {
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

    // 兼容member为空
    if (raw_key.size() > (ptr - raw_key.data())) {
      member_ = Slice(ptr, raw_key.size() - (ptr - raw_key.data()));
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

  const Slice member() const {
    return member_;
  }

 private:
  uint32_t key_size_;
  Slice user_key_;
  int32_t version_;
  Slice member_;
};


}  // namespace blackwidow