#pragma once

#include "coding.h"
#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <string>

// https://github.com/OpenAtomFoundation/pika/wiki/pika-blackwidow%E5%BC%95%E6%93%8E%E6%95%B0%E6%8D%AE%E5%AD%98%E5%82%A8%E6%A0%BC%E5%BC%8F

namespace blackwidow {

using Slice = rocksdb::Slice;

class InternalValue {
 public:
  explicit InternalValue(Slice user_value)
    : start_(space_), user_value_(user_value), version_(0), timestamp_(0) {}

  virtual ~InternalValue() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  void set_timestamp(int32_t timestamp = 0) {
    timestamp = timestamp;
  }

  void SetRelativeTimestamp(int32_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    timestamp_ = static_cast<int32_t>(unix_time) + ttl;
  }

  void set_version(int32_t version) {
    version_ = version;
  }

  // version + timestamp
  static constexpr size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2;

  virtual const Slice Encode() {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
      if (start_ != space_) {
        delete[] start_;
      }
    }

    start_ = dst;
    size_t len = AppendTimestampAndVersion();
    return rocksdb::Slice(start_, len);
  }

  virtual size_t AppendTimestampAndVersion() = 0;

 protected:
  char space_[200];  // 预分配，小的keyvalue则可以避免二次分配内存
  char* start_;
  Slice user_value_;
  int32_t version_;
  int32_t timestamp_;
};

class ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get
  explicit ParsedInternalValue(std::string* value)
    : value_(value), version_(0), timestamp_(0) {}

  // Use this constructor in rocksdb::CompactionFilter::filter
  explicit ParsedInternalValue(const Slice& value)
    : value_(nullptr), version_(0), timestamp_(0) {}

  virtual ~ParsedInternalValue() = default;

  Slice user_value() const {
    return user_value_;
  }

  int32_t version() const {
    return version_;
  }

  void set_version(int32_t version) {
    version_ = version;
    SetVersionToValue();
  }

  int32_t timestamp() const {
    return timestamp_;
  }

  void set_timestamp(int32_t timestamp) {
    timestamp_ = timestamp;
    SetTimestampToValue();
  }

   void SetRelativeTimestamp(int32_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    timestamp_ = static_cast<int32_t>(unix_time) + ttl;
    SetTimestampToValue();
  }

  // 是否永久有效无过期时间
   bool IsPermanentSurvival() const {
    return timestamp_ == 0;
  }

  // 是否过期
  bool IsStale() const {
    if (timestamp_ == 0) {
      return false;
    }
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    return timestamp_ < unix_time;
  }

  virtual void StripSuffix() = 0;

 protected:
  virtual void SetVersionToValue() = 0;
  virtual void SetTimestampToValue()=0;

  std::string* value_;
  Slice user_value_;
  int32_t version_;
  int32_t timestamp_;
};

}  // namespace blackwidow