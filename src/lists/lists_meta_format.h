#pragma once

#include "base_value_format.h"
#include "coding.h"

#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include <cstdint>
#include <limits>

namespace blackwidow {

using Slice = rocksdb::Slice;

static constexpr uint64_t kInitialListsLeftSequence =
  std::numeric_limits<int64_t>::max();
static constexpr uint64_t kInitialListsRightSequence = kInitialLeftSequence + 1;

// meta key: | user_key |
// meta val:
// |length(8bytes)
// |version(4byte)|timestamp(4bytes)|LeftSequence(8bytes)|RightSequence(8bytes)|
class ListsMetaValue : public InternalValue {
 public:
  // user_data(length) + version + timestamp + LeftIndex + RightIndex
  static constexpr size_t kDefaultValueSuffixLength =
    sizeof(int32_t) * 2 + sizeof(uint64_t) * 2;

  // NOTE: user_value is always the length of list.
  explicit ListsMetaValue(Slice user_value)
    : InternalValue(user_value),
      left_index_(kInitialListsLeftSequence),
      right_index_(kInitialListsRightSequence) {}

  virtual size_t AppendIndex() {
    char* dst = start_;
    // Skip user_val (aka listlen) and version and timestamp
    dst += user_value_.size() + 2 * sizeof(int32_t);
    EncodeFixed64(dst, left_index_);
    dst += sizeof(uint64_t);
    EncodeFixed64(dst, right_index_);
    return 2 * sizeof(uint64_t);
  }

  size_t AppendTimestampAndVersion() override {
    // Append user data
    size_t usize = user_value_.size();
    char* dst = start_;
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    // Append version
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    // Append timestamp
    EncodeFixed32(dst, timestamp_);
    return usize + 2 * sizeof(int32_t);
  }

  const Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    size_t len = AppendTimestampAndVersion() + AppendIndex();
    return Slice(start_, len);
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

  uint64_t left_index() const {
    return left_index_;
  }

  uint64_t right_index() const {
    return right_index_;
  }

  void ModifyLeftIndex(uint64_t delta) {
    left_index_ -= delta;
  }

  void ModifyRightIndex(uint64_t delta) {
    right_index_ += delta;
  }

 private:
  uint64_t left_index_;
  uint64_t right_index_;
};

class ParsedListsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get()
  explicit ParsedListsMetaValue(std::string* internal_value_str)
    : ParsedInternalValue(internal_value_str),
      count_(0),
      left_index_(0),
      right_index_(0) {
    // blackwindow的assert
    // assert(internal_value_str->size() >= kListsMetaValueSuffixLength);
    assert(internal_value_str->size() ==
           (kListsMetaValueSuffixLength + sizeof(uint64_t)));
    if (internal_value_str->size() >= kListsMetaValueSuffixLength) {
      user_value_ =
        Slice(internal_value_str->data(),
              internal_value_str->size() - kListsMetaValueSuffixLength);
      version_ =
        DecodeFixed32(internal_value_str->data() + internal_value_str->size() -
                      sizeof(int32_t) * 2 - sizeof(int64_t) * 2);
      timestamp_ =
        DecodeFixed32(internal_value_str->data() + internal_value_str->size() -
                      sizeof(int32_t) - sizeof(int64_t) * 2);
      left_index_ =
        DecodeFixed64(internal_value_str->data() + internal_value_str->size() -
                      sizeof(int64_t) * 2);
      right_index_ =
        DecodeFixed64(internal_value_str->data() + internal_value_str->size() -
                      sizeof(int64_t));
    }
    count_ = DecodeFixed64(internal_value_str->data());
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedListsMetaValue(const Slice& internal_value_slice)
    : ParsedInternalValue(internal_value_slice),
      count_(0),
      left_index_(0),
      right_index_(0) {
    assert(internal_value_slice.size() >= kListsMetaValueSuffixLength);
    if (internal_value_slice.size() >= kListsMetaValueSuffixLength) {
      user_value_ =
        Slice(internal_value_slice.data(),
              internal_value_slice.size() - kListsMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_slice.data() +
                               internal_value_slice.size() -
                               sizeof(int32_t) * 2 - sizeof(int64_t) * 2);
      timestamp_ = DecodeFixed32(internal_value_slice.data() +
                                 internal_value_slice.size() - sizeof(int32_t) -
                                 sizeof(int64_t) * 2);
      left_index_ =
        DecodeFixed64(internal_value_slice.data() +
                      internal_value_slice.size() - sizeof(int64_t) * 2);
      right_index_ =
        DecodeFixed64(internal_value_slice.data() +
                      internal_value_slice.size() - sizeof(int64_t));
    }
    count_ = DecodeFixed64(internal_value_slice.data());
  }


  static const size_t kListsMetaValueSuffixLength =
    2 * sizeof(int32_t) + 2 * sizeof(int64_t);

  void StripSuffix() override {
    if (value_ != nullptr) {
      value_->erase(value_->size() - kListsMetaValueSuffixLength,
                    kListsMetaValueSuffixLength);
    }
  }

  void SetVersionToValue() override {
    if (value_ != nullptr) {
      char* dst = value_->data() + value_->size() - kListsMetaValueSuffixLength;
      EncodeFixed32(dst, version_);
    }
  }

  void SetTimestampToValue() override {
    if (value_ != nullptr) {
      char* dst = value_->data() + value_->size() - (2 * sizeof(uint64_t)) -
        (1 * sizeof(int32_t));
      EncodeFixed32(dst, timestamp_);
    }
  }

  void SetIndexToValue() {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
        2 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
      dst += sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

  uint64_t count() const {return count_};

  void set_count(uint64_t count) {
    count_ = count;
  }

  void ModifyCount(uint64_t delta) {
    count_ += delta;
    if(value_ != nullptr) {
      char* dst = value_->data();
      EncodeFixed64(dst, count_);
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

  uint64_t left_index() const {
    return left_index_;
  }

  void set_left_index(uint64_t left_index) {
    left_index_ = left_index;
  }

  void ModifyLeftIndex(uint64_t index) {
    left_index_ -= index;
    if (value_ != nullptr) {
      char* dst = const_cast<char *>(value_->data()) + value_->size() -
        2 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
    }
  }

  uint64_t right_index() const {
    return right_index_;
  }
  void set_right_index(uint64_t right_index) {
    right_index_ = right_index;
  }


  void ModifyRightIndex(uint64_t index) {
    right_index_ += index;
    if (value_ != nullptr) {
      char* dst = const_cast<char *>(value_->data()) + value_->size() -
        sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

  int32_t InitialMetaValue() {
    set_count(0);
    set_left_index(kInitialListsLeftSequence);
    set_right_index(kInitialListsRightSequence);
    set_timestamp(0);
    return UpdateVersion();
  }

 private:
  uint64_t count_;
  uint64_t left_index_;
  uint64_t right_index_;
};
}  // namespace blackwidow