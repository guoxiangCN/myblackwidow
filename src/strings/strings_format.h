#pragma once

#include "base_value_format.h"
#include <cassert>
#include <string>

namespace blackwidow {

// strings format:
// key   | user_key |
// val   | user_val | timestamp(4bytes) |

class StringsValue : public InternalValue {
 public:
  explicit StringsValue(const Slice& user_value) : InternalValue(user_value) {}

  size_t AppendTimestampAndVersion() override {
    char* dst = start_;
    size_t usize = user_value_.size();
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed32(dst, timestamp_);
    return usize + sizeof(int32_t);
  }
};

class ParsedStringsValue : public ParsedInternalValue {
 public:
  // Use after DB::Get()
  explicit ParsedStringsValue(std::string* internal_value_str)
    : ParsedInternalValue(internal_value_str) {
    assert(internal_value_str->size() >= kStringsValueSuffixLength);
    if (internal_value_str->size() >= kStringsValueSuffixLength) {
      user_value_ =
        Slice(internal_value_str->data(),
              internal_value_str->size() - kStringsValueSuffixLength);
      timestamp_ =
        DecodeFixed32(internal_value_str->data() + user_value_.size());
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedStringsValue(const Slice& internal_value_slice)
    : ParsedInternalValue(internal_value_slice) {
    assert(internal_value_slice.size() >= kStringsValueSuffixLength);
    if (internal_value_slice.size() >= kStringsValueSuffixLength) {
      user_value_ =
        Slice(internal_value_slice.data(),
              internal_value_slice.size() - kStringsValueSuffixLength);
      timestamp_ =
        DecodeFixed32(internal_value_slice.data() +
                      internal_value_slice.size() - kStringsValueSuffixLength);
    }
  }

  void StripSuffix() override {
    if (value_ != nullptr) {
      value_->erase(value_->size() - kStringsValueSuffixLength,
          kStringsValueSuffixLength);
    }
  }

  // Strings type do not have version field;
  void SetVersionToValue() override {
  }

  void SetTimestampToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
        kStringsValueSuffixLength;
      EncodeFixed32(dst, timestamp_);
    }
  }

  Slice value() {
    return user_value_;
  }

  static constexpr size_t kStringsValueSuffixLength = sizeof(int32_t);
};


}  // namespace blackwidow