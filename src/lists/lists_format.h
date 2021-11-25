#pragma once

#include "coding.h"
#include "base_value_format.h"
#include "rocksdb/slice.h"

#include <cstdint>
#include <limits>

namespace blackwidow {

using Slice = rocksdb::Slice;

static constexpr uint64_t kInitialListsLeftSequence = std::numeric_limits<int64_t>::max();
static constexpr uint64_t kInitialListsRightSequence = kInitialLeftSequence + 1;

// meta key: | user_key |
// meta val:
// |length(4bytes)|version(4byte)|timestamp(4bytes)|LeftSequence(4bytes)|RightSequence(4bytes)|
class ListsMetaValue : public InternalValue {
 public:
  // user_data(length) + version + timestamp + LeftIndex + RightIndex
  static constexpr size_t kDefaultValueSuffixLength =
    sizeof(int32_t) * 2 + sizeof(uint64_t) * 2;

  explicit ListsMetaValue(Slice user_value) 
    : InternalValue(user_value),
    left_index_(kInitialListsLeftSequence),
    right_index_(kInitialListsRightSequence){}

  virtual size_t AppendIndex() {
    char* dst = start_;
  }

 private:
  uint64_t left_index_;
  uint64_t right_index_;
};

class ParsedListsMetaValue {};
}  // namespace blackwidow