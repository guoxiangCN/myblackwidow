#pragma once


#include "coding.h"
#include <rocksdb/status.h>
#include <rocksdb/comparator.h>


namespace blackwidow {

class ListDataKeyComparatorImpl : public rocksdb::Comparator {
public:
    ListDataKeyComparatorImpl() {}

    const char* Name() const override {
      return "blackwidow.ListDataKeyComparator";
    }

    // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  // Note that Compare(a, b) also compares timestamp if timestamp size is
  // non-zero. For the same user key with different timestamps, larger (newer)
  // timestamp comes first.
  int Compare(const Slice& a, const Slice& b) const override {
    // list data key format: |keysz|key|version|index|
    assert(!a.empty() && !b.empty());
    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    int32_t a_size = static_cast<int32_t>(a.size());
    int32_t b_size = static_cast<int32_t>(b.size());
    int32_t key_a_len = DecodeFixed32(ptr_a);
    int32_t key_b_len = DecodeFixed32(ptr_b);
    ptr_a += sizeof(int32_t);
    ptr_b += sizeof(int32_t);
    Slice sets_key_a(ptr_a,  key_a_len);
    Slice sets_key_b(ptr_b,  key_b_len);
    ptr_a += key_a_len;
    ptr_b += key_b_len;
    if (sets_key_a != sets_key_b) {
      return sets_key_a.compare(sets_key_b);
    }
    if (ptr_a - a.data() == a_size &&
      ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    int32_t version_a = DecodeFixed32(ptr_a);
    int32_t version_b = DecodeFixed32(ptr_b);
    ptr_a += sizeof(int32_t);
    ptr_b += sizeof(int32_t);
    if (version_a != version_b) {
      return version_a < version_b ? -1 : 1;
    }
    if (ptr_a - a.data() == a_size &&
      ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    uint64_t index_a = DecodeFixed64(ptr_a);
    uint64_t index_b = DecodeFixed64(ptr_b);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (index_a != index_b) {
      return index_a < index_b ? -1 : 1;
    } else {
      return 0;
    }

    return 1;
  }

   // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override {

  }

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortSuccessor(std::string* key) const override {

  }
};

}    // namespace blackwidow