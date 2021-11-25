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
  int Compare(const Slice& a, const Slice& b) override {
    return 0;
  }

   // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortestSeparator(std::string* start,
                                     const Slice& limit) override {

  }

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortSuccessor(std::string* key) override {

  }
};

}    // namespace blackwidow