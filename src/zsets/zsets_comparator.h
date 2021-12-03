#pragma once

#include "rocksdb/comparator.h"
#include "zsets_format.h"

namespace blackwidow {

using rocksdb::Slice;

// ZsetScoreKeyLayout:
// <key_size><key><version><score><member> : <nil>
class ZsetScoreKeyComparatorImpl : rocksdb::Comparator {
 public:
  const char* Name() const override {
    return "blackwidow.ZsetScoreKeyComparator";
  }

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  // Note that Compare(a, b) also compares timestamp if timestamp size is
  // non-zero. For the same user key with different timestamps, larger (newer)
  // timestamp comes first.
  int Compare(const Slice& a, const Slice& b) const override {
    ParsedZsetsScoreKey scorekey_a(a);
    ParsedZsetsScoreKey scorekey_b(b);
    // 1. compare with zset key
    int cmp_res = scorekey_a.key().compare(scorekey_b.key());
    if (cmp_res != 0) {
      return cmp_res;
    }

    // 2. compare the version number
    int32_t version_a = scorekey_a.version();
    int32_t version_b = scorekey_b.version();
    cmp_res = version_a - version_b;
    if (cmp_res != 0) {
      return cmp_res;
    }

    // 3.compare the store
    double score_a = scorekey_a.score();
    double score_b = scorekey_b.score();
    cmp_res = score_a - score_b;
    if(cmp_res != 0) {
      return cmp_res;
    }

    // 4. compare the member with dictionary order.
    return scorekey_a.member().compare(scorekey_b.member());
  }

  // 如果start<limit 返回1个短的字符串，使得 start<= string < limit范围
  // 主要是压缩字符串的存储空间.
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {}

  // 返回一个短的字符串string, 前提是string>=key, 主要是为了减少字符串的存储空间
  // 假设key是"aaab", 最短的大于key的值是"b"
  void FindShortSuccessor(std::string* key) const override {}
};

}  // namespace blackwidow
