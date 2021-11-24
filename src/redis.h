#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

#include "blackwidow/blackwidow.h"
#include "lock_mgr.h"
#include "lru_cache.h"
#include "mutex_impl.h"

namespace blackwidow {

class Redis {
 public:
  Redis(BlackWidow* const bw, const DataType& type);
  virtual ~Redis();

  rocksdb::DB* GetDB() {
    return db_;
  }

  Status SetOptions(
    const OptionType& option_type,
    const std::unordered_map<std::string, std::string>& options);

  // Common Commands
  virtual Status Open(const BlackWidowOptions& bw_options,
                      const std::string& dbpath) = 0;
  virtual Status CompactRange(const rocksdb::Slice* begin,
                              const rocksdb::Slice* end,
                              const ColumnFamilyType& type = kMetaAndData) = 0;
  virtual Status GetProperty(const std::string& property, uint64_t* out) = 0;
  // Keys Commands

  // Aux

 protected:
  BlackWidow* const bw_;
  DataType type_;
  LockMgr* lock_mgr_;

  rocksdb::DB* db_;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
  rocksdb::WriteOptions default_write_options_;
  rocksdb::ReadOptions default_read_options_;
  rocksdb::CompactRangeOptions default_compact_range_options_;

  // For Scan
  LRUCache<std::string, std::string>* scan_cursors_store_;

  Status GetScanStartPoint(const Slice& key,
                           const Slice& pattern,
                           int64_t cursor,
                           std::string* start_point);
  Status StoreScanNextPoint(const Slice& key,
                            const Slice& pattern,
                            int64_t cursor,
                            const std::string& next_point);

  // For Statistics
  std::atomic<size_t> small_compaction_threshold_;
  LRUCache<std::string, size_t>* statistics_store_;

  Status UpdateSpecificKeyStatistics(const std::string& key, size_t count);
  Status AddCompactKeyTaskIfNeeded(const std::string& key, size_t total);
};
}  // namespace blackwindow