#pragma once


#include "rocksdb/db.h"

namespace blackwidow {
class ScopeSnapshot {
 public:
  ScopeSnapshot(rocksdb::DB* db, const rocksdb::Snapshot** snapshot)
    : db_(db), snapshot_(snapshot) {
    *snapshot_ = db_->GetSnapshot();
  }
  ~ScopeSnapshot() {
    db_->ReleaseSnapshot(*snapshot_);
  }

 private:
  rocksdb::DB* const db_;
  const rocksdb::Snapshot** snapshot_;
  ScopeSnapshot(const ScopeSnapshot&);
  void operator=(const ScopeSnapshot&);
};

}  // namespace blackwidow