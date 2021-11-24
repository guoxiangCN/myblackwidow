#pragma once

#include "rocksdb/db.h"

namespace blackwindow {
class ScopeSnapshot {
    public:
    ScopeSnapshot(rocksdb::DB *db, const rocksdb::Snapshot **snapshot)
        : db_(db), snapshot_(snapshot) {
      *snapshot = db->GetSnapshot();
    }

    ~ScopeSnapshot() {
      db_->ReleaseSnapshot(*snapshot_);
    }

     ScopeSnapshot(const ScopeSnapshot&) = delete;
     ScopeSnapshot& operator==(const ScopeSnapshot&) = delete;
    private:
     rocksdb::DB* const db_;
     const rocksdb::Snapshot** snapshot_;
};
};