#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "lock_mgr.h"

namespace blackwidow {


class ScopeRecordLock {
 public:
  ScopeRecordLock(const ScopeRecordLock&) = delete;
  ScopeRecordLock& operator==(const ScopeRecordLock&) = delete;

  ScopeRecordLock(LockMgr* lock_mgr, const Slice& key)
    : lock_mgr_(lock_mgr), key_(key) {
    lock_mgr_->TryLock(key_.ToString());
  }

  ~ScopeRecordLock() {
    lock_mgr_->UnLock(key_.ToString());
  }

 private:
  LockMgr* const lock_mgr_;
  Slice key_;
};

using RecordLockGuard = ScopeRecordLock;

// TODO
class MultiScopedReadLock {};

}  // namespace blackwidow
