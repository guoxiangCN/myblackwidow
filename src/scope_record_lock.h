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
class MultiScopedRecordLock {
public:
    MultiScopedRecordLock(const MultiScopedRecordLock&) = delete;
    MultiScopedRecordLock& operator==(const MultiScopedRecordLock&) = delete;

    MultiScopedRecordLock(LockMgr *lock_mgr, const std::vector<std::string> &keys)
        :lock_mgr_(lock_mgr), keys_(keys) {
         std::string pre_key;
    std::sort(keys_.begin(), keys_.end());
    if (!keys_.empty() &&
      keys_[0].empty()) {
      lock_mgr_->TryLock(pre_key);
    }

    for (const auto& key : keys_) {
      if (pre_key != key) {
        lock_mgr_->TryLock(key);
        pre_key = key;
      }
    }
    }

    ~MultiScopedRecordLock() {
        std::string pre_key;
    if (!keys_.empty() &&
      keys_[0].empty()) {
      lock_mgr_->UnLock(pre_key);
    }

    for (const auto& key : keys_) {
      if (pre_key != key) {
        lock_mgr_->UnLock(key);
        pre_key = key;
      }
    }
    }
private:
 LockMgr* const lock_mgr_;
 std::vector<std::string> keys_;
};

}  // namespace blackwidow
