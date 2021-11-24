#pragma once

#include "mutex.h"

#include <memory>
#include <string>

namespace blackwidow {

struct LockMap;
struct LockMapStripe;

class LockMgr {
 public:
  LockMgr(const LockMgr&) = delete;
  LockMgr(const LockMgr&&) = delete;
  LockMgr& operator==(const LockMgr&) = delete;
  LockMgr& operator==(const LockMgr&&) = delete;

  LockMgr(size_t default_num_stripes,
          int64_t max_num_locks,
          std::shared_ptr<MutexFactory> factory);

  ~LockMgr();

  Status TryLock(const std::string& key);
  void UnLock(const std::string& key);

 private:
  // Never used.
  Status Acquire(LockMapStripe* stripe, const std::string& key);
  // Never used.
  void UnLockKey(const std::string& key, LockMapStripe* stripe);

 private:
  const size_t default_num_stripes_;
  const int64_t max_num_locks_;
  std::shared_ptr<MutexFactory> mutex_factory_;
  std::shared_ptr<LockMap> lock_map_;
};


}  // namespace blackwidow