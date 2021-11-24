#include "lock_mgr.h"
#include "murmurhash.h"

#include <atomic>
#include <cassert>
#include <unordered_set>
#include <vector>

namespace blackwidow {

struct LockMapStripe {
  explicit LockMapStripe(std::shared_ptr<MutexFactory> factory) {
    stripe_mutex = factory->AllocateMutex();
    stripe_cv = factory->AllocateCondVar();
    assert(stripe_mutex);
    assert(stripe_cv);
  }
  std::shared_ptr<Mutex> stripe_mutex;
  std::shared_ptr<CondVar> stripe_cv;
  std::unordered_set<std::string> keys;
};

struct LockMap {
  explicit LockMap(size_t num_stripes, std::shared_ptr<MutexFactory> factory)
    : num_stripes_(num_stripes) {
    lock_map_stripes_.reserve(num_stripes);

    for (size_t i = 0; i < num_stripes; i++) {
      auto stripe = new LockMapStripe(factory);
      lock_map_stripes_.push_back(stripe);
    }
  }

  ~LockMap() {
    for (auto& stripe : lock_map_stripes_) {
      delete stripe;
    }
  }

  size_t GetStripe(const std::string& key) const;

  const size_t num_stripes_;
  std::atomic<int64_t> lock_cnt{0};
  std::vector<LockMapStripe*> lock_map_stripes_;
};

size_t LockMap::GetStripe(const std::string& key) const {
  assert(num_stripes_ > 0);
  static murmur_hash hash;
  size_t stripe = hash(key) % num_stripes_;
  return stripe;
}


LockMgr::LockMgr(size_t default_num_stripes,
                 int64_t max_num_locks,
                 std::shared_ptr<MutexFactory> factory)
  : default_num_stripes_(default_num_stripes),
    max_num_locks_(max_num_locks),
    mutex_factory_(factory),
    lock_map_(std::make_shared<LockMap>(default_num_stripes, factory)) {}

LockMgr::~LockMgr() {}

Status LockMgr::TryLock(const std::string& key) {
#ifdef LOCKLESS
  return Status::OK();
#else
  size_t stripe_num = lock_map_->GetStripe(key);
  assert(lock_map_->lock_map_stripes_.size());
  LockMapStripe* stripe = lock_map_->lock_map_stripes_.at(stripe_num);
  return stripe->stripe_mutex->Lock();
#endif
}

void LockMgr::UnLock(const std::string& key){
#ifdef LOCKLESS
#else
  size_t stripe_num = lock_map_->GetStripe(key);
  assert(lock_map_->lock_map_stripes_.size());
  LockMapStripe *stripe = lock_map_->lock_map_stripes_.at(stripe_num);
  stripe->stripe_mutex->UnLock();
#endif
}

Status LockMgr::Acquire(LockMapStripe* stripe, const std::string& key) {
  return Status::OK();
}

void LockMgr::UnLockKey(const std::string& key, LockMapStripe* stripe){
  // NOP
}

}  // namespace blackwidow