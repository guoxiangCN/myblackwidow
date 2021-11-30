#include "redis.h"

namespace blackwidow
{

Redis::Redis(BlackWidow *const bw, const DataType & type)
    : bw_(bw), 
    type_(type),
    lock_mgr_(new LockMgr(1000, 0, std::make_shared<MutexFactoryImpl>())),
    db_(nullptr),
    small_compaction_threshold_(5000) {
  handles_.clear();
}

Redis::~Redis() {
  auto tmp_handlers = handles_;
  handles_.clear();

  for(auto handle : tmp_handlers) {
    //delete handle;
    db_->DestroyColumnFamilyHandle(handle);
  }

  delete db_;
  delete lock_mgr_;
  delete statistics_store_;
  delete scan_cursors_store_;
}




Status Redis::SetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options) {
        if(option_type == OptionType::kDB) {
          return db_->SetDBOptions(options);
        }
        // 列族options
        if (handles_.size() == 0) {
          return db_->SetOptions(db_->DefaultColumnFamily(), options);
        }
        // 所有列族设置
        Status s;
        for(auto handle: handles_) {
          s = db_->SetOptions(handle, options);
          if(!s.ok())
            break;
        }
        return s;
}
Status Redis::SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys) {
  // TODO
  //statistics_store_->SetCapacity(max_cache_statistic_keys);
  return Status::OK();
}

Status Redis::SetSmallCompactionThreshold(size_t small_compaction_threshold) {
  small_compaction_threshold_ = small_compaction_threshold;
  return Status::OK();
}




} // namespace blackwindow
