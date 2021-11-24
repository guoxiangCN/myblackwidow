#include "redis.h"

namespace blackwidow
{

Redis::Redis(BlackWidow *const bw, const DataType & type)
    : bw_(bw) {
    // TODO
}

Redis::~Redis() {
  auto tmp_handlers = handles_;
  handles_.clear();

  for(auto handle : tmp_handlers) {
    delete handle;
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




} // namespace blackwindow
