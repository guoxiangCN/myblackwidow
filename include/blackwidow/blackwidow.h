#pragma once

#include <list>
#include <map>
#include <queue>
#include <string>
#include <unistd.h>  // NOTE
#include <vector>
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

namespace blackwidow {

const double ZSET_SCORE_MAX = std::numeric_limits<double>::max();
const double ZSET_SCORE_MIN = std::numeric_limits<double>::lowest();

// https://rocksdb.org.cn/doc/Memory-usage-in-RocksDB.html
const std::string PROPERTY_TYPE_ROCKSDB_MEMTABLE =
  "rocksdb.cur-size-all-mem-tables";
// 获取index和filterblock占用的内存
const std::string PROPERTY_TYPE_ROCKSDB_TABLE_READER =
  "rocksdb.estimate-table-readers-mem";
const std::string PROPERTY_TYPE_ROCKSDB_BACKGROUND_ERRORS =
  "rocksdb.background-errors";

static const std::string ALL_DB = "all";
static const std::string STRINGS_DB = "strings";
static const std::string HASHES_DB = "hashes";
static const std::string LISTS_DB = "lists";
static const std::string ZSETS_DB = "zsets";
static const std::string SETS_DB = "sets";

static constexpr size_t BATCH_DELETE_LIMIT = 100;
static constexpr size_t COMPACT_THRESHOLD_COUNT = 2000;

using Options = rocksdb::Options;
using BlockBasedTableOptions = rocksdb::BlockBasedTableOptions;  // TODO
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class Mutex;
class CondVar;

class RedisStrings;
class RedisHashes;
class RedisSets;
class RedisLists;
class RedisZSets;
class HyperLogLog;
enum class OptionType;

template <typename T1, typename T2>
class LRUCache;

struct BlackWidowOptions {
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;
  size_t block_cache_size;
  bool share_block_cache;
  size_t statistics_max_size;
  size_t small_compaction_threshold;

  explicit BlackWidowOptions()
      : block_cache_size(0),
        share_block_cache(false),
        statistics_max_size(0),
        small_compaction_threshold(5000) {}

  Status ResetOptions(const OptionType& option_type,
                      const std::unordered_map<std::string, std::string>& options_map);
};

struct KeyValue {
  std::string key;
  std::string value;

  KeyValue() = default;
  KeyValue(const std::string &key_, const std::string &value_)
    :key(key_), value(value_) {}

  bool operator==(const KeyValue& kv) const {
    return (kv.key == key && kv.value == value);
  }
  bool operator<(const KeyValue& kv) const {
    return key < kv.key;
  }
};

struct KeyInfo {
  uint64_t keys;
  uint64_t expires;
  uint64_t avg_ttl;
  uint64_t invalid_keys;
};

struct ValueStatus {
  std::string value;
  Status status;
  bool operator==(const ValueStatus& vs) const {
    return (vs.value == value && vs.status == status);
  }
};

struct FieldValue {
  std::string field;
  std::string value;
  bool operator==(const FieldValue& fv) const {
    return (fv.field == field && fv.value == value);
  }
};

struct KeyVersion {
  std::string key;
  int32_t version;
  bool operator == (const KeyVersion& kv) const {
    return (kv.key == key && kv.version == version);
  }
};

struct ScoreMember {
  double score;
  std::string member;
  bool operator == (const ScoreMember& sm) const {
    return (sm.score == score && sm.member == member);
  }
};

enum BeforeOrAfter {
  Before,
  After
};

enum DataType {
  kAll,
  kStrings,
  kHashes,
  kLists,
  kZSets,
  kSets
};

static const char DataTypeTag[] = {
  'a',  // all
  'k',  // key
  'h',  // hash
  'l',  // list
  'z',  // zset
  's'   // set
};

enum class OptionType {
  kDB,
  kColumnFamily,
};

enum ColumnFamilyType {
  kMeta,
  kData,
  kMetaAndData
};

enum AGGREGATE {
  SUM,
  MIN,
  MAX
};

enum BitOpType {
  kBitOpAnd = 1,
  kBitOpOr,
  kBitOpXor,
  kBitOpNot,
  kBitOpDefault
};

enum Operation {
  kNone = 0,
  kCleanAll,
  kCleanStrings,
  kCleanHashes,
  kCleanZSets,
  kCleanSets,
  kCleanLists,
  kCompactKey
};

struct BGTask {
  DataType type;
  Operation operation;
  std::string argv;

  BGTask(const DataType& _type = DataType::kAll,
         const Operation& _opeation = Operation::kNone,
         const std::string& _argv = "") : type(_type), operation(_opeation), argv(_argv) {}
};

class BlackWidow {
public:
 BlackWidow();
 ~BlackWidow();

 Status Open(const BlackWidowOptions& bw_options, const std::string& dbpath);


    // Strings Commands

    // Hashes Commands

    // Sets Commands

    // ...

private:
    RedisStrings* strings_db_;
    RedisHashes* hashes_db_;
    RedisSets* sets_db_;
    RedisZSets* zsets_db_;
    RedisLists* lists_db_;
    std::atomic_bool is_opened_;

    LRUCache<std::string, std::string>* cursors_store_;

    pthread_t bg_tasks_thread_id;
    std::shared_ptr<Mutex> bg_tasks_mutex_;
    std::shared_ptr<CondVar> bg_tasks_cond_var_;
    std::queue<BGTask> bg_tasks_queue_;

    std::atomic<int> current_task_type_;
    std::atomic<bool> bg_tasks_should_exit_;

  // For scan keys in data base
  std::atomic<bool> scan_keynum_exit_;
};


}  // namespace blackwindow