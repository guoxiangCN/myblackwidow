#include "gtest/gtest.h"
#include "redis_hashes.h"
#include "testing_util.h"
#include "unistd.h"
#include <execinfo.h>
#include <iostream>
#include <thread>

namespace {
static std::string kTestingPath = "./testdb_hashes";
static const char* kCmdDeleteTestingPath = "rm -rf ./testdb_hashes";
}  // namespace


TEST(TestHExists, RedisHashesTest) {
  blackwidow::RedisHashes* redis = nullptr;

  testing::Defer df([&]() {
    if (redis != nullptr)
      delete redis;
    system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisHashes(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::string field_value;
  uint32_t hash_size = -1;

  s = redis->HExists("USER_17802525", "Name");
  EXPECT_TRUE(s.IsNotFound());

  s = redis->HLen("USER_17802525", &hash_size);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(0, hash_size);

  s = redis->HSet("USER_17802525", "Name", "喜欢RAP和篮球", nullptr);
  EXPECT_TRUE(s.ok());

  s = redis->HLen("USER_17802525", &hash_size);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, hash_size);

  s = redis->HExists("USER_17802525", "Name");
  EXPECT_TRUE(s.ok());

  s = redis->HGet("USER_17802525", "Name", &field_value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ("喜欢RAP和篮球", field_value);

  // s = redis->HSet("USER_17802525", "Birthday", "19961228");
  // EXPECT_TRUE(s.ok());
}

TEST(TestHStrlen, RedisHashesTest) {
  blackwidow::RedisHashes* redis = nullptr;

  testing::Defer df([&]() {
    if (redis != nullptr)
      delete redis;
    system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisHashes(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  int32_t len = -1;
  std::string key = "USER_INFO_17802530";

  s = redis->HStrlen(key, "UID", &len);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(0, len);

  s = redis->HStrlen(key, "AGE", &len);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(0, len);

  s = redis->HSet(key, "UID", "helloworld", nullptr);
  EXPECT_TRUE(s.ok());  

  s = redis->HStrlen(key, "UID", &len);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(10, len);

  s = redis->HStrlen(key, "AGE", &len);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(0, len);
}

TEST(TestHGetAll, RedisHashesTest) {
  blackwidow::RedisHashes* redis = nullptr;

  testing::Defer df([&]() {
    if (redis != nullptr)
      delete redis;
    system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisHashes(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::string key = "USER_INFO_17802525";
  s = redis->HSet(key, "uid", "17802525", nullptr);
  EXPECT_TRUE(s.ok());

  std::vector<blackwidow::FieldValue> fvs;
  s = redis->HGetAll(key, &fvs);
  EXPECT_TRUE(s.ok());

  for (auto& fv : fvs) {
    std::cout << "field:" << fv.field << ", value:" << fv.value << std::endl;
  }

  // HDEL
  int32_t deleted = 0;
  std::vector<std::string> fields;
  fields.push_back("uid");
  s = redis->HDel(key, fields, &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, deleted);
  
  // GetAll after hdel
  s = redis->HGetAll(key, &fvs);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(0, fvs.size());
}

TEST(TestHVals, RedisHashesTest) {
  blackwidow::RedisHashes* redis = nullptr;

  testing::Defer df([&]() {
    if (redis != nullptr)
      delete redis;
    system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisHashes(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::string key = "USER_INFO_17802525";
  s = redis->HSet(key, "uid", "17802525", nullptr);
  EXPECT_TRUE(s.ok());

  std::vector<std::string> vals;
  s = redis->HVals(key, &vals);
  EXPECT_TRUE(s.ok());

  for (auto& fv : vals) {
    std::cout << fv << std::endl;
  }
}

#define NO_EXPIRE  (-1)
#define KEY_ABSENT (-2)
TEST(TestExpireAndTTL, RedisHashesTest) {
  blackwidow::RedisHashes* redis = nullptr;

  testing::Defer df([&]() {
    if (redis != nullptr)
      delete redis;
    system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisHashes(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  int64_t ttl = -999;
  int64_t unix_time_now;

  s = redis->TTL("tencent_games", &ttl);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(ttl, KEY_ABSENT);

  s = redis->HSet("tencent_games", "crossfire", "1000", nullptr);
  EXPECT_TRUE(s.ok());

  s = redis->TTL("tencent_games", &ttl);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ttl, NO_EXPIRE);

  s = redis->Expire("tencent_games", 100);
  EXPECT_TRUE(s.ok());

  s = redis->TTL("tencent_games", &ttl);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ttl, 100);

  rocksdb::Env::Default()->GetCurrentTime(&unix_time_now);
  s = redis->ExpireAt("tencent_games", unix_time_now + 88);
  EXPECT_TRUE(s.ok());

   s = redis->TTL("tencent_games", &ttl);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ttl, 88);

}
#undef NO_EXPIRE
#undef KEY_ABSENT

TEST(TestHSet_V2, RedisHashesTest) {
  blackwidow::RedisHashes* redis = nullptr;

  testing::Defer df([&]() {
    if (redis != nullptr)
      delete redis;
    system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisHashes(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  opts.share_block_cache = false;
  opts.block_cache_size = 100 * 1024 * 1024;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::string hash_key = "TencentGames";
  std::vector<blackwidow::FieldValue> fvs;
  std::int32_t newfield_added = -1;
  std::uint32_t hash_size = -1;
  std::string field_value;

  s = redis->HGetAll(hash_key, &fvs);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_TRUE(fvs.empty());

  s = redis->HSet(hash_key, "英雄联盟", "10", &newfield_added);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, newfield_added);

  s = redis->HSet(hash_key, "穿越火线", "9", &newfield_added);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, newfield_added);

  s = redis->HSet(hash_key, "王者荣耀", "8", &newfield_added);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, newfield_added);

  s = redis->HSet(hash_key, "刺激战场", "7", &newfield_added);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, newfield_added);

  s = redis->HLen(hash_key, &hash_size);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(4, hash_size);

  s = redis->HGetAll(hash_key, &fvs);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs.size(), 4);

  std::vector<std::string> game_names;
  std::transform(fvs.begin(),
                 fvs.end(),
                 std::back_inserter(game_names),
                 [](const blackwidow::FieldValue& fv) { return fv.field; });

  EXPECT_TRUE(std::find(game_names.begin(), game_names.end(), "穿越火线") != game_names.end());
  EXPECT_TRUE(std::find(game_names.begin(), game_names.end(), "英雄联盟") != game_names.end());
  EXPECT_TRUE(std::find(game_names.begin(), game_names.end(), "王者荣耀") != game_names.end());
  EXPECT_TRUE(std::find(game_names.begin(), game_names.end(), "刺激战场") != game_names.end());
  EXPECT_TRUE(std::find(game_names.begin(), game_names.end(), "CSGO") == game_names.end());

  std::unordered_map<std::string, std::string> game_score_map;
  for(const auto &fv : fvs) {
    game_score_map[fv.field] = fv.value;
    std::cout << "field:" << fv.field << ", value:" << fv.value << std::endl;
  }

  EXPECT_EQ(game_score_map["英雄联盟"], "10");
  EXPECT_EQ(game_score_map["穿越火线"], "9");
  EXPECT_EQ(game_score_map["王者荣耀"], "8");
  EXPECT_EQ(game_score_map["刺激战场"], "7");

  // hset a existing field
  s = redis->HSet(hash_key, "刺激战场", "777", &newfield_added);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, newfield_added);


  s = redis->HGet(hash_key, "刺激战场", &field_value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ("777", field_value);

   s = redis->HGetAll(hash_key, &fvs);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(fvs.size(), 4);
   for(const auto &fv : fvs) {
    game_score_map[fv.field] = fv.value;
    std::cout << "field:" << fv.field << ", value:" << fv.value << std::endl;
  }


  // Delete All fields.
  int32_t deleted = -1;
  s = redis->HDel(hash_key, game_names, &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(4, deleted);

  uint32_t hlen = -999;
  s = redis->HLen(hash_key, &hlen);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(0, hlen);
}

#define BT_BUF_SIZE 100
void signal_handler(int signo) {
  std::cout << "SIGNO:" << signo << std::endl;

  int j, nptrs;
  void* buffer[BT_BUF_SIZE];
  char** strings;

  nptrs = backtrace(buffer, BT_BUF_SIZE);
  printf("backtrace() returned %d addresses\n", nptrs);

  /* The call backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO)
     would produce similar output to the following: */

  strings = backtrace_symbols(buffer, nptrs);
  if (strings == NULL) {
    perror("backtrace_symbols");
    exit(EXIT_FAILURE);
  }

  for (j = 0; j < nptrs; j++)
    printf("%s\n", strings[j]);

  free(strings);

  exit(1);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  signal(SIGSEGV, signal_handler);
  signal(SIGABRT, signal_handler);
  return RUN_ALL_TESTS();
}
