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

  s = redis->HSet("USER_17802525", "Name", "喜欢RAP和篮球");
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
  s = redis->HSet(key, "uid", "17802525");
  EXPECT_TRUE(s.ok());

  std::vector<blackwidow::FieldValue> fvs;
  s = redis->HGetAll(key, &fvs);
  EXPECT_TRUE(s.ok());

  for (auto& fv : fvs) {
    std::cout << "field:" << fv.field << ", value:" << fv.value << std::endl;
  }
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
  s = redis->HSet(key, "uid", "17802525");
  EXPECT_TRUE(s.ok());

  std::vector<std::string> vals;
  s = redis->HVals(key, &vals);
  EXPECT_TRUE(s.ok());

  for (auto& fv : vals) {
    std::cout << fv << std::endl;
  }
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
