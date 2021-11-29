#include "redis_hashes.h"
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "testing_util.h"
#include "unistd.h"

namespace {
static std::string kTestingPath = "./testdb_lists";
static const char* kCmdDeleteTestingPath = "rm -rf ./testdb_lists";
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


s = redis->HExists("USER_17802525", "Name");
  EXPECT_TRUE(s.IsNotFound());
}


int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
