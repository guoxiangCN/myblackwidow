#include "redis_lists.h"
#include <iostream>
#include <thread>

#include "unistd.h"
#include "gtest/gtest.h"
#include "testing_util.h"

namespace {
  static std::string kTestingPath = "./testdb_lists";
  static const char* kCmdDeleteTestingPath = "rm -rf ./testdb_lists";
  }  // namespace

TEST(TestLPushX, RedisListsTest) {

  testing::Defer df([]() {
    std::cout << "Trying to ::remove()" << std::endl;
    ::system(kCmdDeleteTestingPath);
  });

  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;

  blackwidow::RedisLists* redis = new blackwidow::RedisLists(nullptr);
  testing::Defer df2([&]() {
    std::cout << "Trying to delete redis" << std::endl;
    delete redis;
  });

  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  uint64_t listlen;
  std::string strval;
  s = redis->LLen("test_lpushx", &listlen);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(listlen, 0);

  s = redis->LPushX("test_lpushx", "17802525", &listlen);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(listlen, 0);
}

TEST(TestRPushX, RedisListsTest) {
   testing::Defer df([]() {
    std::cout << "Trying to ::remove()" << std::endl;
    ::system(kCmdDeleteTestingPath);
  });
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;

  blackwidow::RedisLists *redis = new blackwidow::RedisLists(nullptr);
   testing::Defer df2([&]() {
    std::cout << "Trying to delete redis" << std::endl;
    delete redis;
  });

  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::string strval;
  std::uint64_t listlen;

  s = redis->RPushX("test_rpushx", "1234567", &listlen);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(listlen, 0);
}

TEST(TestLPush, RedisListsTest) {
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;

  blackwidow::RedisLists *redis = new blackwidow::RedisLists(nullptr);
   testing::Defer df2([&]() {
    std::cout << "*****************************" << std::endl;
    delete redis;
    ::system(kCmdDeleteTestingPath);
    std::cout << "*****************************" << std::endl;
  });

  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::string strval;
  std::uint64_t listlen;

  std::vector<std::string> vec;
  vec.push_back("zhangsan");
  vec.push_back("lisi");
  vec.push_back("wangwu");
  vec.push_back("oouyangcheng");
  vec.push_back("guowei");
  vec.push_back("guowei");

  s = redis->LPush("test_lpush_QAQ", vec, &listlen);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(listlen, vec.size());

  s = redis->LLen("test_lpush_QAQ", &listlen);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(listlen, vec.size());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
