#include "redis_strings.h"
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "testing_util.h"

namespace {
  static std::string kTestingPath = "./testdb_strings";
  static const char* kCmdDeleteTestingPath = "rm -rf ./testdb_strings";
  }  // namespace

TEST(TestSetAndGet, RedisStringsTest) {
  blackwidow::RedisStrings* redis = nullptr;

  testing::Defer df2([&]() {
    if (redis != nullptr)
      delete redis;
    ::system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisStrings(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  s = redis->Set("name", "guoxiangCN");
  EXPECT_TRUE(s.ok());
  s = redis->Set("age", "24");
  EXPECT_TRUE(s.ok());
  s = redis->Set("address", "ChinaShenzhen");
  EXPECT_TRUE(s.ok());
  s = redis->Set("skills", "c++/java/golang");
  EXPECT_TRUE(s.ok());

  std::string value;
  
  s = redis->Get("name", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "guoxiangCN");

  s = redis->Get("age", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "24");

  s = redis->Get("address", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "ChinaShenzhen");

  s = redis->Get("skills", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "c++/java/golang");
}

TEST(TestMSetAndGet, RedisStringsTest) {
  blackwidow::RedisStrings* redis = nullptr;

  testing::Defer df2([&]() {
    if (redis != nullptr)
      delete redis;
    ::system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisStrings(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::vector<blackwidow::KeyValue> kvs;
  kvs.emplace_back("guangzhou_1","Gzcb");
  kvs.emplace_back("guangzhou_2","Joyy");
  kvs.emplace_back("shenzhen_1","AntGroup");
  kvs.emplace_back("shenzhen_2","????");

  s = redis->MSet(kvs);
  EXPECT_TRUE(s.ok());

  std::string value;
  
  s = redis->Get("guangzhou_1", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "Gzcb");

  s = redis->Get("guangzhou_2", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "Joyy");

  s = redis->Get("shenzhen_1", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "AntGroup");

  s = redis->Get("shenzhen_2", &value);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(value, "????");

  s = redis->Get("shenzhen_3", &value);
  EXPECT_TRUE(s.IsNotFound());
}

TEST(TestStrlen, RedisStringsTest) {
  blackwidow::RedisStrings* redis = nullptr;

  testing::Defer df2([&]() {
    if (redis != nullptr)
      delete redis;
    ::system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisStrings(nullptr);
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  std::vector<blackwidow::KeyValue> kvs;
  kvs.emplace_back("key_0","");
  kvs.emplace_back("key_1","1");
  kvs.emplace_back("key_2","AB");
  kvs.emplace_back("key_3","CDE");
  kvs.emplace_back("key_8","12345678");
  kvs.emplace_back("key_26","ABCDEFGHIJKLMNOPQRSTUVWXYZ");
  kvs.emplace_back("key_299",std::string(299, '&'));
  kvs.emplace_back("key_1000",std::string(1000, 'c'));

  s = redis->MSet(kvs);
  EXPECT_TRUE(s.ok());

  uint64_t length;
  for(const auto &kv : kvs) {
    s = redis->Strlen(kv.key, &length);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(length, kv.value.length());
  }

  s = redis->Strlen("key_not_exists", &length);
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(0, length);
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}