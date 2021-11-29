#include "redis_strings.h"
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "testing_util.h"

namespace {
static std::string kTestingPath = "./testdb_strings_compaction";
static const char* kCmdDeleteTestingPath = "rm -rf ./testdb_strings_compaction";
}  // namespace

TEST(TestCompaction, StringCompactionTest) {
  blackwidow::RedisStrings* redis = nullptr;

  testing::Defer df2([&]() {
    if (redis != nullptr)
      delete redis;
    //system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisStrings(nullptr);

  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  opts.options.write_buffer_size = 16 * 1024 * 1024; // aka 16MB
  opts.options.level0_file_num_compaction_trigger = 2;
  opts.options.level0_slowdown_writes_trigger = 8;
  opts.options.level0_stop_writes_trigger = 12;
  opts.options.max_open_files = 300000;

  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  char keybuf[1024] = {0};
  std::string bigval =
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "ASDSDADDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDaaaaaaaaaaaaaaaaaawdsfaa1234"
    "faaaaaaaaaqqqqqqqqqqwwww"; /* 1kb */

  for (auto i = 0; i < 1024 * 1024; i++) {
    std::snprintf(keybuf, 1024, "%9d", i);
    s = redis->Set(keybuf, bigval);
    EXPECT_TRUE(s.ok());
  }

  for (auto i = 0; i < 1024 * 1024; i++) {
    std::snprintf(keybuf, 1024, "%9d", i);
    s = redis->Set(keybuf, "version2+sdfsdffsdfsf");
    EXPECT_TRUE(s.ok());
  }

  // for (auto i = 0; i < 1024 * 1024; i++) {
  //   std::snprintf(keybuf, 1024, "%9d", i);
  //   s = redis->Expire(keybuf, 3);
  //   EXPECT_TRUE(s.ok());
  // }
  while(1) {
    redis->CompactRange(nullptr, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(15));
  }
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}