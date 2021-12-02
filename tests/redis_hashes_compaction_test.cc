#include "redis_hashes.h"
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "testing_util.h"
#include <chrono>


using namespace std::chrono_literals;
namespace {
static std::string kTestingPath = "./testdb_hashes_compaction";
static const char* kCmdDeleteTestingPath = "rm -rf ./testdb_hashes_compaction";
}  // namespace


TEST(TestCompaction, RedisHashesCompactionTest) {
  blackwidow::RedisHashes* redis = nullptr;

  testing::Defer df2([&]() {
    if (redis != nullptr)
      delete redis;
    system(kCmdDeleteTestingPath);
  });

  redis = new blackwidow::RedisHashes(nullptr);

  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  opts.options.write_buffer_size = 1 * 1024 * 1024;  // aka 4MB
  opts.options.level0_file_num_compaction_trigger = 2;
  opts.options.level0_slowdown_writes_trigger = 8;
  opts.options.level0_stop_writes_trigger = 12;
  opts.options.max_open_files = 300000;

  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  char keybuf[1024] = {0};
  char valbuf[1024] = {0};

#if 0
  for (auto i = 0; i < 1000000; i++) {
    snprintf(keybuf, 1024, "USER_%d", i);
    snprintf(valbuf,
             1024,
             "Name%d_ASDFGHJKLQWERTYUOPAASDDSDSD",
             i);
    s = redis->HSet("USERS_NAMES", keybuf, valbuf, nullptr);
    EXPECT_TRUE(s.ok());
  }
  s = redis->Expire("USERS_NAMES", 6);
  EXPECT_TRUE(s.ok());



  for (auto i = 0; i < 3000000; i++) {
    snprintf(keybuf, 1024, "GAME_%d", i);
    snprintf(valbuf,
             1024,
             "LOL%d_xxxx*********%%@!$#$#!@#@!#!@#!@#!@#)(*(DSA",
             i);
    s = redis->HSet("TENCENT_GAMES", keybuf, valbuf, nullptr);
    EXPECT_TRUE(s.ok());
  }
  s = redis->Expire("TENCENT_GAMES", 1);
  EXPECT_TRUE(s.ok());
#endif

  redis->CompactRange(nullptr, nullptr);

  for (;;) {
    std::cout << "NOP......" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
