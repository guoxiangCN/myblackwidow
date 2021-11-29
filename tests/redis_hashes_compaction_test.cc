#include "redis_hashes.h"
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "testing_util.h"

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
  opts.options.write_buffer_size = 4 * 1024 * 1024;  // aka 4MB
  opts.options.level0_file_num_compaction_trigger = 2;
  opts.options.level0_slowdown_writes_trigger = 8;
  opts.options.level0_stop_writes_trigger = 12;
  opts.options.max_open_files = 300000;

  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  char keybuf[1024] = {0};
  char valbuf[1024] = {0};

  for (auto i = 0; i < 500000; i++) {
    snprintf(keybuf, 1024, "FIELD_QWERTYUIOPXYZ_%d", i);
    snprintf(valbuf, 1024, "VALUE_%d_asdfqweresdfsfsdfdsfdsf!Dchouzishu qsdfXas1@#43434", i);
    s = redis->HSet("USER", keybuf, valbuf);
    EXPECT_TRUE(s.ok());
  }

  s = redis->Expire("USER", 3);
  EXPECT_TRUE(s.ok());

  std::this_thread::sleep_for(4s);

  for (auto x = 0; i < 2;i++) {
    redis->CompactRange(nullptr, nullptr);
    std::this_thread::sleep_for(std::chrono::seconds(60));
  }
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
