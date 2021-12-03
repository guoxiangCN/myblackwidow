#include "gtest/gtest.h"
#include "redis_zsets.h"
#include "testing_util.h"
#include "unistd.h"
#include <execinfo.h>
#include <iostream>
#include <thread>

namespace {
static std::string kTestingPath = "./testdb_zsets";
static const char* kCmdDeleteTestingPath = "rm -rf ./testdb_zsets";
}  // namespace

TEST(TestZAdd, RedisZsetsTest) {
  blackwidow::RedisZsets* redis = nullptr;
  testing::Defer d([&]() {
    if (redis != nullptr) {
      delete redis;
    }
    system(kCmdDeleteTestingPath);
  });

  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  opts.options.error_if_exists = false;
  opts.options.write_buffer_size = 1 * 1024 * 1024;  // aka 4MB
  opts.options.level0_file_num_compaction_trigger = 2;
  opts.options.level0_slowdown_writes_trigger = 8;
  opts.options.level0_stop_writes_trigger = 12;
  opts.options.max_open_files = 300000;

  redis = new blackwidow::RedisZsets(nullptr);
  blackwidow::Status s = redis->Open(opts, kTestingPath);
  EXPECT_TRUE(s.ok());

  int32_t ret = -3;
  int32_t len = -3;
  int32_t rank = -3;
  double score = 0;
  std::vector<blackwidow::ScoreMember> sm;

  // ZAdd
  sm.push_back({131400.02, "17802525"});
  sm.push_back({5200.999, "17802530"});
  sm.push_back({9900.726, "17802331"});
  s = redis->ZAdd("monster_rank_20211203", sm, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, ret);

  // ZCard
  s = redis->ZCard("monster_rank_20211203", &len);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(3, len);

  // ZScore
  s = redis->ZScore("monster_rank_20211203", "17802525", &score);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(131400.02, score);
  s = redis->ZScore("monster_rank_20211203", "17802530", &score);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(5200.999, score);
  s = redis->ZScore("monster_rank_20211203", "17802331", &score);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(9900.726, score);
  s = redis->ZScore("monster_rank_20211203", "aabbccdd", &score);
  EXPECT_TRUE(s.IsNotFound());

  // ZRank from small to big
  s = redis->ZRank("monster_rank_20211203", "17802525", &rank);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(2, rank);

  s = redis->ZRank("monster_rank_20211203", "17802530", &rank);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(0, rank);
  
  s = redis->ZRank("monster_rank_20211203", "17802331", &rank);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(1, rank);
  
  s = redis->ZRank("monster_rank_20211203", "aabbccdd", &rank);
  EXPECT_TRUE(s.IsNotFound());
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
