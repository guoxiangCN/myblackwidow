#include "redis_lists.h"
#include <iostream>
#include <thread>

int main(int argc, char **argv) {
  blackwidow::RedisLists redis(nullptr);

  uint64_t listlen;
  Status s = redis.LLen("runaway_uids", &listlen);
  if(s.IsOk() || s.IsNotFound()) {
    std::cout << "list length:" << listlen << std::endl;
  }

  return 0;
}
