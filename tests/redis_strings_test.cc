#include "redis_strings.h"
#include <iostream>


int main(int argc, char **argv) {
  blackwidow::BlackWidowOptions opts;
  opts.options.create_if_missing = true;
  blackwidow::RedisStrings redis(nullptr);
  auto status = redis.Open(opts, "./redis_strings_test_dbpath");
  if(status.ok()) {
    std::cout << "RedisStrings open ok." << std::endl;
  } else {
    std::cout << "RedisStrings open failed: " << status.ToString() << std::endl;
  }

 status = redis.Expire("test_string_redis_key", 24*60*60);
 std::cout << status.ToString() << std::endl;
 return 0;
}