#include "redis_strings.h"
#include <iostream>
#include <thread>

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


 int64_t ttl;
 std::string value;
 redis.Set("name", "guoxaing");
 redis.TTL("name", &ttl);
 std::cout << "ttl:" << ttl << std::endl;    // -1

 redis.Expire("name", 5);
 redis.TTL("name", &ttl);
 std::cout << "ttl:" << ttl << std::endl;    // 5
 redis.Get("name", &value);
 std::cout << "name = "<< value << std::endl;

 std::this_thread::sleep_for(std::chrono::seconds(6));
 redis.Get("name", &value);
 std::cout << "name = "<< value << std::endl;


 return 0;
}