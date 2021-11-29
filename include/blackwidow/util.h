#pragma once

#include <cstdint>
#include <sys/time.h>

namespace blackwidow {

static inline uint64_t  NsSinceEpoch() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return tv.tv_usec * 1000 + tv.tv_sec * 1000 * 1000 * 1000;
}




}