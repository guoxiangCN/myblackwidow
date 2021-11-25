#pragma once

#include <functional>

namespace testing {

class Defer final {
 public:
  Defer() {}

  template <typename F, typename... Args>
#if __cplusplus >= 201703L
  [[nodiscard("WARNING: Discard me will trigger destructor ASAP!!!")]]
#endif
  Defer(F&& fn, Args&&... args)
    : f_(std::bind(std::forward<F>(fn), std::forward<Args>(args)...)) {
  }

  ~Defer() {
    if (f_) {
      f_();
    }
  }

 private:
  std::function<void()> f_;
};

}
