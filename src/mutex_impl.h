#pragma once

#include "mutex.h"
#include <memory>

namespace blackwidow {

// Default implementation of MutexFactory.
class MutexFactoryImpl : public MutexFactory {
 public:
  std::shared_ptr<Mutex> AllocateMutex() override;
  std::shared_ptr<CondVar> AllocateCondVar() override;
};

}  //  namespace blackwidow