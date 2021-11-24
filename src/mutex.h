#pragma once

#include <memory>
#include <rocksdb/status.h>

namespace blackwindow {

using Status = rocksdb::Status;

class Mutex {
public:
 virtual ~Mutex() {}
 virtual Status Lock() = 0;
 virtual Status TryLockFor(int64_t timeoutMircro);
 virtual void UnLock() = 0;
};

class CondVar {
public:
    virtual ~CondVar() {}

    virtual Status Wait(std::shared_ptr<Mutex> mutex) = 0;
    virtual Status WaitFor(std::shared_ptr<Mutex> mutex,
                         int64_t timeout_time) = 0;

    virtual void Notify() = 0;
    virtual void NotifyAll() = 0;
};

class MutexFactory {
public:
  // Create a Mutex object.
  virtual std::shared_ptr<Mutex> AllocateMutex() = 0;

  // Create a CondVar object.
  virtual std::shared_ptr<CondVar> AllocateCondVar() = 0;

  virtual ~MutexFactory() {}
};

} // namespace blackwindow
