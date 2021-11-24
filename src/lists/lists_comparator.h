#pragma once


#include "coding.h"
#include <rocksdb/status.h>
#include <rocksdb/comparator.h>

namespace blackwidow {

class ListDataKeyComparatorImpl : public rocksdb::Comparator {
public:
    ListDataKeyComparatorImpl() {}

    const char* Name() const override {
      return "blackwidow.ListDataKeyComparator";
    }

  

};

}    // namespace blackwidow