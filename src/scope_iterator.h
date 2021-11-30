#pragma once

#include "rocksdb/iterator.h"

namespace blackwidow {

class ScopeIterator {
public:
    ScopeIterator(rocksdb::Iterator **it)
        : it_(it){
    }

    ~ScopeIterator() {
      delete (*it_);
    }

private:
 rocksdb::Iterator** it_;
};

 }  // namespace blackwidow