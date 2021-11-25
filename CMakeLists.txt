cmake_minimum_required(VERSION 3.16.3)

project("myblackwidow")

message(${CMAKE_CXX_COMPILER})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -O0")

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${PROJECT_SOURCE_DIR}/src/strings")
include_directories("${PROJECT_SOURCE_DIR}/src/lists")

include_directories("${PROJECT_SOURCE_DIR}/deps/googletest/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/deps/benchmark/include")
include_directories("${PROJECT_SOURCE_DIR}/deps/rocksdb/include")


add_subdirectory(tests)
add_subdirectory(deps/rocksdb)


add_library(myblackwidow STATIC 
    src/util.cc
    src/redis.cc
    src/mutex_impl.cc 
    src/blackwidow.cc 
    src/murmurhash.cc
    src/lock_mgr.cc
    src/build_version.cc 
    src/strings/redis_strings.cc
    src/lists/redis_lists.cc
)
target_link_libraries(myblackwidow rocksdb)
