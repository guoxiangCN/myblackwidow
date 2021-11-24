cmake_minimum_required(VERSION 3.16.3)

project("myblackwindow")

message(${CMAKE_CXX_COMPILER})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g -O0")

include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${PROJECT_SOURCE_DIR}/deps/googletest/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/deps/benchmark/include")
include_directories("${PROJECT_SOURCE_DIR}/deps/rocksdb/include")


# add_subdirectory(test)
# add_subdirectory(deps/gtest)
# add_subdirectory(deps/benchmark)
add_subdirectory(deps/rocksdb)
