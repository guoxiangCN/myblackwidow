cmake_minimum_required(VERSION 3.16.3)

project("myblackwidow")

message(${CMAKE_CXX_COMPILER})

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
    message (STATUS "No CMAKE_BUILD_TYPE selected, defaulting to ${CMAKE_BUILD_TYPE}")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -g")

#
include(CheckFunctionExists)
include(CheckLibraryExists)
include(CheckSymbolExists)
include(CheckCXXSymbolExists)
include(CheckSymbolExists)

# config version.h.in
set(BLACKWIDOW_MAJOR 1)
set(BLACKWIDOW_MINOR 0)
set(BLACKWIDOW_PATCH 0)
configure_file("${PROJECT_SOURCE_DIR}/include/blackwidow/version.h.in"
               "${PROJECT_SOURCE_DIR}/include/blackwidow/version.h")


# config port_config.h.in
check_function_exists(epoll_wait HAVE_EPOLL)
check_function_exists(poll HAVE_POLL)
check_function_exists(select HAVE_SELECT)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)
configure_file("${PROJECT_SOURCE_DIR}/include/blackwidow/port_config.h.in"
               "${PROJECT_SOURCE_DIR}/include/blackwidow/port_config.h")


# include headers
include_directories("${PROJECT_SOURCE_DIR}/include")
include_directories("${PROJECT_SOURCE_DIR}/tests")
include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${PROJECT_SOURCE_DIR}/src/strings")
include_directories("${PROJECT_SOURCE_DIR}/src/lists")
include_directories("${PROJECT_SOURCE_DIR}/src/hashes")
include_directories("${PROJECT_SOURCE_DIR}/src/sets")
include_directories("${PROJECT_SOURCE_DIR}/src/zsets")
include_directories("${PROJECT_SOURCE_DIR}/deps/gtest/googletest/include")
include_directories("${PROJECT_SOURCE_DIR}/deps/benchmark/include")
include_directories("${PROJECT_SOURCE_DIR}/deps/rocksdb/include")

set(WITH_TESTS OFF)
set(WITH_BENCHMARK_TOOLS OFF)

add_subdirectory(tests)
add_subdirectory(deps/gtest)
add_subdirectory(deps/rocksdb EXCLUDE_FROM_ALL)

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
    src/hashes/redis_hashes.cc
    src/zsets/redis_zsets.cc
)
target_link_libraries(myblackwidow rocksdb)
