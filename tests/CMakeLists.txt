# testcases
add_executable(redis_strings_test ./redis_strings_test.cc)
target_link_libraries(redis_strings_test myblackwidow gtest)

add_executable(redis_strings_compaction_test ./redis_strings_compaction_test.cc)
target_link_libraries(redis_strings_compaction_test myblackwidow gtest)

add_executable(redis_lists_test ./redis_lists_test.cc)
target_link_libraries(redis_lists_test myblackwidow gtest)

add_executable(redis_hashes_test ./redis_hashes_test.cc)
target_link_libraries(redis_hashes_test myblackwidow gtest)

add_executable(redis_hashes_compaction_test ./redis_hashes_compaction_test.cc)
target_link_libraries(redis_hashes_compaction_test myblackwidow gtest)

add_executable(redis_zsets_test ./redis_zsets_test.cc)
target_link_libraries(redis_zsets_test myblackwidow gtest)