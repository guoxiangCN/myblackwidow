# testcases
add_executable(redis_strings_test ./redis_strings_test.cc)
target_link_libraries(redis_strings_test myblackwidow)

add_executable(redis_lists_test ./redis_lists_test.cc)
target_link_libraries(redis_lists_test myblackwidow)