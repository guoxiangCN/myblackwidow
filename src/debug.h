#pragma once

#ifndef NDEBUG
#define Trace(M, ...) fprintf(stderr, "[Trace] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#define Debug(M, ...) fprintf(stderr, "[Debug] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define Trace(M, ...) {}
#define Debug(M, ...) {}
#endif  // NDEBUG