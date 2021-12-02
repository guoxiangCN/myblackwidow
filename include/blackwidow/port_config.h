#pragma once

#if !defined(HAVE_EPOLL)
#define HAVE_EPOLL 0
#endif

#if !defined(HAVE_POLL)
#define HAVE_POLL 1
#endif

#if !defined(HAVE_SELECT)
#define HAVE_SELECT 1
#endif

#if !defined(HAVE_O_CLOEXEC)
#define HAVE_O_CLOEXEC 1
#endif
