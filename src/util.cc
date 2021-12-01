#include "blackwidow/util.h"
#include <string>
#include <stdio.h>

namespace blackwidow {

/* Glob-style pattern matching. */
int StringMatch(const char *pattern, int pattern_len, const char* str,
                int string_len, int nocase) {
  while (pattern_len) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          pattern_len--;
        }
        if (pattern_len == 1)
          return 1; /* match */
        while (string_len) {
          if (StringMatch(pattern+1, pattern_len-1,
                          str, string_len, nocase))
            return 1; /* match */
          str++;
          string_len--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (string_len == 0)
          return 0; /* no match */
        str++;
        string_len--;
        break;
      case '[':
        {
        int not_flag, match;
        pattern++;
        pattern_len--;
        not_flag = pattern[0] == '^';
        if (not_flag) {
          pattern++;
          pattern_len--;
        }
        match = 0;
        while (1) {
          if (pattern[0] == '\\') {
            pattern++;
            pattern_len--;
            if (pattern[0] == str[0])
              match = 1;
            } else if (pattern[0] == ']') {
              break;
            } else if (pattern_len == 0) {
              pattern--;
              pattern_len++;
              break;
            } else if (pattern[1] == '-' && pattern_len >= 3) {
              int start = pattern[0];
              int end = pattern[2];
              int c = str[0];
              if (start > end) {
                int t = start;
                start = end;
                end = t;
              }
              if (nocase) {
                start = tolower(start);
                end = tolower(end);
                c = tolower(c);
              }
              pattern += 2;
              pattern_len -= 2;
              if (c >= start && c <= end)
                match = 1;
              } else {
                if (!nocase) {
                  if (pattern[0] == str[0])
                    match = 1;
                } else {
                  if (tolower(static_cast<int>(pattern[0])) ==
                      tolower(static_cast<int>(str[0])))
                    match = 1;
                }
              }
              pattern++;
              pattern_len--;
            }
            if (not_flag)
                match = !match;
            if (!match)
                return 0; /* no match */
            str++;
            string_len--;
            break;
        }
      case '\\':
        if (pattern_len >= 2) {
          pattern++;
          pattern_len--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != str[0])
            return 0; /* no match */
        } else {
          if (tolower(static_cast<int>(pattern[0])) !=
              tolower(static_cast<int>(str[0])))
            return 0; /* no match */
        }
        str++;
        string_len--;
        break;
      }
      pattern++;
      pattern_len--;
      if (string_len == 0) {
        while (*pattern == '*') {
          pattern++;
          pattern_len--;
        }
        break;
      }
    }
    if (pattern_len == 0 && string_len == 0)
      return 1;
    return 0;
}


}
