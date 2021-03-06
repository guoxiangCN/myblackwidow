//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "blackwidow/build_version.h"
const char* blackwidow_build_git_sha = "blackwidow_build_git_sha:@@GIT_SHA@@";
const char* blackwidow_build_git_date = "blackwidow_build_git_date:@@GIT_DATE_TIME@@";
const char* blackwidow_build_compile_date = __DATE__;


// 需要设置cmake生成.cc.in 这里暂时不搞