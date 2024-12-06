#pragma once

#include <chrono>
#include <cstddef>

#include "constant.h"

using Duration = std::chrono::microseconds;

// common
constexpr size_t WRITE_BUF_SIZE = 4 * KB;
constexpr char TMP_FILE_PREFIX[] = "tmp_file";
constexpr size_t FILE_NUM = 10;
constexpr size_t ROUND = 10;
constexpr size_t WRITE_BUF_NUM_PER_ROUND = 100;

// blobfs
constexpr size_t BLOBFS_CACHE_SIZE_MB = 1024;
