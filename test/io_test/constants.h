#pragma once

#include <chrono>
#include <cstddef>

#include "print_util.h"

using Duration = std::chrono::microseconds;

// common
constexpr size_t BUF_SIZE = 1 * KB;
constexpr size_t LIMIT_SIZE = 64 * MB;
constexpr char TMP_FILE_NAME[] = "tmp_file";
constexpr size_t ROUND = 5;

// blobfs
constexpr size_t BLOBFS_CACHE_SIZE_MB = 1024;
