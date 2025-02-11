#pragma once

#include "constant.h"
#include <cstddef>

constexpr char kServerName[]{"server"};
constexpr char kHostDevPci[]{"b1:00.0"};
constexpr char kDpuDevPci[]{"03:00.0"};
constexpr char kDpuDevRepPci[]{"b1:00.0"};
constexpr size_t DMA_BUF_MAX_SIZE{128 * pulsedb::KB};

struct InitMsgHeader {
    char *remote_mem_addr;
    size_t remote_mem_size;
    size_t remote_mmap_desc_size;
};
