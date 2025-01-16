#pragma once

#include "constant.h"
#include <cstddef>

constexpr char SERVER_NAME[]{"server"};
constexpr char HOST_DEV_PCI[]{"b1:00.0"};
constexpr char DPU_DEV_PCI[]{"03:00.0"};
constexpr char DPU_DEV_REP_PCI[]{"b1:00.0"};
constexpr size_t DMA_BUF_MAX_SIZE{128 * pulsedb::KB};

struct InitMsgHeader {
    char *remote_mem_addr;
    size_t remote_mem_size;
    size_t remote_mmap_desc_size;
};
