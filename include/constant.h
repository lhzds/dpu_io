#pragma once

#include <cstddef>

constexpr size_t KB = (1 << 10);
constexpr size_t MB = (1 << 20);
constexpr size_t GB = (1 << 30);

constexpr size_t HOST_REQ_RB_CAP = 512 * KB;
constexpr size_t HOST_RESP_RB_CAP = 512 * KB;
constexpr size_t DPU_DMA_TASK_SLOT_NUM = 256;
constexpr size_t MSG_QUEUE_SIZE = 1024;
