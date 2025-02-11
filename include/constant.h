#pragma once

#include <cstddef>

namespace pulsedb {

constexpr size_t KB = (1 << 10);
constexpr size_t MB = (1 << 20);
constexpr size_t GB = (1 << 30);

constexpr size_t kHostReqRbCap = 512 * KB;
constexpr size_t kHostRespRbCap = 512 * KB;
constexpr size_t kDpuDmaTaskSlotNum = 256;
constexpr size_t kMsgQueueSize = 1024;

constexpr size_t kCommRbMetaSize = 128;

} // namespace pulsedb
