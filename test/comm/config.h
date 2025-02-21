#pragma once

#include <string>

constexpr size_t KB = (1 << 10);
constexpr size_t MB = (1 << 20);
constexpr size_t GB = (1 << 30);

constexpr char kBackendName[]{"backend"};
constexpr char kHostDevPci[]{"b1:00.1"};
constexpr char kDpuDevPci[]{"03:00.1"};
constexpr char kDpuDevRepPci[]{"b1:00.1"};

// 容量设置较小，用于测试空间复用场景
constexpr size_t kHostReqRbCap{32 * MB};
constexpr size_t kHostRespRbCap{64};
constexpr size_t kDpuReqRbCap{kHostReqRbCap};
constexpr size_t kDpuRespRbCap{kHostRespRbCap};

const std::string kHostSstPoolDir{"/home/olh/mnt/local/nvme3n1"};
const std::string kDpuSstPoolDir{"/home/olh/mnt/57-nvme3n1"};
constexpr size_t kSstBasePoolSize{10};
constexpr size_t kSstFileBaseSize{64 * MB};
constexpr size_t kUringQueueDepth{512};
