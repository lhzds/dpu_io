#pragma once

#include <string>

constexpr size_t KiB = (1 << 10);
constexpr size_t MiB = (1 << 20);
constexpr size_t GiB = (1 << 30);

constexpr char kBackendName[]{"backend"};
constexpr char kHostDevPci[]{"b1:00.1"};
constexpr char kDpuDevPci[]{"03:00.1"};
constexpr char kDpuDevRepPci[]{"b1:00.1"};

// 容量设置较小，用于测试空间复用场景
constexpr size_t kHostReqRbCap{64 * MiB};
constexpr size_t kHostRespRbCap{1 * MiB};
constexpr size_t kDpuReqRbCap{kHostReqRbCap};
constexpr size_t kDpuRespRbCap{kHostRespRbCap};

const std::string kHostSstPoolDir{"/home/olh/mnt/local/sst_pool"};
const std::string kDpuSstPoolDir{"/home/olh/mnt/sst_pool"};
constexpr size_t kSstBasePoolSize{2000};
constexpr size_t kSstFileBaseSize{64 * MiB};
constexpr size_t kSstPadSize{1 * MiB};
constexpr size_t kUringQueueDepth{512};
