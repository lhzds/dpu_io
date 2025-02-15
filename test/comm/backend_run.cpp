#include "comm/backend.h"
#include "config.h"
#include "io/sst_pool.h"
#include "io/uring_ctx.h"

using namespace pulsedb;

int main() {
    auto sst_pool = std::make_unique<SstPool>(kSstDir, kBasePoolSize, 0, 0, kFileSizeBase, 0);
    auto uring_ctx = std::make_unique<UringCtx>(kUringQueueDepth, std::move(sst_pool));
    Backend backend(kBackendName, kDpuDevPci, kDpuDevRepPci,
        std::move(uring_ctx), kDpuReqRbCap, kDpuRespRbCap);
    backend.run();
    return 0;
}
