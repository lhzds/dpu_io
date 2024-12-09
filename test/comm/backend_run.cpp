#include "comm/backend.h"
#include "config.h"

using namespace pulsedb;

int main() {
    Backend backend(BACKEND_NAME, DPU_DEV_PCI, DPU_DEV_REP_PCI);
    backend.run();
    return 0;
}
