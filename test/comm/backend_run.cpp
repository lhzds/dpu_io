#include "comm/backend.h"
#include "config.h"

using namespace pulsedb;

int main() {
    Backend backend(kBackendName, kDpuDevPci, kDpuDevRepPci);
    backend.run();
    return 0;
}
