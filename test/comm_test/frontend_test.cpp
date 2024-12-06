#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "config.h"

using namespace pulsedb;

int main() {
    Frontend frontend(BACKEND_NAME, HOST_DEV_PCI, 0);
    return 0;
}