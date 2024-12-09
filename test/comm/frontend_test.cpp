#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "config.h"

using namespace pulsedb;

int main() {
    Frontend frontend(BACKEND_NAME, HOST_DEV_PCI, 1);
    auto &qp = frontend.qp_ref(0);
    size_t file_id;
    qp.post_open("filename", 0, [&](int32_t error_code, size_t fid) {
        file_id = fid;
    });
    qp.poll_wait();
    
    return 0;
}