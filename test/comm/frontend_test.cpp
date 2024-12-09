#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "config.h"

using namespace pulsedb;

constexpr size_t OP_NUM = 100;

int main() {
    Frontend frontend(BACKEND_NAME, HOST_DEV_PCI, 1);
    auto &qp = frontend.qp_ref(0);
    std::vector<size_t> file_ids;
    for (size_t i = 0; i < OP_NUM; ++i) {
        qp.post_open(std::to_string(i), 0, [&](int32_t error_code, size_t file_id) {
            if (error_code != 0) {
                throw_with("unexpected error");
            }
            file_ids.emplace_back(file_id);
        });
    }
    qp.poll_wait();
    std::sort(file_ids.begin(), file_ids.end());
    for (size_t i = 0; i < OP_NUM; ++i) {
        if (file_ids[i] != i) {
            throw_with("file_id[={}] != {}", file_ids[i], i);
        }
    }
    for (size_t i = 0; i < OP_NUM; ++i) {
        qp.post_close(i, [&](int32_t error_code) {
            if (error_code != 0) {
                throw_with("req error");
            }
        });
    }
    qp.poll_wait();
    std::cout << "test pass" << std::endl;
    return 0;
}
