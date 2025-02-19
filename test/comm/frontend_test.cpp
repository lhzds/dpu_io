#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "config.h"
#include "gtest/gtest.h"
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>

using namespace pulsedb;

constexpr size_t kOpNum = 100;

// TODO: 关闭日志功能，进行性能测试
// TEST(FrontendTest, SimpleWriteTest) {
//     auto frontend = std::make_unique<Frontend>(kBackendName, kHostDevPci, 1, kHostReqRbCap, kHostRespRbCap);
//     auto *qp = frontend->alloc_qp();

//     std::string target = "Hello, world!";
//     for (size_t file_num = 1; file_num <= kBasePoolSize; ++file_num) {
//         qp->prep_write(file_num, 0);
//         qp->append(target);
//         qp->post_write();
//         qp->post_sync_close(file_num, false);
//     }

//     auto slot_ids = qp->wait_all();

//     void *align_mem;
//     posix_memalign(&align_mem, 4096, 4096);
//     for (auto slot_id : slot_ids) {
//         std::string slot_fname = kHostSstPoolDir + "/" + std::to_string(slot_id);
//         int fd = open(slot_fname.c_str(), O_DIRECT | O_RDONLY);
//         if (fd < 0) {
//             throw_with("Failed to open {}: {}", slot_fname, strerror(errno));
//         }

//         if (auto ret = read(fd, align_mem, 4096); ret < 0) {
//             throw_with("Failed to read: {}", strerror(errno));
//         }
//         std::string_view test((char *)align_mem, target.size());
//         EXPECT_EQ(test, target);

//         close(fd);
//     }
//     free(align_mem);
// }

TEST(FrontendTest, WriteMoreTest) {
    size_t kPageSize = 1024 * 1024;
    auto frontend = std::make_unique<Frontend>(kBackendName, kHostDevPci, 1, kHostReqRbCap, kHostRespRbCap);
    auto *qp = frontend->alloc_qp();

    char ch = random();
    std::string target(kPageSize, ch);
    for (size_t file_num = 1; file_num <= 8; ++file_num) {
        for (size_t offset = 0; offset < kFileSizeBase; offset += kPageSize) {
            qp->prep_write(file_num, offset);
            qp->append(target);
            qp->post_write();
        }
        qp->post_sync_close(file_num, false);
    }

    auto slot_ids = qp->wait_all();

    void *align_mem;
    posix_memalign(&align_mem, 4096, kPageSize);
    for (auto slot_id : slot_ids) {
        std::string slot_fname = kHostSstPoolDir + "/" + std::to_string(slot_id);
        int fd = open(slot_fname.c_str(), O_DIRECT | O_RDONLY);
        if (fd < 0) {
            throw_with("Failed to open {}: {}", slot_fname, strerror(errno));
        }

        for (size_t offset = 0; offset < kFileSizeBase; offset += kPageSize) {
            int ret = read(fd, align_mem, kPageSize);
            if (ret < 0) {
                throw_with("Failed to read: {}", strerror(errno));
            }
            EXPECT_EQ(ret, kPageSize);
            std::string_view test((char *)align_mem, ret);
            EXPECT_EQ(test, target);
        }

        close(fd);
    }
    free(align_mem);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}