#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "config.h"
#include "gtest/gtest.h"
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <random>
#include <string>

// TODO: 关闭日志功能，进行性能测试

char random_ch() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist('a', 'z');
    return dist(gen);
}

// TEST(FrontendTest, SimpleWriteTest) {
//     size_t kFileCnt = 10;

//     auto frontend = std::make_unique<pulsedb::Frontend>(kBackendName, kHostDevPci, 1, kHostReqRbCap, kHostRespRbCap);
//     auto *qp = frontend->alloc_qp();

//     std::string target = "Hello, world!";
//     for (size_t file_num = 1; file_num <= kFileCnt; ++file_num) {
//         qp->prep_write(file_num, 0);
//         qp->append(target);
//         qp->post_write();
//         qp->post_sync_close(file_num, false);
//     }

//     auto slot_ids = qp->wait_all();

//     void *align_mem;
//     posix_memalign(&align_mem, 4 * KiB, 4 * KiB);
//     for (auto slot_id : slot_ids) {
//         std::string slot_fname = kHostSstPoolDir + "/" + std::to_string(slot_id);
//         int fd = open(slot_fname.c_str(), O_DIRECT | O_RDONLY);
//         if (fd < 0) {
//             pulsedb::throw_with("Failed to open {}: {}", slot_fname, strerror(errno));
//         }

//         if (auto ret = read(fd, align_mem, 4 * KiB); ret < 0) {
//             pulsedb::throw_with("Failed to read: {}", strerror(errno));
//         }
//         std::string_view test((char *)align_mem, target.size());
//         EXPECT_EQ(test, target);

//         close(fd);
//     }
//     free(align_mem);
// }

TEST(FrontendTest, WriteMoreTest) {
    size_t kFileCnt = 10;
    size_t kPageSize = std::min(48 * MiB, kHostReqRbCap);
    size_t kSingleWritePageCnt = std::min(size_t(2), kHostReqRbCap / kPageSize);

    auto frontend = std::make_unique<pulsedb::Frontend>(kBackendName, kHostDevPci, 1,
        kHostReqRbCap, kHostRespRbCap);
    auto *qp = frontend->alloc_qp();

    std::deque<char> target_chs;
    for (size_t file_num = 1; file_num <= kFileCnt; ++file_num) {
        for (size_t offset = 0; offset < kPageSize * kSingleWritePageCnt; offset += kPageSize) {
            char ch = random_ch();
            target_chs.push_back(ch);

            qp->prep_write(file_num, offset);
            qp->append(std::string(kPageSize, ch));
            qp->post_write();
        }
        qp->post_sync_close(file_num, false);
    }

    auto slot_ids = qp->wait_all();
    void *align_mem;
    posix_memalign(&align_mem, 4096, kPageSize);

    for (auto [_, slot_id] : slot_ids) {
        std::string slot_fname = kHostSstPoolDir + "/" + std::to_string(slot_id);
        int fd = open(slot_fname.c_str(), O_DIRECT | O_RDONLY);
        if (fd < 0) {
            pulsedb::throw_with("Failed to open {}: {}", slot_fname, strerror(errno));
        }

        for (size_t offset = 0; offset < kPageSize * kSingleWritePageCnt; offset += kPageSize) {
            int ret = read(fd, align_mem, kPageSize);
            if (ret < 0) {
                pulsedb::throw_with("Failed to read: {}", strerror(errno));
            }

            std::string_view test((char *)align_mem, ret);
            char ch = target_chs.front();
            target_chs.pop_front();
            EXPECT_EQ(ret, kPageSize);
            EXPECT_EQ(test, std::string(kPageSize, ch));
        }

        close(fd);
    }
    free(align_mem);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}