#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "config.h"
#include "gtest/gtest.h"
#include <cstring>
#include <fcntl.h>

using namespace pulsedb;

constexpr size_t OP_NUM = 100;

// TODO: 关闭日志功能，进行性能测试

class FrontendTest: public testing::Test {
public:
    static void SetUpTestSuite() {}
    static void TearDownTestSuite() {}

    virtual void SetUp() override {
        frontend = std::make_unique<Frontend>(kBackendName, kHostDevPci, 1, kHostReqRbCap, kHostRespRbCap);
    }

    std::unique_ptr<Frontend> frontend;
};

TEST_F(FrontendTest, SimpleWrite) {
    auto *qp = frontend->alloc_qp();
        
    qp->prep_write(1, 0);
    std::string target = "Hello, world!";
    qp->append(target);
    qp->post_write();
    qp->post_sync_close(1, false);
    qp->wait_all();

    std::string slot_fname = kHostSstPoolDir + "/0";
    int fd = open(slot_fname.c_str(), O_DIRECT | O_RDONLY);
    if (fd < 0) {
        throw_with("Failed to open {}: {}", slot_fname, strerror(errno));
    } 

    std::string test;
    test.resize(target.size());
    if (auto ret = read(fd, test.data(), test.size()); ret < 0) {
        throw_with("Failed to read: {}", strerror(errno));
    }

    EXPECT_EQ(test, target);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}