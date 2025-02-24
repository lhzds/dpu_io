#include <cstdlib>

#include "bytes.h"
#include "gtest/gtest.h"
#include "ring_buffer.h"

constexpr size_t kRingBufferCap{1 << 8};

using namespace pulsedb;

TEST(RingBufferTest, RepTest1) {
    RingBufferRep rep(kRingBufferCap);
    size_t expected_size{};

    auto slice = rep.write(kRingBufferCap / 2);
    expected_size += kRingBufferCap / 2;
    ASSERT_TRUE(slice && (slice->size() == expected_size));
    ASSERT_EQ(rep.tail(), kRingBufferCap / 2);
    ASSERT_EQ(rep.size(), slice->size());
    ASSERT_EQ(rep.size(), expected_size);
    
    slice = rep.read(kRingBufferCap / 4);
    expected_size -= kRingBufferCap / 4;
    ASSERT_TRUE(slice && (slice->size() == expected_size));
    ASSERT_EQ(rep.head(), kRingBufferCap / 4);
    ASSERT_EQ(rep.size(), slice->size());
    ASSERT_EQ(rep.size(), expected_size);
}

TEST(RingBufferTest, RepTest2) {
    RingBufferRep rep(kRingBufferCap);
    size_t expected_size{};

    for (size_t i{}; i < (1 << 10); ++i) {
        auto old_size = rep.size();
        auto slice = rep.write(kRingBufferCap / 4);
        if (not slice) {
            rep.read(expected_size);
            ASSERT_TRUE(rep.empty());
            expected_size = 0;
            continue;
        }
        expected_size += kRingBufferCap / 4;
        rep.read(kRingBufferCap / 8);
        expected_size -= kRingBufferCap / 8;
        ASSERT_EQ(rep.size(), expected_size);
        ASSERT_EQ(rep.size(), old_size + kRingBufferCap / 8);
    }
}

TEST(RingBufferTest, RbTest1) {
    RingBuffer rb(kRingBufferCap);
    
    rb.rep().write(kRingBufferCap / 2 + 2);
    rb.rep().read(kRingBufferCap / 2 + 2);

    for (size_t i{}; i < kRingBufferCap / sizeof(size_t) - 1; ++i) {
        rb.write({(char *)&i, sizeof(size_t)});
    }
    auto full_view = rb.to_view();
    for (size_t i{}; i < kRingBufferCap / sizeof(size_t) - 1; ++i) {
        auto buf = full_view.cut(sizeof(size_t)).merge();
        auto *num = (size_t *)buf.data();
        ASSERT_EQ(i, *num);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}
