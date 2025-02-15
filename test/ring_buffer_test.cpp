#include <cstdlib>

#include "bytes.h"
#include "gtest/gtest.h"
#include "ring_buffer.h"

constexpr size_t RING_BUFFER_CAP{1 << 8};

using namespace pulsedb;

TEST(RingBufferTest, RepTest1) {
    RingBufferRep rep(RING_BUFFER_CAP);
    size_t expected_size{};

    auto slice = rep.write(RING_BUFFER_CAP / 2);
    expected_size += RING_BUFFER_CAP / 2;
    ASSERT_TRUE(slice && (slice->size() == expected_size));
    ASSERT_EQ(rep.tail(), RING_BUFFER_CAP / 2);
    ASSERT_EQ(rep.size(), slice->size());
    ASSERT_EQ(rep.size(), expected_size);
    
    slice = rep.read(RING_BUFFER_CAP / 4);
    expected_size -= RING_BUFFER_CAP / 4;
    ASSERT_TRUE(slice && (slice->size() == expected_size));
    ASSERT_EQ(rep.head(), RING_BUFFER_CAP / 4);
    ASSERT_EQ(rep.size(), slice->size());
    ASSERT_EQ(rep.size(), expected_size);
}

TEST(RingBufferTest, RepTest2) {
    RingBufferRep rep(RING_BUFFER_CAP);
    size_t expected_size{};

    for (size_t i{}; i < (1 << 10); ++i) {
        auto old_size = rep.size();
        auto slice = rep.write(RING_BUFFER_CAP / 4);
        if (not slice) {
            rep.read(expected_size);
            ASSERT_TRUE(rep.empty());
            expected_size = 0;
            continue;
        }
        expected_size += RING_BUFFER_CAP / 4;
        rep.read(RING_BUFFER_CAP / 8);
        expected_size -= RING_BUFFER_CAP / 8;
        ASSERT_EQ(rep.size(), expected_size);
        ASSERT_EQ(rep.size(), old_size + RING_BUFFER_CAP / 8);
    }
}

TEST(RingBufferTest, RbTest1) {
    RingBuffer rb(RING_BUFFER_CAP);
    
    rb.rep().write(RING_BUFFER_CAP / 2 + 2);
    rb.rep().read(RING_BUFFER_CAP / 2 + 2);

    for (size_t i{}; i < RING_BUFFER_CAP / sizeof(size_t) - 1; ++i) {
        rb.write({(char *)&i, sizeof(size_t)});
    }
    auto full_view = rb.to_view();
    for (size_t i{}; i < RING_BUFFER_CAP / sizeof(size_t) - 1; ++i) {
        auto buf = full_view.cut(sizeof(size_t)).merge();
        auto *num = (size_t *)buf.data();
        ASSERT_EQ(i, *num);
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}
