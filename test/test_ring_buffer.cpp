#include <cstdlib>
#include <cstdint>

#include "bytes.h"
#include "constant.h"
#include "format/rb_req_resp_msg.h"
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

TEST(RingBufferTest, DirectHeaderPaddingParse) {
    constexpr size_t kAlign = kDefaultDirectAlignment;
    RingBuffer rb(1 << 14, kAlign);

    IoReqMsg::WriteHeader header;
    auto padding_len = static_cast<uint32_t>(
        (kAlign - (sizeof(header) % kAlign)) % kAlign);
    IoReqMsg::init_write_req_header(header, 7, 0, 1, true, padding_len);
    header.size = kAlign;

    BytesBuffer padding(header.payload_padding_len);
    BytesBuffer payload(header.size);
    std::memset(payload.data(), 'x', payload.size());

    ASSERT_TRUE(rb.write({reinterpret_cast<char*>(&header), sizeof(header)}));
    if (header.payload_padding_len > 0) {
        ASSERT_TRUE(rb.write({padding.data(), padding.size()}));
    }
    ASSERT_TRUE(rb.write({payload.data(), payload.size()}));

    auto view = rb.to_view();
    auto req_var = IoReqMsg::parse_from(view);
    auto req = IoReqMsg::get_req_ptr<IoReqMsg::WriteMsgRbRef>(req_var);
    ASSERT_TRUE(req->header->is_direct);
    ASSERT_EQ(req->header->payload_padding_len, header.payload_padding_len);
    ASSERT_TRUE(req->content.has_value());
    ASSERT_EQ(req->content->size(), payload.size());
    ASSERT_EQ(reinterpret_cast<uintptr_t>(req->content->first_part().data()) % kAlign, 0U);
    auto merged = req->content->merge();
    ASSERT_EQ(std::memcmp(merged.data(), payload.data(), payload.size()), 0);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}
