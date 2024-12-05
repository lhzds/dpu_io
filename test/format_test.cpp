#include <cstdlib>
#include <string_view>

#include "bytes.h"
#include "format.h"
#include "ring_buffer.h"
#include "gtest/gtest.h"
#include <type_traits>

constexpr size_t RING_BUFFER_CAP{1 << 8};

using namespace pulsedb;

TEST(FormatTest, CtrlMsgTest) {
    BytesBuffer buf;
    
    buf = CtrlMsg::make_init_msg(66);
    auto init_msg = std::get<CtrlMsg::InitMsg>(CtrlMsg::read(buf));
    ASSERT_EQ(init_msg.header->type, CtrlMsg::Type::INIT);
    ASSERT_EQ(init_msg.header->qp_num, 66);

    buf = CtrlMsg::make_init_qp_msg(11, (char *)22, 33, (char *)44, 55,
        std::string_view("aaa"), std::string_view("bbb"));
    auto init_qp_msg = std::get<CtrlMsg::InitQpMsg>(CtrlMsg::read(buf));
    ASSERT_EQ(init_qp_msg.header->type, CtrlMsg::Type::INIT_QP);
    ASSERT_EQ(init_qp_msg.header->qp_id, 11);
    ASSERT_EQ(init_qp_msg.header->host_req_mem_addr, (char *)22);
    ASSERT_EQ(init_qp_msg.header->host_req_mem_size, 33);
    ASSERT_EQ(init_qp_msg.header->host_resp_mem_addr, (char *)44);
    ASSERT_EQ(init_qp_msg.header->host_resp_mem_size, 55);
    ASSERT_EQ(std::string_view(init_qp_msg.host_req_mmap_desc,
        init_qp_msg.header->host_req_mmap_desc_size), "aaa");
    ASSERT_EQ(std::string_view(init_qp_msg.host_resp_mmap_desc,
        init_qp_msg.header->host_resp_mmap_desc_size), "bbb");

    buf = CtrlMsg::make_ack_msg();
    auto ack_msg = std::get<CtrlMsg::AckMsg>(CtrlMsg::read(buf));
    ASSERT_EQ(ack_msg.header->type, CtrlMsg::Type::ACK);
  
    buf = CtrlMsg::make_exit_msg();
    auto exit_msg = std::get<CtrlMsg::ExitMsg>(CtrlMsg::read(buf));
    ASSERT_EQ(exit_msg.header->type, CtrlMsg::Type::EXIT);
}

bool write_rb_req(RingBuffer &rb, RingBufferRequest::Type rb_req_type) {
    using RbReq = RingBufferRequest;
    using ReqTy = RbReq::Type;
    bool ret;
    switch (rb_req_type) {
        case ReqTy::OPEN:
            ret = RbReq::write_open_request(rb, 11, "filename", 22);
            break;
        case ReqTy::CLOSE:
            ret = RbReq::write_close_request(rb, 33, 44);
            break;
        case ReqTy::READ:
            ret = RbReq::write_read_request(rb, 55, 66, 77, 88);
            break;
        case ReqTy::WRITE:
            ret = RbReq::write_write_request(rb, 99, 100, 110, std::string_view("content"));
            break;
        case ReqTy::FSYNC:
            ret = RbReq::write_fsync_request(rb, 120, 130);
            break;
    }
    return ret;
}

template<typename Req>
void test_rb_req(const Req &req) {
    using RbReq = RingBufferRequest;
    using ReqTy = RbReq::Type;
    if constexpr (std::is_same_v<Req, RbReq::OpenRequest>) {
        ASSERT_EQ(req.header->type, ReqTy::OPEN);
        ASSERT_EQ(req.header->req_id, 11);
        ASSERT_EQ(req.header->open_flags, 22);
        ASSERT_EQ(std::string_view(req.filename, req.header->filename_size),
            "filename");
    } else if constexpr (std::is_same_v<Req, RbReq::CloseRequest>) {
        ASSERT_EQ(req.header->type, ReqTy::CLOSE);
        ASSERT_EQ(req.header->req_id, 33);
        ASSERT_EQ(req.header->file_id, 44);
    } else if constexpr (std::is_same_v<Req, RbReq::ReadRequest>) {
        ASSERT_EQ(req.header->type, ReqTy::READ);
        ASSERT_EQ(req.header->req_id, 55);
        ASSERT_EQ(req.header->file_id, 66);
        ASSERT_EQ(req.header->offset, 77);
        ASSERT_EQ(req.header->read_size, 88);
    } else if constexpr (std::is_same_v<Req, RbReq::WriteRequest>) {
        ASSERT_EQ(req.header->type, ReqTy::WRITE);
        ASSERT_EQ(req.header->req_id, 99);
        ASSERT_EQ(req.header->file_id, 100);
        ASSERT_EQ(req.header->offset, 110);
        auto view = req.content->merge();
        ASSERT_EQ(std::string_view(view.data(), view.size()), "content");
    } else {
        ASSERT_EQ(req.header->type, ReqTy::FSYNC);
        ASSERT_EQ(req.header->req_id, 120);
        ASSERT_EQ(req.header->file_id, 130);
    }
}

TEST(FormatTest, RingBufferRequest) {
    using RbReq = RingBufferRequest;
    using ReqTy = RbReq::Type;
    RingBuffer rb(1 << 10);
    for (size_t i{}; i < (1 << 10); ++i) {
        switch (random() % 5) {
            case 0: {
                write_rb_req(rb, ReqTy::WRITE);
                auto full_view = rb.to_view();
                auto req = RbReq::read(full_view);
                auto write_req = RbReq::get_req_ptr<RbReq::WriteRequest>(req);
                test_rb_req(*write_req);
                rb.read(write_req->size());
                break;
            }
            case 1: {
                write_rb_req(rb, ReqTy::READ);
                auto full_view = rb.to_view();
                auto req = RbReq::read(full_view);
                auto read_req = RbReq::get_req_ptr<RbReq::ReadRequest>(req);
                test_rb_req(*read_req);
                rb.read(read_req->size());
                break;
            }
            case 2: {
                write_rb_req(rb, ReqTy::FSYNC);
                auto full_view = rb.to_view();
                auto req = RbReq::read(full_view);
                auto fsync_req = RbReq::get_req_ptr<RbReq::FsyncRequest>(req);
                test_rb_req(*fsync_req);
                rb.read(fsync_req->size());
                break;
            }
            case 3: {
                write_rb_req(rb, ReqTy::OPEN);
                auto full_view = rb.to_view();
                auto req = RbReq::read(full_view);
                auto open_req = RbReq::get_req_ptr<RbReq::OpenRequest>(req);
                test_rb_req(*open_req);
                rb.read(open_req->size());
                break;
            }
            case 4: {
                write_rb_req(rb, ReqTy::CLOSE);
                auto full_view = rb.to_view();
                auto req = RbReq::read(full_view);
                auto close_req = RbReq::get_req_ptr<RbReq::CloseRequest>(req);
                test_rb_req(*close_req);
                rb.read(close_req->size());
                break;
            }
        }
        ASSERT_TRUE(rb.empty());
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();   
}
