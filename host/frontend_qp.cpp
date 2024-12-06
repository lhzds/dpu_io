#include "comm/frontend_qp.h"
#include "bytes.h"
#include "format.h"
#include <doca_pe.h>

namespace pulsedb {
FrontendQp::FrontendQp(Frontend &frontend, size_t id,
    size_t req_rb_cap, size_t resp_rb_cap)
    : m_id(id), m_req_rb(req_rb_cap), m_resp_rb(resp_rb_cap) {
    docacxx::check_derr(doca_pe_create(&m_pe), "failed to create pe");
    
    m_req_mmap = std::make_unique<docacxx::Mmap>(frontend.dev(), m_req_rb.data(),
        m_req_rb.cap(), DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    m_resp_mmap = std::make_unique<docacxx::Mmap>(frontend.dev(), m_resp_rb.data(),
        m_resp_rb.cap(), DOCA_ACCESS_FLAG_PCI_READ_WRITE);

    size_t remote_consumer_id = frontend.recv_new_conumser_id();
    m_producer = std::make_unique<SpecializedProducer>(frontend.conn(), m_pe, frontend.dev(),
        SpecializedProducer::Cfg{1}, remote_consumer_id);
    m_producer->set_send_cb([&](auto) {
        m_is_h2d_rb_meta_msg_sended = true;
    });
    m_producer->start_sync();

    m_dummy_consumer_mmap = std::make_unique<docacxx::Mmap>(frontend.dev(), nullptr, 0);
    m_consumer = std::make_unique<docacxx::comch::Consumer>(frontend.conn(), m_pe,
        frontend.pe(), *m_dummy_consumer_mmap, frontend.dev(), docacxx::comch::Consumer::Cfg{1});
    m_consumer->start_sync();
};

FrontendQp::~FrontendQp() {
    m_consumer->clean_sync();
}

void FrontendQp::post_open(const std::string_view &filename, uint32_t open_flags, OpenCallback callback) {
    post_request(RingBufferRequest::write_open_request, callback, filename, open_flags);
}

void FrontendQp::post_read(size_t file_id, size_t offset, size_t size, ReadCallback callback) {
    post_request(RingBufferRequest::write_read_request, callback, file_id, offset, size);
}

void FrontendQp::post_write(size_t file_id, size_t offset, const ConstBytesView &content, WriteCallback callback) {
    post_request(RingBufferRequest::write_write_request, callback, file_id, offset, content);
}

void FrontendQp::post_fsync(size_t file_id, FsyncCallback callback) {
    post_request(RingBufferRequest::write_fsync_request, callback, file_id);
}

void FrontendQp::post_close(size_t file_id, CloseCallback callback) {
    post_request(RingBufferRequest::write_close_request, callback, file_id);
}

template <typename WriteReqFunc, typename... Args, typename Callback>
void FrontendQp::post_request(WriteReqFunc write_req_func, Callback callback, Args... args) {
    ++m_pending_req_cnt;
    
    auto slot_id = find_free_req_slot();
    m_req_slots[slot_id].m_is_free = true;
    if (callback != nullptr) {
        m_req_slots[slot_id].m_callback = std::move(callback);
    }
    
    while (not write_req_func(m_req_rb, slot_id, args...)) {
        update_host_req_rb_head();
    }
    send_host_req_rb_tail();
}

void FrontendQp::poll_wait() {
    poll_wait(m_pending_req_cnt);
}

void FrontendQp::poll_wait(size_t n) {
    for (size_t i{}; i < std::min(n, m_pending_req_cnt); ++i) {
        // 轮询等待 DPU 处理结果
        while (m_resp_rb.empty()) {
            update_host_resp_rb_tail();
        }

        auto full_view = m_resp_rb.to_view();
        auto resp_variant = RingBufferResponse::read(full_view);
        RingBufferResponse::Type resp_type{};
        size_t req_id{};
        std::visit([&resp_type, &req_id](const auto &resp) {
            resp_type = resp->header->type;
            req_id = resp->header->req_id;
        }, resp_variant);

        auto &req_slot = m_req_slots[req_id];
        using RbResp = RingBufferResponse;
        switch (resp_type) {
            case RbResp::Type::OPEN: {
                auto open_resp = RbResp::get_req_ptr<RingBufferResponse::OpenResponse>(resp_variant);
                auto open_callback = std::any_cast<OpenCallback>(req_slot.m_callback);
                open_callback(open_resp->header->error_code, open_resp->header->file_id);
                m_resp_rb.read(open_resp->size());
                break;
            }
            case RingBufferResponse::Type::CLOSE: {
                auto close_resp = RbResp::get_req_ptr<RingBufferResponse::CloseResponse>(resp_variant);
                auto close_callback = std::any_cast<CloseCallback>(req_slot.m_callback);
                close_callback(close_resp->header->error_code);
                m_resp_rb.read(close_resp->size());
                break;
            }
            case RingBufferResponse::Type::READ: {
                auto read_resp = RbResp::get_req_ptr<RingBufferResponse::ReadResponse>(resp_variant);
                auto read_callback = std::any_cast<ReadCallback>(req_slot.m_callback);
                read_callback(read_resp->header->error_code, read_resp->content);
                m_resp_rb.read(read_resp->size());
                break;
            }
            case RingBufferResponse::Type::WRITE: {
                auto write_resp = RbResp::get_req_ptr<RingBufferResponse::WriteResponse>(resp_variant);
                auto write_callback = std::any_cast<WriteCallback>(req_slot.m_callback);
                write_callback(write_resp->header->error_code);
                m_resp_rb.read(write_resp->size());
                break;
            }
            case RingBufferResponse::Type::FSYNC: {
                auto fsync_resp = RbResp::get_req_ptr<RingBufferResponse::FsyncResponse>(resp_variant);
                auto fsync_callback = std::any_cast<FsyncCallback>(req_slot.m_callback);
                fsync_callback(fsync_resp->header->error_code);
                m_resp_rb.read(fsync_resp->size());
                break;
            }
        }
        req_slot.m_is_free = true;
        req_slot.m_callback = std::any{};
        --m_pending_req_cnt;
        // send_host_req_rb_tail();
    }
    send_host_resp_rb_head();
}

void FrontendQp::send_host_req_rb_tail() {
    H2DRbMetaMsg h2d_rb_meta_msg;
    h2d_rb_meta_msg.type = H2DRbMetaMsg::Type::NEW_HOST_REQ_RB_TAIL;
    h2d_rb_meta_msg.payload.new_host_req_rb_tail = m_req_rb.tail();
    send_h2d_rb_meta_msg({(const char *)&h2d_rb_meta_msg, sizeof(h2d_rb_meta_msg)});
}

void FrontendQp::send_host_resp_rb_head() {
    H2DRbMetaMsg h2d_rb_meta_msg;
    h2d_rb_meta_msg.type = H2DRbMetaMsg::Type::NEW_HOST_RESP_RB_HEAD;
    h2d_rb_meta_msg.payload.new_host_resp_rb_head = m_resp_rb.head();
    send_h2d_rb_meta_msg({(const char *)&h2d_rb_meta_msg, sizeof(h2d_rb_meta_msg)});
}

void FrontendQp::update_host_req_rb_head() {
    // 提前 submit 一个 post_recv 任务，等待获取 req_rb_head
    bool is_d2h_rb_meta_msg_recved{};
    D2HRbMetaMsg d2h_rb_meta_msg;
    auto post_recv_task = m_consumer->alloc_post_recv_task([&](const auto &task) {
        is_d2h_rb_meta_msg_recved = true;
        std::memcpy(&d2h_rb_meta_msg, task.imm_data(), task.imm_data_size());
    });
    auto ret = m_consumer->submit(std::move(post_recv_task));
    assert(not ret);

    H2DRbMetaMsg h2d_rb_meta_msg;
    h2d_rb_meta_msg.type = H2DRbMetaMsg::Type::REQUIRE_HOST_REQ_RB_HEAD;
    h2d_rb_meta_msg.payload.host_req_rb_head = m_req_rb.head();
    send_h2d_rb_meta_msg({(const char *)&h2d_rb_meta_msg, sizeof(h2d_rb_meta_msg)});

    while (not is_d2h_rb_meta_msg_recved) {
        doca_pe_progress(m_pe);
    }
    if (d2h_rb_meta_msg.type != D2HRbMetaMsg::Type::NEW_HOST_REQ_RB_HEAD) {
        throw_with("wrong d2h_rb_meta_msg type");
    }
    auto read_size = m_req_rb.len(m_req_rb.head(), d2h_rb_meta_msg.payload.host_req_rb_head);
    m_req_rb.read(read_size);
}

void FrontendQp::update_host_resp_rb_tail() {
    // 提前 submit 一个 post_recv 任务，等待获取 req_rb_head
    bool is_d2h_rb_meta_msg_recved{};
    D2HRbMetaMsg d2h_rb_meta_msg;
    auto post_recv_task = m_consumer->alloc_post_recv_task([&](const auto &task) {
        is_d2h_rb_meta_msg_recved = true;
        std::memcpy(&d2h_rb_meta_msg, task.imm_data(), task.imm_data_size());
    });
    auto ret = m_consumer->submit(std::move(post_recv_task));
    assert(not ret);

    H2DRbMetaMsg h2d_rb_meta_msg;
    h2d_rb_meta_msg.type = H2DRbMetaMsg::Type::REQUIRE_HOST_RESP_RB_TAIL;
    h2d_rb_meta_msg.payload.host_resp_rb_tail = m_resp_rb.tail();
    send_h2d_rb_meta_msg({(const char *)&h2d_rb_meta_msg, sizeof(h2d_rb_meta_msg)});

    while (not is_d2h_rb_meta_msg_recved) {
        doca_pe_progress(m_pe);
    }
    if (d2h_rb_meta_msg.type != D2HRbMetaMsg::Type::NEW_HOST_RESP_RB_TAIL) {
        throw_with("wrong d2h_rb_meta_msg type");
    }
    auto write_size = m_resp_rb.len(m_resp_rb.tail(), d2h_rb_meta_msg.payload.host_resp_rb_tail);
    m_resp_rb.rep().write(write_size);
}

void FrontendQp::send_h2d_rb_meta_msg(const ConstBytesView &raw_msg) {
    m_is_h2d_rb_meta_msg_sended = false;
    auto task = m_producer->alloc_send_task((uint8_t *)raw_msg.data(), raw_msg.size(), nullptr);
    auto ret = m_producer->submit(std::move(task));
    while (ret) {
        doca_pe_progress(m_pe);
        ret = m_producer->submit(std::move(task));
    }
    while (not m_is_h2d_rb_meta_msg_sended) {
        doca_pe_progress(m_pe);
    }
}

size_t FrontendQp::find_free_req_slot() {
    for (size_t i{}; i < m_req_slots.size(); ++i) {
        if (m_req_slots[i].m_is_free) {
            return i;
        }
    }
    m_req_slots.emplace_back();
    return m_req_slots.size() - 1;
}
}
