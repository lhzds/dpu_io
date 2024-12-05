#include <cerrno>
#include <memory>

#include "dma_qp_mapping.h"
#include "bytes.h"
#include "doca_wrapper/queue_dma.h"
#include "format.h"
#include "ring_buffer.h"
#include "types.h"

namespace pulsedb {
DmaQpMapping::QueueData::QueueData(doca_dev *dev, size_t dpu_rb_cap,
    ConstBytesView host_mmap_desc, char *host_mem_addr, size_t host_mem_size)
    : dpu_rb(dpu_rb_cap), host_rb_rep(host_mem_size) {
    dpu_mmap = std::make_unique<docacxx::Mmap>(dev, dpu_rb.data(), dpu_rb_cap);
    host_mmap = std::make_unique<docacxx::Mmap>(host_mmap_desc, dev, host_mem_addr, host_mem_size);
}

DmaQpMapping::DmaQpMapping(Backend &backend, IoCtx *io_ctx,
        size_t dpu_req_rb_cap, ConstBytesView host_req_mmap_desc, 
        char *host_req_mem_addr, size_t host_req_mem_size,
        size_t dpu_resp_rb_cap, ConstBytesView host_resp_mmap_desc,
        char *host_resp_mem_addr, size_t host_resp_mem_size): m_dma(backend.dma()), m_io_ctx(io_ctx),
    m_req_mapping(backend.dev(), dpu_req_rb_cap, host_req_mmap_desc, host_req_mem_addr, host_req_mem_size),
    m_resp_mapping(backend.dev(), dpu_resp_rb_cap, host_resp_mmap_desc, host_resp_mem_addr, host_resp_mem_size) {
    
    m_consumer = std::make_unique<PassiveConsumer>(
        backend.conn(), backend.pe(),
        backend.pe(), backend.dev(),
        PassiveConsumer::Cfg{1}, [&](const auto &raw_msg)
            { recv_h2d_rb_meta_msg(raw_msg); });

    // 设计上同步传输，目前只需分配一个 task slot
    size_t remote_consumer_id = backend.recv_new_conumser_id();
    m_producer = std::make_unique<SpecializedProducer>(backend.conn(), backend.pe(), backend.dev(),
        SpecializedProducer::Cfg{1}, remote_consumer_id);
    m_producer->start_sync();
}

void DmaQpMapping::recv_h2d_rb_meta_msg(const ConstBytesView &raw_msg) {
    if (raw_msg.size() != sizeof(H2DRbMetaMsg)) {
        throw_with("wrong h2d msg size ");
    }
    auto *msg = (H2DRbMetaMsg *)raw_msg.data();
    using Type = H2DRbMetaMsg::Type;
    switch (msg->type) {
        case Type::NEW_HOST_REQ_RB_TAIL: {
            submit_new_reqs(msg->payload.new_host_req_rb_tail);
            break;
        }
        case Type::REQUIRE_HOST_REQ_RB_HEAD: {
            if (msg->payload.host_req_rb_head == m_req_mapping.host_rb_rep.head()) {
                m_need_host_req_rb_head = true;
            } else {
                send_host_req_rb_head();
            }
            break;
        }
        case Type::NEW_HOST_RESP_RB_HEAD: {
            auto &host_resp_rb_rep = m_resp_mapping.host_rb_rep;
            size_t old_host_resp_rb_head = host_resp_rb_rep.head();
            size_t read_size = host_resp_rb_rep.len(old_host_resp_rb_head, msg->payload.new_host_resp_rb_head);
            host_resp_rb_rep.read(read_size);
            while (m_resp_dma_wait_queue.front().cond()) {
                auto &[_, resp_dma_action] = m_resp_dma_wait_queue.front();
                resp_dma_action();
                m_resp_dma_wait_queue.pop();
            }
            break;
        }
        case Type::REQUIRE_HOST_RESP_RB_TAIL: {
            if (msg->payload.host_resp_rb_tail == m_host_resp_rb_dma_done_tail) {
                m_need_host_resp_rb_tail = true;
            } else {
                send_host_resp_rb_tail();
            }
            break;
        }
    }
}

void DmaQpMapping::submit_new_reqs(size_t new_host_req_rb_tail) {
    auto &[dpu_req_rb, host_req_rb_rep, req_msgs, req_insert_wait_queue, dpu_req_mmap, host_req_mmap]
        = m_req_mapping;

    // 计算新数据长度，并更新 host rb 元数据
    size_t old_host_req_tail = host_req_rb_rep.tail();
    size_t new_req_msg_size = host_req_rb_rep.len(old_host_req_tail, new_host_req_rb_tail);
    auto host_slice = host_req_rb_rep.write(new_req_msg_size);
    
    // 用于判断 dpu_req_rb 是否有空间，否则放入等待队列
    auto insert_req_cond = [&, new_req_msg_size]() -> bool { 
        return dpu_req_rb.free_size() >= new_req_msg_size;
    };
    // 用于将 req 插入 req 环形队列
    auto insert_req_action = [&, host_slice, new_req_msg_size] {
        auto dpu_slice = dpu_req_rb.rep().write(new_req_msg_size); // 申请空间
        assert(dpu_slice.has_value());
        req_msgs.emplace_back(Msg::Status::DmaInProgress, *dpu_slice); // 记录元数据
        // 添加 dma 任务
        auto req_batch_msg_iter = std::prev(req_msgs.end());
        m_dma->post_memcpyv(*host_req_mmap, *dpu_req_mmap,
            make_memcpy_segs(*host_slice, *dpu_slice),
            [&, req_batch_msg_iter = std::move(req_batch_msg_iter)] {
                req_batch_dma_done_cb(req_batch_msg_iter); 
            });
    };

    // 如果队列中有等待进行的任务（FIFO），或者已经有任务了
    if (not req_insert_wait_queue.empty() || not insert_req_cond()) {
        req_insert_wait_queue.emplace(std::move(insert_req_cond), std::move(insert_req_action));
    } else {
        insert_req_action();
    }
}

void DmaQpMapping::req_batch_dma_done_cb(std::list<Msg>::iterator req_batch_msg_iter) {
    auto &req_msgs = m_req_mapping.dpu_msgs;
    auto &host_req_rb_rep = m_req_mapping.host_rb_rep;
    bool update_host_req_rb_head{};

    req_batch_msg_iter->start_process();
    for (const auto &msg : req_msgs) {
        if (msg.status() == Msg::Status::DmaInProgress) {
            break;
        }
        host_req_rb_rep.read(msg.rb_slice().size());
        update_host_req_rb_head = true;
    }
    if (update_host_req_rb_head && m_need_host_req_rb_head) {
        send_host_req_rb_head();
    }

    // 将请求 batch 解析为逐个请求并提交
    auto &resp_msgs = m_resp_mapping.dpu_msgs;
    auto &dpu_req_rb = m_req_mapping.dpu_rb;
    auto &dpu_resp_rb = m_resp_mapping.dpu_rb;
    
    auto batch_req_view = dpu_req_rb.view(req_batch_msg_iter->rb_slice());
    auto view_copy = batch_req_view;
    std::list<Msg>::iterator prev_msg_iter = req_batch_msg_iter;

    while (batch_req_view.size() > 0) {
        auto req_slice = dpu_req_rb.slice(RingBufferRequest::read_view(batch_req_view));
        prev_msg_iter = req_msgs.insert(prev_msg_iter, Msg(Msg::Status::Processing, req_slice));
        auto &req_msg = *prev_msg_iter;

        auto req_variant = RingBufferRequest::read(view_copy);
        RingBufferRequest::Type req_type;
        size_t estimated_resp_size{};
        std::visit([&req_type, &estimated_resp_size](auto &req) {
            req_type = req->header->type;
            estimated_resp_size = req->estimated_resp_size();
        }, req_variant);

        auto &resp_insert_wait_queue = m_resp_mapping.msg_insert_wait_queue;
        auto insert_resp_cond = [&, estimated_resp_size]() -> bool { // 用于判断 dpu_resp_rb 是否有空间,否则放入等待队列
            return dpu_resp_rb.free_size() >= estimated_resp_size;
        };
        auto insert_resp_action = [&, estimated_resp_size,
            req_variant = std::make_shared<RingBufferRequest::Variant>(std::move(req_variant))]() mutable { // 用于将 resp 插入 resp 环形队列
            auto resp_view = dpu_resp_rb.reserve_space(estimated_resp_size); // 申请空间
            resp_msgs.emplace_back(Msg::Status::Processing, dpu_resp_rb.slice(resp_view)); // 记录元数据
            auto &resp_msg = resp_msgs.back();
            process_req(req_type, *req_variant, resp_view, req_msg, resp_msg); // 提交 req，并设置结果写到 resp 
        };

        // 如果队列中有等待进行的任务（FIFO），或者已经有任务了
        if (not resp_insert_wait_queue.empty() || not insert_resp_cond()) {
            resp_insert_wait_queue.emplace(std::move(insert_resp_cond), std::move(insert_resp_action));
        } else {
            insert_resp_action();
        }   
    }
    req_msgs.erase(req_batch_msg_iter);
}

void DmaQpMapping::process_req(RingBufferRequest::Type req_type, RingBufferRequest::Variant &req_variant,
    RingBuffer::View resp_view, Msg &req_msg, Msg &resp_msg) {
    using RbReq = RingBufferRequest;
    using RbResp = RingBufferResponse;
    auto &dpu_resp_rb = m_resp_mapping.dpu_rb;

    switch (req_type) {
        case RbReq::Type::OPEN: {
            auto open_req = RbReq::get_req_ptr<RbReq::OpenRequest>(req_variant);
            m_io_ctx->open_async({open_req->filename, open_req->header->filename_size},
                open_req->header->open_flags,
                [&, req_id = open_req->header->req_id](int32_t error_code, size_t file_id) {
                RbResp::OpenHeader header;
                header.type = RbResp::Type::OPEN;
                header.req_id = req_id;
                header.error_code = error_code;
                header.file_id = file_id;
                auto ret = resp_view.write({(char *)&header, sizeof(header)});
                assert(ret);
                req_process_done_cb(req_msg, resp_msg);
            });
            break;
        }
        case RbReq::Type::CLOSE: {
            auto close_req = RbReq::get_req_ptr<RbReq::CloseRequest>(req_variant);
            m_io_ctx->close_async(close_req->header->file_id,
                [&, req_id = close_req->header->req_id](int32_t error_code) {
                RbResp::CloseHeader header;
                header.type = RbResp::Type::CLOSE;
                header.error_code = error_code;
                header.req_id = req_id;
                auto ret = resp_view.write({(char *)&header, sizeof(header)});
                assert(ret);
                req_process_done_cb(req_msg, resp_msg);
            });
            break;
        }
        case RbReq::Type::READ: {
            auto read_req = RbReq::get_req_ptr<RbReq::ReadRequest>(req_variant);
            auto header_view = resp_view.cut(sizeof(RbResp::ReadResponse));
            auto &read_dst_view = resp_view;
            std::vector<BytesView> iov;
            if (read_dst_view.size()) {
                iov.emplace_back(read_dst_view.first_part());
                if (read_dst_view.second_part()) {
                    iov.emplace_back(*read_dst_view.second_part());
                }
            }
            m_io_ctx->read_async(read_req->header->file_id, read_req->header->offset, std::move(iov),
                [&, req_id = read_req->header->req_id](int32_t error_code, size_t read_size) {
                RbResp::ReadHeader header;
                header.type = RbResp::Type::READ;
                header.req_id = req_id;
                header.error_code = error_code;
                header.actual_read_size = read_size;
                auto ret = header_view.write({(char *)&header, sizeof(header)});
                assert(ret);
                req_process_done_cb(req_msg, resp_msg);
            });
            break;
        }
        case RbReq::Type::WRITE: {
            auto write_req = RbReq::get_req_ptr<RbReq::WriteRequest>(req_variant);
            std::vector<ConstBytesView> iov;
            if (write_req->content && write_req->content->size()) {
                iov.emplace_back( write_req->content->first_part());
                if ( write_req->content->second_part()) {
                    iov.emplace_back(* write_req->content->second_part());
                }
            }
            m_io_ctx->write_async(write_req->header->file_id, write_req->header->offset, std::move(iov),
                [&, req_id = write_req->header->req_id](int32_t error_code) {
                RbResp::WriteHeader header;
                header.type = RbResp::Type::WRITE;
                header.req_id = req_id;
                header.error_code = error_code;
                auto ret = resp_view.write({(char *)&header, sizeof(header)});
                assert(ret);
                req_process_done_cb(req_msg, resp_msg);
            });
            break;
        }
        case RbReq::Type::FSYNC: {
            auto fsync_req = RbReq::get_req_ptr<RbReq::FsyncRequest>(req_variant);
            m_io_ctx->fsync_async(fsync_req->header->file_id,
                [&, req_id = fsync_req->header->req_id](int32_t error_code) {
                RbResp::FsyncHeader header;
                header.type = RbResp::Type::FSYNC;
                header.error_code = error_code;
                header.req_id = req_id;
                auto ret = resp_view.write({(char *)&header, sizeof(header)});
                assert(ret);
                req_process_done_cb(req_msg, resp_msg);
            });
            break;
        }
    }
}

void DmaQpMapping::req_process_done_cb(Msg &req_msg, Msg &resp_msg) {
    // 回收 req 占用的空间
    req_msg.done();
    auto &req_msgs = m_req_mapping.dpu_msgs;
    auto &req_rb = m_req_mapping.dpu_rb;
    auto &req_insert_wait_queue = m_req_mapping.msg_insert_wait_queue;
    for (auto iter = req_msgs.begin(); iter != req_msgs.end(); iter = req_msgs.erase(iter)) {
        if (iter->status() != Msg::Status::Done) {
            break;
        }
        req_rb.read(iter->rb_slice().size());
    }
    
    // 执行等待任务
    while (req_insert_wait_queue.front().cond()) {
        auto &[_, req_insert_action] = req_insert_wait_queue.front();
        req_insert_action();
        req_insert_wait_queue.pop();
    }

    // 获取 resp 实际大小
    auto &[dpu_resp_rb, host_resp_rb_rep, resp_msgs, resp_insert_wait_queue, dpu_resp_mmap, host_resp_mmap]
        = m_resp_mapping;
    auto resp_view = dpu_resp_rb.view(resp_msg.rb_slice());
    auto resp_variant = RingBufferResponse::read(resp_view);
    size_t resp_size{};
    std::visit([&resp_size](const auto &resp) { 
        resp_size = resp->size(); 
    }, resp_variant);

    auto resp_dma_cond = [&, resp_size]() -> bool { // 用于判断 host_resp_rb 是否有空间进行 dma，否则放入等待队列
        return host_resp_rb_rep.free_size() >= resp_size;
    };
    auto resp_dma_action = [&, resp_size] { // 用于进行 dma
        auto host_resp_rb_slice = host_resp_rb_rep.write(resp_size);   
        assert(host_resp_rb_slice.has_value());     
        resp_msg.start_dma();
        // TODO：这里属于使用另外的线程 submit 任务，但 pe 线性不安全
        m_dma->post_memcpyv(*dpu_resp_mmap, *host_resp_mmap,
            make_memcpy_segs(resp_msg.rb_slice(), *host_resp_rb_slice),
            [&] { resp_dma_done_cb(resp_msg); });
    };

    // 如果队列中有等待进行的任务（FIFO），或者已经有任务了
    if (not m_resp_dma_wait_queue.empty() || not resp_dma_cond()) {
        m_resp_dma_wait_queue.emplace(std::move(resp_dma_cond), std::move(resp_dma_action));
    } else {
        resp_dma_action();
    }
}

void DmaQpMapping::resp_dma_done_cb(Msg &resp_msg) {
    resp_msg.done();

    auto &resp_msgs = m_resp_mapping.dpu_msgs;
    auto &dpu_resp_rb = m_resp_mapping.dpu_rb;
    auto &host_resp_rb_rep = m_resp_mapping.host_rb_rep;
    auto &resp_insert_wait_queue = m_resp_mapping.msg_insert_wait_queue;

    bool update_host_resp_rb_tail{};
    for (auto iter = resp_msgs.begin(); iter != resp_msgs.end(); iter = resp_msgs.erase(iter)) {
        if (iter->status() != Msg::Status::Done) {
            break;
        }
        update_host_resp_rb_tail = true;
        dpu_resp_rb.read(iter->rb_slice().size());
        if (auto &second_part = iter->rb_slice().second_part(); second_part.has_value()) {
            m_host_resp_rb_dma_done_tail = second_part->size + second_part->offset;
        } else {
            auto &first_part = iter->rb_slice().first_part();
            m_host_resp_rb_dma_done_tail = first_part.size + first_part.offset;
        }        
    }

    if (update_host_resp_rb_tail && m_need_host_resp_rb_tail) {
        send_host_resp_rb_tail();
    }

    while (resp_insert_wait_queue.front().cond()) {
        auto &[_, resp_insert_action] = resp_insert_wait_queue.front();
        resp_insert_action();
        resp_insert_wait_queue.pop();
    }
}

std::vector<QueueDma::MemcpySeg>
DmaQpMapping::make_memcpy_segs(RingBufferRep::Slice src, RingBufferRep::Slice dst) {
    assert(src.size() == dst.size());
    std::vector<QueueDma::MemcpySeg> ret;
    auto src_first_part = src.first_part();
    auto dst_first_part = dst.first_part();
    auto src_second_part = src.second_part();
    auto dst_second_part = dst.second_part();

    if (dst_first_part.size >= src_first_part.size) {
        // src_first_part 拷贝
        ret.emplace_back(src_first_part.offset, dst_first_part.offset, src_first_part.size);
        src_first_part.shrink_forward(src_first_part.size);
        dst_first_part.shrink_forward(src_first_part.size);

        // src_second_part 拷贝
        if (not src.second_part()) {
            return ret;
        }
        if (dst_first_part.size) {
            ret.emplace_back(src_second_part->offset, dst_first_part.offset, dst_first_part.size);
            src_second_part->shrink_forward(dst_first_part.size);
            dst_first_part.shrink_forward(dst_first_part.size);
        }
        if (dst_second_part) {
            ret.emplace_back(src_second_part->offset, dst_second_part->offset, src_second_part->size);
            src_second_part->shrink_forward(src_second_part->size);
            dst_second_part->shrink_forward(src_second_part->size);
        }
    } else {
        // src_first_part 拷贝
        ret.emplace_back(src_first_part.offset, dst_first_part.offset, dst_first_part.size);
        src_first_part.shrink_forward(dst_first_part.size);
        dst_first_part.shrink_forward(dst_first_part.size);

        ret.emplace_back(src_first_part.offset, dst_second_part->offset, src_first_part.size);
        src_first_part.shrink_forward(src_first_part.size);
        dst_second_part->shrink_forward(src_first_part.size);

        // src_second_part 拷贝
        if (not src.second_part()) {
            return ret;
        }
        ret.emplace_back(src_second_part->offset, dst_second_part->offset, src_second_part->size);
        src_second_part->shrink_forward(src_second_part->size);
        dst_second_part->shrink_forward(src_second_part->size);
    }
    return ret;
}

void DmaQpMapping::send_host_req_rb_head() {
    // TODO: 大端/小端字节序？imm_data 的生命周期是否需要延长？
    D2HRbMetaMsg d2h_rb_meta_msg;
    d2h_rb_meta_msg.type = D2HRbMetaMsg::Type::NEW_HOST_REQ_RB_HEAD;
    d2h_rb_meta_msg.payload.host_req_rb_head = m_req_mapping.host_rb_rep.head();
    send_d2h_rb_meta_msg({(const char *)&d2h_rb_meta_msg, sizeof(d2h_rb_meta_msg)});
    m_need_host_req_rb_head = false;
}

void DmaQpMapping::send_host_resp_rb_tail() {
    // TODO: 大端/小端字节序？imm_data 的生命周期是否需要延长？
    D2HRbMetaMsg d2h_rb_meta_msg;
    d2h_rb_meta_msg.type = D2HRbMetaMsg::Type::NEW_HOST_RESP_RB_TAIL;
    d2h_rb_meta_msg.payload.host_resp_rb_tail = m_host_resp_rb_dma_done_tail;
    send_d2h_rb_meta_msg({(const char *)&d2h_rb_meta_msg, sizeof(d2h_rb_meta_msg)});
    m_need_host_resp_rb_tail = false;
}

void DmaQpMapping::send_d2h_rb_meta_msg(const ConstBytesView &raw_msg) {
    auto task = m_producer->alloc_send_task((uint8_t *)raw_msg.data(), raw_msg.size(), nullptr);
    auto ret = m_producer->submit(std::move(task));
    // 不用循环提交，因为 FrontendQp 已经提前提交了一个 post_recv 任务
    assert(not ret);
}
} // namespace pulsedb
