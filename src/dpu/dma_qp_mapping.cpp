#include <cerrno>
#include <memory>
#include <spdlog/spdlog.h>

#include "comm/backend.h"
#include "comm/dma_qp_mapping.h"
#include "bytes.h"
#include "constant.h"
#include "doca_wrapper/passive_consumer.h"
#include "doca_wrapper/queue_dma.h"
#include "ring_buffer.h"
#include "types.h"
#include "util.h"

namespace pulsedb {
std::vector<QueueDma::MemcpySeg>
RbMapping::make_memcpy_segs(RingBufferRep::Slice src, RingBufferRep::Slice dst) {
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

DmaQpMapping::DmaQpMapping(Backend &backend, size_t id,
    size_t dpu_req_rb_cap, ConstBytesView host_req_mmap_desc, 
    char *host_req_mem_addr, size_t host_req_mem_size,
    size_t dpu_resp_rb_cap, ConstBytesView host_resp_mmap_desc,
    char *host_resp_mem_addr, size_t host_resp_mem_size,
    ReqRbMapping::DmaDoneCb req_dma_done_cb)
    : m_id(id),
    m_req_rb_mapping(id, backend.dev(), dpu_req_rb_cap, host_req_mmap_desc,
        host_req_mem_addr, host_req_mem_size, backend.dma()),
    m_resp_rb_mapping(id, backend.dev(), dpu_resp_rb_cap, host_resp_mmap_desc,
        host_resp_mem_addr, host_resp_mem_size, backend.dma()) {
    SPDLOG_INFO("create a dma_qp_mapping with id={}"
        "[dpu_req_rb_cap={}, host_req_rb_cap={}, "
        "dpu_resp_rb_cap={}, host_resp_rb_cap={}", m_id,
        dpu_req_rb_cap, host_req_mem_size,
        dpu_resp_rb_cap, host_resp_mem_size);
    m_req_rb_mapping.set_dma_done_cb(std::move(req_dma_done_cb));
}

DmaQpMapping::~DmaQpMapping() {
    m_producer->clean_sync();
}

void DmaQpMapping::create_consumer(Backend &backend) {
    PassiveConsumer::Cfg cfg;
    cfg.post_recv_task_slot_num = 1; // 设计上同步传输，目前只需分配一个 task slot
    cfg.max_msg_size = kCommRbMetaSize;
    m_consumer = std::make_unique<PassiveConsumer>(backend.conn(), backend.pe(), backend.pe(),
        backend.dev(), cfg, [&](const auto &raw_msg) { recv_h2d_rb_meta_msg(raw_msg); });
}

void DmaQpMapping::create_producer(Backend &backend) {
    size_t remote_consumer_id = backend.recv_new_conumser_id();
    m_producer = std::make_unique<SingleTaskProducer>(backend.conn(), backend.pe(), backend.dev(),
        kCommRbMetaSize, remote_consumer_id);
    m_producer->start_sync();

    m_req_rb_mapping.set_producer(m_producer.get());
    m_resp_rb_mapping.set_producer(m_producer.get());
}

void DmaQpMapping::recv_h2d_rb_meta_msg(const ConstBytesView &raw_msg) {
    if (raw_msg.size() != sizeof(H2DRbMetaMsg)) {
        throw_with("wrong h2d msg size ");
    }
    auto *msg = (H2DRbMetaMsg *)raw_msg.data();
    using Type = H2DRbMetaMsg::Type;
    switch (msg->type) {
        case Type::kReqRbNewSize: {
            size_t new_size = msg->payload.req_rb_new_size;
            m_req_rb_mapping.read_async(new_size);
            break;
        }
        case Type::kReqRbNeededSpace: {
            size_t needed_size = msg->payload.req_rb_needed_space;
            m_req_rb_mapping.receive_needed_size(needed_size);
            break;
        }
        case Type::kWaitStat: {
            auto wait_stat = msg->payload.wait_stat;
            m_resp_rb_mapping.receive_wait_stat(wait_stat.processed_size, wait_stat.wait_size);
            break;
        }
        default:
            throw_with("unexpected error");
    }
}

void DmaQpMapping::finish_req(ReqRbMapping::DpuRbView &req_view, BytesBuffer data) {
    m_req_rb_mapping.invalid_dpu_view(req_view);
    m_resp_rb_mapping.append(std::move(data));
}

} // namespace pulsedb
