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

void TEST_print_resp_rb_view(const ConstBytesView &data) {
#ifndef NDEBUG
    auto *resp = (IoRespMsg *)data.data();
    std::string type_str;
    if (resp->type == IoRespMsg::Type::kWrite) {
        type_str = "Write";
    } else if (resp->type == IoRespMsg::Type::kRangeSync) {
        type_str = "RangeSync";
    } else if (resp->type == IoRespMsg::Type::kSyncClose) {
        type_str = "SyncClose";
    } else {
        type_str = "Unexpected";
    }
    std::cout << fmt::format(
        "IoRespMsg[Type={}, file_number={}, res={}]",
        type_str, resp->file_number, resp->res
    ) << std::endl;;
#endif
}
        
void TEST_print_resp_rb_view(const RingBuffer::View &v) {
#ifndef NDEBUG
    auto data = v.merge();
    TEST_print_resp_rb_view(data);
#endif
}

std::vector<QueueDma::MemcpySeg>
RbMapping::make_memcpy_segs(RingBufferRep::Slice src, RingBufferRep::Slice dst) {
    assert(src.size() == dst.size());
    std::vector<QueueDma::MemcpySeg> ret;
    auto src_first_part = src.first_part();
    auto dst_first_part = dst.first_part();
    auto src_second_part = src.second_part();
    auto dst_second_part = dst.second_part();

    size_t copy_size{};
    if (dst_first_part.size >= src_first_part.size) {
        // src_first_part 拷贝
        copy_size = src_first_part.size;
        ret.emplace_back(src_first_part.offset, dst_first_part.offset, copy_size);
        dst_first_part.shrink_forward(copy_size);
        src_first_part.shrink_forward(copy_size);

        // src_second_part 拷贝
        if (not src.second_part()) goto RET;

        if (dst_first_part.size) {
            copy_size = dst_first_part.size;
            ret.emplace_back(src_second_part->offset, dst_first_part.offset, copy_size);
            src_second_part->shrink_forward(copy_size);
            dst_first_part.shrink_forward(copy_size);
        }
        if (dst_second_part) {
            copy_size = src_second_part->size;
            ret.emplace_back(src_second_part->offset, dst_second_part->offset, src_second_part->size);
            dst_second_part->shrink_forward(copy_size);
            src_second_part->shrink_forward(copy_size);
        }
    } else {
        // src_first_part 拷贝
        copy_size = dst_first_part.size;
        ret.emplace_back(src_first_part.offset, dst_first_part.offset, copy_size);
        src_first_part.shrink_forward(copy_size);
        dst_first_part.shrink_forward(copy_size);

        copy_size = src_first_part.size;
        ret.emplace_back(src_first_part.offset, dst_second_part->offset, copy_size);
        src_first_part.shrink_forward(copy_size);
        dst_second_part->shrink_forward(copy_size);

        // src_second_part 拷贝
        if (not src.second_part()) goto RET;
        
        copy_size = src_second_part->size;
        ret.emplace_back(src_second_part->offset, dst_second_part->offset, copy_size);
        src_second_part->shrink_forward(copy_size);
        dst_second_part->shrink_forward(copy_size);
    }

RET:
#ifndef NDEBUG
    std::string log_entry = "[Helper] ";
    for (size_t i{}; i < ret.size(); ++i) {
        auto &seg = ret[i];
        if (i) {
            log_entry.append(", ");
        }
        log_entry.append(fmt::format(
            "Make memcpy segs: seg#{}[src_offset={}, dst_offset={}, size={}]",
            i, seg.src_offset, seg.dst_offset, seg.size
        ));
    }
    SPDLOG_INFO(log_entry);
#endif
    return ret;
}

DmaQpMapping::DmaQpMapping(Backend &backend, int id,
    size_t dpu_req_rb_cap, ConstBytesView host_req_mmap_desc, 
    char *host_req_rb_data, size_t host_req_rb_cap,
    size_t dpu_resp_rb_cap, ConstBytesView host_resp_mmap_desc,
    char *host_resp_rb_data, size_t host_resp_rb_cap,
    ReqRbMapping::DmaDoneCb req_dma_done_cb,
    size_t direct_alignment)
    : m_id(id),
    m_req_rb_mapping(id, backend.dev(), dpu_req_rb_cap, host_req_mmap_desc,
        host_req_rb_data, host_req_rb_cap, backend.dma(), std::move(req_dma_done_cb),
        direct_alignment),
    m_resp_rb_mapping(id, backend.dev(), dpu_resp_rb_cap, host_resp_mmap_desc,
        host_resp_rb_data, host_resp_rb_cap, backend.dma(), direct_alignment) {
    SPDLOG_INFO(
        "[Backend] Create dma_qp_mapping"
        "[id={}, dpu_req_rb_cap={}, host_req_rb_cap={}, "
        "dpu_resp_rb_cap={}, host_resp_rb_cap={}]", m_id,
        m_req_rb_mapping.dpu_rb_cap(), m_req_rb_mapping.host_rb_cap(),
        m_resp_rb_mapping.dpu_rb_cap(), m_resp_rb_mapping.host_rb_cap()
    );
}

DmaQpMapping::~DmaQpMapping() {
    m_producer->clean_sync();
}

void DmaQpMapping::create_consumer(Backend &backend) {
    PassiveConsumer::Cfg cfg;
    // 尽管同步传输，但 task slot 数仍需大于 1
    // 以防止单个 post_recv 任务准备过慢，导致对端 resubmit send_task 次数过多
    cfg.post_recv_task_slot_num = 5;
    cfg.max_msg_size = kCommRbMetaSize;
    m_consumer = std::make_unique<PassiveConsumer>(
        backend.conn(), backend.pe(), backend.pe(),
        backend.dev(), cfg,
        [&](const auto &raw_msg) { recv_h2d_rb_meta_msg(raw_msg); }
    );
}

void DmaQpMapping::create_producer(Backend &backend) {
    size_t remote_consumer_id = backend.recv_new_conumser_id();
    m_producer = std::make_unique<QueueSingleTaskProducer>(
        backend.conn(), backend.pe(), backend.dev(),
        kCommRbMetaSize, remote_consumer_id
    );
    m_producer->start_sync();

    m_req_rb_mapping.set_producer(m_producer.get());
    m_resp_rb_mapping.set_producer(m_producer.get());
}

void DmaQpMapping::recv_h2d_rb_meta_msg(const ConstBytesView &raw_msg) {
    if (raw_msg.size() != sizeof(H2DRbMetaMsg)) {
        throw_with("DmaQpMapping: Wrong h2d meta msg size ");
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
            m_resp_rb_mapping.receive_wait_stat(
                wait_stat.processed_size, wait_stat.wait_size
            );
            break;
        }
        default:
            throw_with("DmaQpMapping: Unexpected h2d meta msg type");
    }
}

void DmaQpMapping::finish_req(ReqRbMapping::DpuRbView &req_view) {
    m_req_rb_mapping.invalid_dpu_view(req_view);
}

void DmaQpMapping::finish_req(ReqRbMapping::DpuRbView &req_view, BytesBuffer resp_data) {
    m_req_rb_mapping.invalid_dpu_view(req_view);
    m_resp_rb_mapping.append(std::move(resp_data));
}

} // namespace pulsedb
