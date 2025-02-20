#include <cstring>
#include <doca_pe.h>
#include <optional>

#include "bytes.h"
#include "comm/frontend_qp.h"
#include "constant.h"
#include "doca_wrapper/passive_consumer.h"
#include "format/rb_meta_msg.h"
#include "format/rb_req_resp_msg.h"
#include "util.h"

namespace pulsedb {
FrontendQp::FrontendQp(Frontend &frontend, int id,
    size_t req_rb_cap, size_t resp_rb_cap)
    : m_id(id), m_req_rb(req_rb_cap), m_resp_rb(resp_rb_cap) {
    docacxx::check_derr(doca_pe_create(&m_pe), "failed to create pe");
    
    m_req_mmap = std::make_unique<docacxx::Mmap>(frontend.dev(), m_req_rb.data(),
        m_req_rb.cap(), DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    m_resp_mmap = std::make_unique<docacxx::Mmap>(frontend.dev(), m_resp_rb.data(),
        m_resp_rb.cap(), DOCA_ACCESS_FLAG_PCI_READ_WRITE);
    SPDLOG_INFO("[Frontend-Qp#{}] Create", id);
};

FrontendQp::~FrontendQp() {
    m_producer->clean_sync();
}

void FrontendQp::create_producer(Frontend &frontend) {
    size_t remote_consumer_id = frontend.recv_new_conumser_id();
    m_producer = std::make_unique<SingleTaskProducer>(frontend.conn(), m_pe, frontend.dev(),
        kCommRbMetaSize, remote_consumer_id);
    m_producer->start_sync();
}

void FrontendQp::create_consumer(Frontend &frontend) {
    PassiveConsumer::Cfg cfg;
    cfg.max_msg_size = kCommRbMetaSize;
    cfg.post_recv_task_slot_num = 1;
    m_consumer = std::make_unique<PassiveConsumer>(frontend.conn(), m_pe,
        frontend.pe(), frontend.dev(), cfg, [&](const auto &view) {
        auto *msg = (D2HRbMetaMsg *)view.data();

        switch (msg->type) {
            case D2HRbMetaMsg::Type::kReqRbCleanedSize:
                SPDLOG_INFO("[Frontend-Qp#{}] Receive meta msg[Req-Rb: cleaned_size={}]",
                    m_id, msg->payload.req_rb_cleaned_size);
                m_req_rb_cleaned_size = msg->payload.req_rb_cleaned_size;
                break;
            case D2HRbMetaMsg::Type::kRespRbNewSize:
                SPDLOG_INFO("[Frontend-Qp#{}] Receive meta msg[Resp-Rb: new_size={}]",
                    m_id, msg->payload.resp_rb_new_size);
                m_resp_rb_new_size = msg->payload.resp_rb_new_size;
                break;
            default:
                throw_with("unexpected D2HRbMetaMsg type");
        }
    });
}

void FrontendQp::post_sync_close(size_t file_number, bool use_fsync) {
    if (m_is_prep_write) {
        throw_with("preparing write now");
    }
    auto req = IoReqMsg::make_sync_close_req(file_number, use_fsync);
    if (m_req_rb.cap() < req.size()) {
        throw_with("too large to write to rb");
    }

    while (m_req_rb.free_size() < req.size()) {
        free_req_rb_space(req.size());
    }

    m_req_rb.write(req);
    ++m_submitted_req_cnt;
    SPDLOG_INFO("[Frontend-Qp#{}] Post request[SyncClose: file_num={}]"
        "[Req-Rb: free_space={}]", m_id, file_number, m_req_rb.free_size());
    send_req_rb_new_size(req.size());
}

void FrontendQp::prep_write(size_t file_number, size_t offset, int base_size_multiplier) {
    if (m_is_prep_write) {
        throw_with("already prepared write");
    }
    IoReqMsg::init_write_req_header(m_cache_header, file_number, offset,
        base_size_multiplier);
    size_t new_size = sizeof(m_cache_header);
    if (m_req_rb.cap() < new_size) {
        throw_with("too large to write to rb");
    }

    while (m_req_rb.free_size() < new_size) {
        free_req_rb_space(new_size);
   }

   m_reserved_header_space = m_req_rb.reserve_space(new_size);
   m_is_prep_write = true;
}

void FrontendQp::append(const ConstBytesView &content) {
    if (not m_is_prep_write) {
        throw_with("need to prep write first");
    }
    if (m_req_rb.cap() < content.size()) {
        throw_with("too large to write to rb");
    }

    while (m_req_rb.free_size() < content.size()) {
        free_req_rb_space(content.size());
    }

    m_req_rb.write(content);
    m_cache_header.size += content.size();
}

void FrontendQp::post_write() {
    if (not m_is_prep_write) {
        throw_with("need to prep write first");
    }
    m_reserved_header_space->write({(char *)&m_cache_header, sizeof(m_cache_header)});
    ++m_submitted_req_cnt;
    m_is_prep_write = false;
    SPDLOG_INFO("[Frontend-Qp#{}] Post request"
        "[Write: file_num={}, offset={}, write_size={}]"
        "[Req-Rb: free_space={}]",
        m_id, m_cache_header.file_number,
        m_cache_header.offset, m_cache_header.size, m_req_rb.free_size());
    send_req_rb_new_size(sizeof(m_cache_header) + m_cache_header.size);
}

std::vector<int> FrontendQp::wait_all() {
    std::vector<int> slot_ids;

    size_t processed_size{};
    auto wait_req_cnt = m_submitted_req_cnt;
    while (wait_req_cnt > 0) {
        wait_new_resps(wait_req_cnt * sizeof(IoRespMsg), processed_size);
        processed_size = 0;
        
        while (m_resp_rb.size() > 0) {
            auto resp = IoRespMsg::read_from(m_resp_rb);
            switch (resp.type) {
                case IoRespMsg::Type::kWrite:
                    SPDLOG_INFO("[Frontend-Qp#{}] Receive response[Write: file_num={}]",
                        m_id, resp.file_number);
                    if (resp.res < 0) {
                        throw_with("Write error with {}: {}", resp.file_number, strerror(-resp.res));
                    }
                    break;
                case IoRespMsg::Type::kSyncClose:
                    SPDLOG_INFO("[Frontend-Qp#{}] Receive response[SyncClose: file_num={}]",
                        m_id, resp.file_number);
                    if (resp.res < 0) {
                        throw_with("Sync close error with {}: {}", resp.file_number, strerror(-resp.res));
                    }
                    slot_ids.emplace_back(resp.res);
                    break;
                default:
                    throw_with("unsupported type");
            }
            --wait_req_cnt;
            processed_size += sizeof(IoRespMsg);
        }
    }

    m_submitted_req_cnt = 0;
    return slot_ids;
}

void FrontendQp::send_req_rb_new_size(size_t new_size) {
    auto *h2d_rb_meta_msg = (H2DRbMetaMsg *)m_producer->buf_data();
    h2d_rb_meta_msg->type = H2DRbMetaMsg::Type::kReqRbNewSize;
    h2d_rb_meta_msg->payload.req_rb_new_size = new_size;
    m_producer->send_sync(sizeof(*h2d_rb_meta_msg));
}

void FrontendQp::send_req_rb_needed_size(size_t needed_size) {
    auto *h2d_rb_meta_msg = (H2DRbMetaMsg *)m_producer->buf_data();
    h2d_rb_meta_msg->type = H2DRbMetaMsg::Type::kReqRbNeededSpace;
    h2d_rb_meta_msg->payload.req_rb_needed_space = needed_size;
    m_producer->send_sync(sizeof(*h2d_rb_meta_msg));
}

void FrontendQp::send_wait_stat(size_t wait_size, size_t processed_size) {
    auto *h2d_rb_meta_msg = (H2DRbMetaMsg *)m_producer->buf_data();
    h2d_rb_meta_msg->type = H2DRbMetaMsg::Type::kWaitStat;
    h2d_rb_meta_msg->payload.wait_stat.wait_size = wait_size;
    h2d_rb_meta_msg->payload.wait_stat.processed_size = processed_size;
    m_producer->send_sync(sizeof(*h2d_rb_meta_msg));
}

void FrontendQp::free_req_rb_space(size_t needed_size) {
    // 提前 submit 一个 post_recv 任务，等待获取 req_rb_head
    SPDLOG_INFO("[Frontend-Qp#{}] Send meta msg[Req-Rb: free_space={}, needed_size={}]",
        m_id, m_req_rb.free_size(), needed_size);
    m_req_rb_cleaned_size = std::nullopt;
    send_req_rb_needed_size(needed_size);
    while (not m_req_rb_cleaned_size) {
        doca_pe_progress(m_pe);
    }
    m_req_rb.read(*m_req_rb_cleaned_size);
}

void FrontendQp::wait_new_resps(size_t wait_size, size_t processed_size) {
    // 提前 submit 一个 post_recv 任务，等待获取 req_rb_head
    SPDLOG_INFO("[Frontend-Qp#{}] Send meta msg[Resp-Rb: wait_size={}, processed_size={}]",
        m_id, wait_size, processed_size);
    m_resp_rb_new_size = std::nullopt;
    send_wait_stat(wait_size, processed_size);
    while (not m_resp_rb_new_size) {
        doca_pe_progress(m_pe);
    }
    m_resp_rb.rep().write(*m_resp_rb_new_size);
}

} // namespace pulsedb
