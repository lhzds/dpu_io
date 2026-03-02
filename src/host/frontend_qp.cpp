#include <cstring>
#include <doca_pe.h>
#include <optional>

#include "bytes.h"
#include "comm/frontend.h"
#include "comm/frontend_qp.h"
#include "constant.h"
#include "doca_wrapper/passive_consumer.h"
#include "format/rb_meta_msg.h"
#include "format/rb_req_resp_msg.h"
#include "util.h"

namespace pulsedb {
FrontendQp::FrontendQp(Frontend &frontend, int id,
    size_t req_rb_cap, size_t resp_rb_cap)
    : m_id(id),
      m_req_rb(req_rb_cap, frontend.direct_alignment()),
      m_resp_rb(resp_rb_cap, frontend.direct_alignment()),
      m_write_alignment(frontend.direct_alignment()) {
    if (m_write_alignment == 0 || (m_write_alignment & (m_write_alignment - 1)) != 0) {
        throw_with("FrontendQp: Invalid direct alignment={}", m_write_alignment);
    }
    docacxx::check_derr(doca_pe_create(&m_pe), "failed to create pe");
    
    m_req_mmap = std::make_unique<docacxx::Mmap>(
        frontend.dev(), m_req_rb.data(), m_req_rb.cap(),
        DOCA_ACCESS_FLAG_PCI_READ_WRITE
    );
    m_resp_mmap = std::make_unique<docacxx::Mmap>(
        frontend.dev(), m_resp_rb.data(), m_resp_rb.cap(),
        DOCA_ACCESS_FLAG_PCI_READ_WRITE
    );
    SPDLOG_INFO(
        "[Frontend-Qp#{}] Create[req_rb_cap={}, resp_rb_cap={}]",
        id, m_req_rb.cap(), m_resp_rb.cap()
    );
};

FrontendQp::~FrontendQp() {
    m_producer->clean_sync();
}

void FrontendQp::create_producer(Frontend &frontend) {
    size_t remote_consumer_id = frontend.recv_new_conumser_id();
    m_producer = std::make_unique<SyncSingleTaskProducer>(
        frontend.conn(), m_pe, frontend.dev(),
        kCommRbMetaSize, remote_consumer_id
    );
    m_producer->start_sync();
}

void FrontendQp::create_consumer(Frontend &frontend) {
    PassiveConsumer::Cfg cfg;
    cfg.max_msg_size = kCommRbMetaSize;
    // 尽管同步传输，但 task slot 数仍需大于 1
    // 以防止单个 post_recv 任务准备过慢，导致对端 resubmit send_task 次数过多
    cfg.post_recv_task_slot_num = 5;
    m_consumer = std::make_unique<PassiveConsumer>(frontend.conn(), m_pe,
        frontend.pe(), frontend.dev(), cfg, [&](const auto &view) {
        auto *msg = (D2HRbMetaMsg *)view.data();

        switch (msg->type) {
            case D2HRbMetaMsg::Type::kReqRbCleanedSize:
                SPDLOG_INFO(
                    "[Frontend-Qp#{}] Receive meta msg[ReqRbCleanedSize: cleaned_size={}]",
                    m_id, msg->payload.req_rb_cleaned_size
                );
                m_req_rb_cleaned_size = msg->payload.req_rb_cleaned_size;
                break;
            case D2HRbMetaMsg::Type::kRespRbNewSize:
                SPDLOG_INFO(
                    "[Frontend-Qp#{}] Receive meta msg[RespRbNewSize: new_size={}]",
                    m_id, msg->payload.resp_rb_new_size
                );
                m_resp_rb_new_size = msg->payload.resp_rb_new_size;
                break;
            default:
                throw_with("FrontendQp: Unexpected D2HRbMetaMsg type");
        }
    });
}

void FrontendQp::post_range_sync(size_t file_number, size_t offset,
                                size_t size, bool wait_before) {
    if (is_preping_write()) {
        throw_with("FrontendQp: Preparing write now");
    }
    auto req = IoReqMsg::make_range_sync_req(file_number, offset, size, wait_before);
    if (m_req_rb.cap() < req.size()) {
        throw_with("FrontendQp: Too large to write");
    }

    while (m_req_rb.free_size() < req.size()) {
        free_req_rb_space(req.size() - m_req_rb.free_size());
    }

    m_req_rb.write(req);
    SPDLOG_INFO(
        "[Frontend-Qp#{}] Post request[RangeSync: file_num={}, offset={}, size={}]"
        "[Req-Rb: free_space={}]", m_id, file_number, offset, size, m_req_rb.free_size()
    );
    send_req_rb_new_size(req.size());
}

void FrontendQp::post_sync_close(size_t file_number, bool use_fsync) {
    if (is_preping_write()) {
        throw_with("FrontendQp: Preparing write now");
    }
    auto req = IoReqMsg::make_sync_close_req(file_number, use_fsync);
    if (m_req_rb.cap() < req.size()) {
        throw_with("FrontendQp: Too large to write");
    }

    while (m_req_rb.free_size() < req.size()) {
        free_req_rb_space(req.size() - m_req_rb.free_size());
    }

    m_req_rb.write(req);
    ++m_sync_close_cnt;
    SPDLOG_INFO(
        "[Frontend-Qp#{}] Post request[SyncClose: file_num={}]"
        "[Req-Rb: free_space={}]", m_id, file_number, m_req_rb.free_size()
    );
    send_req_rb_new_size(req.size());
}

void FrontendQp::prep_write(size_t file_number, size_t offset, int base_size_multiplier,
    bool is_direct) {
    if (is_preping_write()) {
        throw_with("FrontendQp: Prepare write doubly");
    }
    m_cur_write_req_start_offset = m_req_rb.tail();
    size_t direct_padding_len{};
    size_t header_size = sizeof(IoReqMsg::WriteHeader);
    size_t req_prefix_size = header_size;
    if (is_direct) {
        size_t payload_offset = (m_cur_write_req_start_offset + header_size) % m_req_rb.cap();
        direct_padding_len =
            (m_write_alignment - (payload_offset % m_write_alignment)) % m_write_alignment;
        req_prefix_size += direct_padding_len;
    }
    IoReqMsg::init_write_req_header(m_cache_header, file_number, offset,
        base_size_multiplier, is_direct, static_cast<uint32_t>(direct_padding_len));

    if (m_req_rb.cap() < req_prefix_size) {
        throw_with("FrontendQp: Too large to write");
    }

    while (m_req_rb.free_size() < req_prefix_size) {
        free_req_rb_space(req_prefix_size - m_req_rb.free_size());
    }

    auto reserved_prefix_space = m_req_rb.reserve_space(req_prefix_size);
    if (!reserved_prefix_space) {
        throw_with("FrontendQp: Failed to reserve request prefix space");
    }
    m_reserved_req_prefix_space = reserved_prefix_space->cut(header_size);
    m_cur_write_payload_start_offset =
        (m_cur_write_req_start_offset + req_prefix_size) % m_req_rb.cap();
    m_cur_write_req_size += req_prefix_size;
}

void FrontendQp::append(const ConstBytesView &content) {
    if (not is_preping_write()) {
        throw_with("FrontendQp: Prepare write first");
    }
    if (m_req_rb.cap() < content.size()) {
        throw_with("FrontendQp: Too large to write");
    }

    while (m_req_rb.free_size() < content.size()) {
        free_req_rb_space(content.size() - m_req_rb.free_size());
    }

    m_req_rb.write(content);
    m_cache_header.size += content.size();
    m_cur_write_req_size += content.size();
}

void FrontendQp::post_write() {
    if (not is_preping_write()) {
        throw_with("FrontendQp: Prepare write first");
    }
    size_t direct_padding_len = static_cast<size_t>(m_cache_header.payload_padding_len);
    if (m_cache_header.is_direct) {
        if ((m_cache_header.offset % m_write_alignment) != 0) {
            throw_with("FrontendQp: Direct write offset not aligned[offset={}, alignment={}]",
                m_cache_header.offset, m_write_alignment);
        }
        if ((m_cache_header.size % m_write_alignment) != 0) {
            throw_with("FrontendQp: Direct write size not aligned[size={}, alignment={}]",
                m_cache_header.size, m_write_alignment);
        }
        size_t payload_offset = (m_cur_write_req_start_offset + sizeof(m_cache_header))
            % m_req_rb.cap();
        auto expected_padding_len =
            (m_write_alignment - (payload_offset % m_write_alignment)) % m_write_alignment;
        if (direct_padding_len != expected_padding_len) {
            throw_with("FrontendQp: Direct padding mismatch[padding={}, expected={}]",
                direct_padding_len, expected_padding_len);
        }
    } else if (direct_padding_len != 0) {
        throw_with("FrontendQp: Unexpected non-zero padding for non-direct write[padding={}]",
            direct_padding_len);
    }

    if (!m_reserved_req_prefix_space) {
        throw_with("FrontendQp: Missing reserved header space");
    }
    m_reserved_req_prefix_space->write({(char *)&m_cache_header, sizeof(m_cache_header)});

    size_t req_size = sizeof(m_cache_header) + direct_padding_len + m_cache_header.size;
    if (m_cache_header.is_direct) {
        size_t req_end_offset = (m_cur_write_req_start_offset + req_size) % m_req_rb.cap();
        if ((req_end_offset % m_write_alignment) != 0) {
            throw_with("FrontendQp: Direct request end not aligned[end_offset={}, alignment={}]",
                req_end_offset, m_write_alignment);
        }
    }

    SPDLOG_INFO(
        "[Frontend-Qp#{}] Post request[Write: file_num={}, offset={}, write_size={}]"
        "[Req-Rb: free_space={}]", m_id, m_cache_header.file_number,
        m_cache_header.offset, m_cache_header.size, m_req_rb.free_size()
    );
    send_req_rb_new_size(req_size);
    m_reserved_req_prefix_space = std::nullopt;
    m_cur_write_req_size = 0;
    m_cur_write_req_start_offset = 0;
    m_cur_write_payload_start_offset = 0;
}

bool FrontendQp::copy_cur_write_payload_tail(size_t n, char *dst) const {
    if (!is_preping_write()) {
        return false;
    }
    if (n > m_cache_header.size) {
        return false;
    }
    if (n == 0) {
        return true;
    }
    size_t tail_offset =
        (m_cur_write_payload_start_offset + m_cache_header.size - n) % m_req_rb.cap();
    size_t first_part_size = std::min(n, m_req_rb.cap() - tail_offset);
    std::memcpy(dst, m_req_rb.data() + tail_offset, first_part_size);
    if (first_part_size < n) {
        std::memcpy(dst + first_part_size, m_req_rb.data(), n - first_part_size);
    }
    return true;
}

bool FrontendQp::trim_cur_write_payload_tail(size_t n) {
    if (!is_preping_write()) {
        return false;
    }
    if (n > m_cache_header.size) {
        return false;
    }
    if (n == 0) {
        return true;
    }
    if (!m_req_rb.rewind(n)) {
        return false;
    }
    m_cache_header.size -= n;
    m_cur_write_req_size -= n;
    return true;
}

bool FrontendQp::discard_prepared_write() {
    if (!is_preping_write()) {
        return false;
    }
    if (!m_req_rb.rewind(m_cur_write_req_size)) {
        return false;
    }
    m_reserved_req_prefix_space = std::nullopt;
    m_cur_write_req_size = 0;
    m_cur_write_req_start_offset = 0;
    m_cur_write_payload_start_offset = 0;
    m_cache_header = IoReqMsg::WriteHeader{};
    return true;
}

std::vector<std::pair<int, int>> FrontendQp::wait_all() {
    std::vector<std::pair<int, int>> file_map;

    size_t processed_size{};
    auto wait_req_cnt = m_sync_close_cnt;
    while (wait_req_cnt > 0) {
        size_t msg_offset = m_resp_rb.head();
        wait_new_resps(wait_req_cnt * sizeof(IoRespMsg), processed_size);
        processed_size = 0;
        
        while (m_resp_rb.size() > 0) {
            auto resp = IoRespMsg::read_from(m_resp_rb);
            switch (resp.type) {
                // case IoRespMsg::Type::kWrite:
                //     SPDLOG_INFO(
                //         "[Frontend-Qp#{}] Read response(offset={})"
                //         "[Write: file_num={}, res={}]",
                //         m_id, msg_offset, resp.file_number, resp.res
                //     );
                //     if (resp.res < 0) {
                //         throw_with(
                //             "FrontendQp: Write error on {}: {}",
                //             resp.file_number, strerror(-resp.res)
                //         );
                //     }
                //     break;
                // case IoRespMsg::Type::kRangeSync:
                //     SPDLOG_INFO(
                //         "[Frontend-Qp#{}] Read response(offset={})"
                //         "[RangeSync: file_num={}, res={}]",
                //         m_id, msg_offset, resp.file_number, resp.res
                //     );
                //     if (resp.res < 0) {
                //         throw_with(
                //             "FrontendQp: Range sync error on {}: {}",
                //             resp.file_number, strerror(-resp.res)
                //         );
                //     }
                //     break;
                case IoRespMsg::Type::kSyncClose:
                    SPDLOG_INFO(
                        "[Frontend-Qp#{}] Read response(offset={})"
                        "[SyncClose: file_num={}, res={}]",
                        m_id, msg_offset, resp.file_number, resp.res
                    );
                    if (resp.res < 0) {
                        throw_with(
                            "FrontendQp: Sync close error on {}: {}",
                            resp.file_number, strerror(-resp.res)
                        );
                    }
                    file_map.emplace_back(resp.file_number, resp.res);
                    break;
                default:
                    SPDLOG_INFO(
                        "[Frontend-Qp#{}] Read response(offset={})[Unexpected Type]",
                        m_id, msg_offset, m_resp_rb.head(), m_resp_rb.tail()
                    );
                    throw_with("FrontendQp: Unexpected response type");
            }
            --wait_req_cnt;
            processed_size += sizeof(IoRespMsg);
            msg_offset += sizeof(IoRespMsg);
        }
    }
    if (processed_size > 0) {
        send_wait_stat(0, processed_size);
    }

    m_sync_close_cnt = 0;
    return file_map;
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
    SPDLOG_INFO(
        "[Frontend-Qp#{}] Send meta msg"
        "[ReqRbNeededSize: free_space={}, needed_size={}]",
        m_id, m_req_rb.free_size(), needed_size
    );
    m_req_rb_cleaned_size = std::nullopt;
    send_req_rb_needed_size(needed_size);
    while (not m_req_rb_cleaned_size) {
        doca_pe_progress(m_pe);
    }
    m_req_rb.read(*m_req_rb_cleaned_size);
}

void FrontendQp::wait_new_resps(size_t wait_size, size_t processed_size) {
    // 提前 submit 一个 post_recv 任务，等待获取 req_rb_head
    SPDLOG_INFO(
        "[Frontend-Qp#{}] Send meta msg"
        "[WaitStat: wait_size={}, processed_size={}]",
        m_id, wait_size, processed_size
    );
    m_resp_rb_new_size = std::nullopt;
    send_wait_stat(wait_size, processed_size);
    while (not m_resp_rb_new_size) {
        doca_pe_progress(m_pe);
    }
    m_resp_rb.rep().write(*m_resp_rb_new_size);
}

} // namespace pulsedb
