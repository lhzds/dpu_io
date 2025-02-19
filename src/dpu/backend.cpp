#include "comm/backend.h"
#include "bytes.h"
#include "constant.h"
#include "comm/dma_qp_mapping.h"
#include "docacxx/ctx/server.h"
#include "format/ctrl_msg.h"
#include "format/rb_req_resp_msg.h"
#include "io/io_ctx.h"

#include <doca_dev.h>
#include <doca_pe.h>
#include <memory>
#include <spdlog/spdlog.h>

namespace pulsedb {
    
Backend::Backend(const std::string_view &name, const std::string_view &dev_pci,
    const std::string_view &dev_rep_pci, std::unique_ptr<IoCtx> io_ctx,
    size_t dpu_req_rb_cap, size_t dpu_resp_rb_cap) {
    SPDLOG_INFO("[Backend] Starting...");
    m_io_ctx = std::move(io_ctx);

    docacxx::check_derr(docacxx::open_doca_device_with_pci(dev_pci.data(), &m_dev),
        "failed to open dev");
    docacxx::check_derr(docacxx::open_doca_device_rep_with_pci(m_dev, DOCA_DEVINFO_REP_FILTER_NET,
        dev_rep_pci.data(), &m_dev_rep), "failed to open dev rep");
    docacxx::check_derr(doca_pe_create(&m_pe), "failed to create pe");

    m_dma = std::make_unique<QueueDma>(m_pe, m_dev, kDpuDmaTaskSlotNum);
    m_dma->start_sync();

    docacxx::comch::Server::Cfg server_cfg;
    server_cfg.max_send_task_slot_num = 1; // 消息同步传输，所以只分配一个task slot
    m_ctrl_server = std::make_unique<docacxx::comch::Server>(name.data(), m_dev, m_dev_rep,
        m_pe, server_cfg);
    m_ctrl_server->set_new_conn_cb([&](doca_comch_connection *conn) {
        m_conn = conn;
    });
    m_ctrl_server->set_new_consumer_cb([&](doca_comch_connection *conn, uint32_t id) {
        m_new_consumer_id = id;
    });
    m_ctrl_server->set_send_cb([&](auto) {
        m_is_ctrl_msg_sended = true;
    });
    m_ctrl_server->set_recv_cb([&](doca_comch_connection *conn, uint8_t *data, uint32_t len) {
        ConstBytesView bytes((char *)data, len);
        m_ctrl_msg_variant = CtrlMsg::read(bytes);
    });
    m_ctrl_server->start_sync();
    
    // 等待客户端连接
    while (not m_conn) {
        doca_pe_progress(m_pe);
    }

    auto qp_num = recv_ctrl_msg<CtrlMsg::InitMsgRef>().header->qp_num;
    send_ack_msg();

    m_qps.reserve(qp_num);
    for (int i{}; i < qp_num; ++i) {
        auto init_qp_msg = recv_ctrl_msg<CtrlMsg::InitQpMsgRef>();
        send_ack_msg();
        auto *header = init_qp_msg.header;
        
        // 使用 1:1 大小的队列映射
        m_qps.emplace_back(std::make_unique<DmaQpMapping>(*this, i,
            dpu_req_rb_cap,
            ConstBytesView(init_qp_msg.host_req_mmap_desc, header->host_req_mmap_desc_size),
            header->host_req_mem_addr, header->host_req_mem_size,

            dpu_resp_rb_cap,
            ConstBytesView(init_qp_msg.host_resp_mmap_desc, header->host_resp_mmap_desc_size),
            header->host_resp_mem_addr, header->host_resp_mem_size,

            [&](int qp_id, ReqRbMapping::DpuRbView &req_view) { connect_io_and_qp(qp_id, req_view); }
        ));

        auto &qp = m_qps.back();
        qp->create_consumer(*this);
        qp->create_producer(*this);
    }
    SPDLOG_INFO("[Backend] Create {} qps", qp_num);

    // 运行时只接受 exit 退出消息
    m_ctrl_server->set_recv_cb([&](doca_comch_connection *conn, uint8_t *data, uint32_t len) {
        CtrlMsg::Type msg_type;
        ConstBytesView bytes((char *)data, len);
        m_ctrl_msg_variant = CtrlMsg::read(bytes);
        std::visit([&](const auto &msg) {
            msg_type = msg.header->type;
        }, *m_ctrl_msg_variant);
        if (msg_type != CtrlMsg::Type::kExit) {
            throw_with("wrong msg type");
        }
        SPDLOG_INFO("[Backend] Receive exit msg");
        m_is_exited = true;
    });
    send_start_comm_msg();
}

Backend::~Backend() {
    SPDLOG_INFO("[Backend] Exiting...");
    m_qps.clear();
    m_dma->clean_sync();
    m_ctrl_server->clean_sync();
    
    if (auto result = doca_pe_destroy(m_pe); result != DOCA_SUCCESS) {
        std::cerr << "Failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_rep_close(m_dev_rep); result != DOCA_SUCCESS) {
        std::cerr << "Failed to close dev rep: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_close(m_dev); result != DOCA_SUCCESS) {
        std::cerr << "Failed to close dev: " << doca_error_get_descr(result) << std::endl;
    }
}

void Backend::run() {
    std::function<void()> msg;
    while (not m_is_exited) {
        doca_pe_progress(m_pe);
        while (m_io_mq.pop(msg)) {
            msg();
        }
    }
}

template<class CtrlMsgType>
CtrlMsgType Backend::recv_ctrl_msg() {
    while (not m_ctrl_msg_variant) {
        doca_pe_progress(m_pe);
    }

    auto ret = std::get<CtrlMsgType>(*m_ctrl_msg_variant);
    m_ctrl_msg_variant = std::nullopt;
    if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::AckMsgRef>) {
        SPDLOG_INFO("[Backend] Receive ack msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::InitMsgRef>) {
        SPDLOG_INFO("[Backend] Receive init msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::InitQpMsgRef>) {
        SPDLOG_INFO("[Backend] Receive init_qp msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::ExitMsgRef>) {
        SPDLOG_INFO("[Backend] Receive exit msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::StartCommMsgRef>) {
        SPDLOG_INFO("[Backend] Receive start_comm msg");
    }
    return ret;
}

void Backend::send_ctrl_msg(const ConstBytesView &raw_msg) {
    m_is_ctrl_msg_sended = false;
    auto task = m_ctrl_server->alloc_send_task(m_conn, raw_msg.data(), raw_msg.size(), nullptr);
    auto ret = m_ctrl_server->submit(std::move(task));
    while (ret) {
        doca_pe_progress(m_pe);
        ret = m_ctrl_server->submit(std::move(ret));
    }
    while (not m_is_ctrl_msg_sended) {
        doca_pe_progress(m_pe);
    }
}

void Backend::send_ack_msg() {
    send_ctrl_msg(CtrlMsg::make_ack_msg());
    SPDLOG_INFO("[Backend] Send ack msg");
}

void Backend::send_start_comm_msg() {
    send_ctrl_msg(CtrlMsg::make_start_comm_msg());
    SPDLOG_INFO("[Backend] Send start_comm msg");
}

size_t Backend::recv_new_conumser_id() {
    while (not m_new_consumer_id) {
        doca_pe_progress(m_pe);
    }
    auto ret = *m_new_consumer_id;
    m_new_consumer_id = std::nullopt;
    SPDLOG_INFO("[Backend] Receive new_consumer_id={}", *m_new_consumer_id);
    return ret;
}

void Backend::connect_io_and_qp(int qp_id, ReqRbMapping::DpuRbView &req_view) {
    auto ref_var = IoReqMsg::parse_from(req_view.inner);
    IoReqMsg::Type type;
    std::visit([&](const auto &r) {
        type = r->header->type;
    }, ref_var);

    switch (type) {
        case IoReqMsg::Type::kWrite: {
            auto write_ref = IoReqMsg::get_req_ptr<IoReqMsg::WriteMsgRbRef>(ref_var);
            auto &header = *write_ref->header;
            SPDLOG_INFO("[Backend] Submit request from qp_id={} to I/O engine"
                "[Write: file_number={}, offset={}, size={}]",
                qp_id, header.file_number, header.offset, header.size);
            auto *cb = IoCtx::CallbackFn::make([&, qp_id, file_number = header.file_number](int res) {
                SPDLOG_INFO("[Backend] Complete request from qp_id={}"
                    "[Write: file_number={}]",
                    qp_id, file_number);
                auto msg = IoRespMsg::make(IoRespMsg::Type::kWrite, file_number, res);
                m_qps[qp_id]->finish_req(req_view, std::move(msg));
            });
            m_io_ctx->write_async(header.file_number, header.offset, 
                header.base_size_multiplier, *write_ref->content, cb);
            break;
        }
        case IoReqMsg::Type::kSyncClose: {
            auto sync_close_ref = IoReqMsg::get_req_ptr<IoReqMsg::SyncCloseMsgRbRef>(ref_var);
            auto &header = *sync_close_ref->header;
            SPDLOG_INFO("[Backend] Submit request from qp_id={} to I/O engine[SyncClose: file_number={}]",
                qp_id, header.file_number);
            auto *cb = IoCtx::CallbackFn::make([&, qp_id, file_number = header.file_number](int res) {
                SPDLOG_INFO("[Backend] Complete request from qp_id={}[SyncClose: file_number={}]",
                    qp_id, file_number);
                auto msg = IoRespMsg::make(IoRespMsg::Type::kSyncClose, file_number, res);
                m_qps[qp_id]->finish_req(req_view, std::move(msg));
            });
            m_io_ctx->sync_close_async(header.file_number, header.use_fsync, cb);
            break;
        }
        default: {
            throw_with("unexpected type");
        }
    }
}

} // namespace pulsedb
