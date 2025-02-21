#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_pe.h>
#include <optional>
#include <spdlog/spdlog.h>
#include <sys/wait.h>

#include "bytes.h"
#include "comm/frontend.h"

namespace pulsedb {
Frontend::Frontend(const std::string_view &backend_name, const std::string_view &dev_pci,
        int qp_num, size_t req_rb_cap, size_t resp_rb_cap) {
    SPDLOG_INFO("[Frontend] Starting...");
    docacxx::check_derr(docacxx::open_doca_device_with_pci(dev_pci.data(), &m_dev),
            "failed to open dev");
    docacxx::check_derr(doca_pe_create(&m_pe), "failed to create pe");

    docacxx::comch::Client::Cfg client_cfg;
    client_cfg.max_send_task_slot_num = 1; // 消息同步传输，所以只分配一个task slot
    m_ctrl_client = std::make_unique<docacxx::comch::Client>(backend_name.data(), m_dev,
        m_pe, client_cfg);
    m_ctrl_client->set_new_consumer_cb([&](doca_comch_connection *,uint32_t id) {
        m_new_consumer_id = id;
    });
    m_ctrl_client->set_send_cb([&](auto) {
        m_is_ctrl_msg_sended = true;
    });
    m_ctrl_client->set_recv_cb([&](doca_comch_connection *conn, uint8_t *data, uint32_t len) {
        ConstBytesView bytes((char *)data, len);
        m_ctrl_msg_ref_var = CtrlMsg::read(bytes);
    });
    m_ctrl_client->start_sync();
    m_conn = m_ctrl_client->get_connection();

    send_init_msg(qp_num);
    recv_ctrl_msg<CtrlMsg::AckMsgRef>();

    m_qps.reserve(qp_num);
    for (int i{}; i < qp_num; ++i) {
        m_qps.emplace_back(std::make_unique<FrontendQp>(*this, i, req_rb_cap, resp_rb_cap));
        auto &qp = *m_qps.back();

        send_init_qp_msg(i, qp.req_rb_ref().data(), qp.req_rb_ref().cap(),
            qp.resp_rb_ref().data(), qp.resp_rb_ref().cap(),
            qp.req_mmap_ref().export_pci(m_dev), qp.resp_mmap_ref().export_pci(m_dev));
        recv_ctrl_msg<CtrlMsg::AckMsgRef>();

        qp.create_producer(*this);
        qp.create_consumer(*this);
    }
    SPDLOG_INFO("[Frontend] Create {} qps", qp_num);
    recv_ctrl_msg<CtrlMsg::StartCommMsgRef>();
}

Frontend::~Frontend() {
    SPDLOG_INFO("[Frontend] Exiting...");
    send_exit_msg();
    m_qps.clear();
    m_ctrl_client->clean_sync();

    if (auto result = doca_pe_destroy(m_pe); result != DOCA_SUCCESS) {
        std::cerr << "failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_close(m_dev); result != DOCA_SUCCESS) {
        std::cerr << "failed to close dev: " << doca_error_get_descr(result) << std::endl;
    }
}

template<class CtrlMsgRefType>
CtrlMsgRefType Frontend::recv_ctrl_msg() {
    while (not m_ctrl_msg_ref_var) {
        doca_pe_progress(m_pe);
    }

    auto ret = std::get<CtrlMsgRefType>(*m_ctrl_msg_ref_var);
    m_ctrl_msg_ref_var = std::nullopt;
    if constexpr (std::is_same_v<CtrlMsgRefType, CtrlMsg::AckMsgRef>) {
        SPDLOG_INFO("[Frontend] Receive ack msg");
    } else if constexpr (std::is_same_v<CtrlMsgRefType, CtrlMsg::InitMsgRef>) {
        SPDLOG_INFO("[Frontend] Receive init msg");
    } else if constexpr (std::is_same_v<CtrlMsgRefType, CtrlMsg::InitQpMsgRef>) {
        SPDLOG_INFO("[Frontend] Receive init_qp msg");
    } else if constexpr (std::is_same_v<CtrlMsgRefType, CtrlMsg::ExitMsgRef>) {
        SPDLOG_INFO("[Frontend] Receive exit msg");
    } else if constexpr (std::is_same_v<CtrlMsgRefType, CtrlMsg::StartCommMsgRef>) {
        SPDLOG_INFO("[Frontend] Receive start_comm msg");
    }
    return ret;
}

void Frontend::send_ctrl_msg(ConstBytesView raw_msg) {
    m_is_ctrl_msg_sended = false;
    auto task = m_ctrl_client->alloc_send_task(m_conn, raw_msg.data(), raw_msg.size(), nullptr);
    auto ret = m_ctrl_client->submit(std::move(task));
    while (ret) {
        doca_pe_progress(m_pe);
        ret = m_ctrl_client->submit(std::move(ret));
    }
    while (not m_is_ctrl_msg_sended) {
        doca_pe_progress(m_pe);
    }
}

void Frontend::send_init_msg(int qp_num) {
    send_ctrl_msg(CtrlMsg::make_init_msg(qp_num));
    SPDLOG_INFO("[Frontend] Send init msg[qp_num={}]", qp_num);
}

void Frontend::send_init_qp_msg(int qp_id,
    char *host_req_rb_data, size_t host_req_rb_cap,
    char *host_resp_rb_data, size_t host_resp_rb_cap,
    const ConstBytesView &req_export_desc, const ConstBytesView &resp_export_desc) {
    auto init_qp_msg = CtrlMsg::make_init_qp_msg(qp_id, 
        host_req_rb_data, host_req_rb_cap,
        host_resp_rb_data, host_resp_rb_cap,
        req_export_desc, resp_export_desc);
    send_ctrl_msg(init_qp_msg);
    SPDLOG_INFO("[Frontend] Send init_qp msg[qp_id={}, host_req_rb_cap={}, host_resp_rb_cap={}]",
        qp_id, host_req_rb_cap, host_resp_rb_cap);
}

void Frontend::send_exit_msg() {
    send_ctrl_msg(CtrlMsg::make_exit_msg());
    SPDLOG_INFO("[Frontend] Send exit msg");
}

size_t Frontend::recv_new_conumser_id() {
    while (not m_new_consumer_id) {
        doca_pe_progress(m_pe);
    }
    auto ret = *m_new_consumer_id;
    m_new_consumer_id = std::nullopt;
    SPDLOG_INFO("[Frontend] Rceive new consumer id={}", *m_new_consumer_id);
    return ret;
}

FrontendQp *Frontend::alloc_qp() {
    FrontendQp *ret{};
    while (ret == nullptr) {
      bool test = false;
      auto &qp = *m_qps[m_start_alloc_idx];
      if (qp.is_busy.compare_exchange_strong(test, true)) {
        ret = &qp;
      }
      if ((++m_start_alloc_idx) == m_qps.size()) {
        m_start_alloc_idx = 0;
      }
    }
    return ret;
}

void free_qp(FrontendQp *qp) {
    qp->is_busy = false;
}

} // namespace pulsedb
