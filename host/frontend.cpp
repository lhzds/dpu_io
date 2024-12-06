#include "constant.h"
#include <doca_comch.h>
#include <doca_ctx.h>
#include <doca_pe.h>
#include <optional>
#include <sys/wait.h>

#include "bytes.h"
#include "format.h"
#include "comm/frontend.h"

namespace pulsedb {
Frontend::Frontend(const std::string_view &backend_name, const std::string_view &dev_pci,
        size_t qp_num) {
    docacxx::check_derr(docacxx::open_doca_device_with_pci(dev_pci.data(), &m_dev),
            "failed to open dev");
    docacxx::check_derr(doca_pe_create(&m_pe), "failed to create pe");

    m_ctrl_client = std::make_unique<docacxx::comch::Client>(backend_name.data(), m_dev,
        m_pe, docacxx::comch::Client::Cfg{1}); // 消息同步传输，所以只分配一个task slot
    m_ctrl_client->set_new_consumer_cb([&](doca_comch_connection *,uint32_t id) {
        m_new_consumer_id = id;
    });
    m_ctrl_client->set_send_cb([&](auto) {
        m_is_ctrl_msg_sended = true;
    });
    m_ctrl_client->set_recv_cb([&](doca_comch_connection *conn, uint8_t *data, uint32_t len) {
        ConstBytesView bytes((char *)data, len);
        m_ctrl_msg_variant = CtrlMsg::read(bytes);
    });
    m_ctrl_client->start_sync();
    m_conn = m_ctrl_client->get_connection();

    send_init_msg(qp_num);
    recv_ctrl_msg<CtrlMsg::AckMsg>();

    m_qps.reserve(qp_num);
    for (size_t i{}; i < qp_num; ++i) {
        m_qps.emplace_back(std::make_unique<FrontendQp>(*this, i, HOST_REQ_RB_CAP, HOST_RESP_RB_CAP));
        auto &qp = *m_qps.back();

        send_init_qp_msg(i, qp.req_rb_ref().data(), qp.req_rb_ref().size(),
            qp.resp_rb_ref().data(), qp.resp_rb_ref().size(),
            qp.req_mmap_ref().export_pci(m_dev), qp.resp_mmap_ref().export_pci(m_dev));
        recv_ctrl_msg<CtrlMsg::AckMsg>();
    }
}

Frontend::~Frontend() {
    m_qps.clear();
    m_ctrl_client->clean_sync();
    send_exit_msg();
}

template<class CtrlMsgType>
CtrlMsgType Frontend::recv_ctrl_msg() {
    while (not m_ctrl_msg_variant) {
        doca_pe_progress(m_pe);
    }

    auto ret = std::get<CtrlMsgType>(*m_ctrl_msg_variant);
    m_ctrl_msg_variant = std::nullopt;
    return ret;
}

void Frontend::send_ctrl_msg(ConstBytesView raw_msg) {
    m_is_ctrl_msg_sended = false;
    auto task = m_ctrl_client->alloc_send_task(m_conn, raw_msg.data(), raw_msg.size(), nullptr);
    auto ret = m_ctrl_client->submit(std::move(task));
    while (ret) {
        doca_pe_progress(m_pe);
        ret = m_ctrl_client->submit(std::move(task));
    }
    while (not m_is_ctrl_msg_sended) {
        doca_pe_progress(m_pe);
    }
}

void Frontend::send_init_msg(size_t qp_num) {
    send_ctrl_msg(CtrlMsg::make_init_msg(qp_num));
}

void Frontend::send_init_qp_msg(size_t qp_id,
    char *host_req_mem_addr, size_t host_req_mem_size,
    char *host_resp_mem_addr, size_t host_resp_mem_size,
    const ConstBytesView &req_export_desc, const ConstBytesView &resp_export_desc) {
    auto init_qp_msg = CtrlMsg::make_init_qp_msg(qp_id, 
        host_req_mem_addr, host_req_mem_size,
        host_resp_mem_addr, host_resp_mem_size,
        req_export_desc, resp_export_desc);
    send_ctrl_msg(init_qp_msg);
}

void Frontend::send_exit_msg() {
    send_ctrl_msg(CtrlMsg::make_exit_msg());
}

size_t Frontend::recv_new_conumser_id() {
    while (not m_new_consumer_id) {
        doca_pe_progress(m_pe);
    }
    auto ret = *m_new_consumer_id;
    m_new_consumer_id = std::nullopt;
    return ret;
}
}
