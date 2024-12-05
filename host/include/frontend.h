#pragma once

#include <memory>
#include <optional>

#include "bytes.h"
#include "docacxx/ctx/client.h"
#include "format.h"
#include "frontend_qp.h"
#include "noncopyable.h"

namespace pulsedb {
class FrontendQp;

// 非线程安全
class Frontend: Noncopyable {
public:
    Frontend(const std::string_view &backend_name, const std::string_view &dev_pci,
        size_t qp_num);
    ~Frontend();

    // control path
    void send_init_msg(size_t qp_num);
    void send_init_qp_msg(size_t qp_id,
        char *host_req_mem_addr, size_t host_req_mem_size,
        char *host_resp_mem_addr, size_t host_resp_mem_size,
        const ConstBytesView &req_export_desc, const ConstBytesView &resp_export_desc);
    template<class CtrlMsgType> CtrlMsgType recv_ctrl_msg();
    size_t recv_new_conumser_id(); 

    FrontendQp &qp_ref(size_t qp_id) { return *m_qps[qp_id]; }
    doca_dev *dev() { return m_dev; }
    doca_pe *pe() { return m_pe; }
    doca_comch_connection *conn() { return m_conn; }

private:
    void send_ctrl_msg(ConstBytesView raw_msg);

    doca_dev *m_dev{};
    doca_pe *m_pe{};
    doca_comch_connection *m_conn{};
    std::unique_ptr<docacxx::comch::Client> m_ctrl_client{};
    std::vector<std::unique_ptr<FrontendQp>> m_qps;

    // tmp var for callback
    bool m_is_ctrl_msg_sended{};
    std::optional<CtrlMsg::Variant> m_ctrl_msg_variant{};
    std::optional<size_t> m_new_consumer_id{};
};
}
