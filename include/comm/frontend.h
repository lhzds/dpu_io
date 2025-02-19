#pragma once

#include <memory>
#include <optional>

#include "bytes.h"
#include "docacxx/ctx/client.h"
#include "format/ctrl_msg.h"
#include "frontend_qp.h"
#include "noncopyable.h"

namespace pulsedb {
class FrontendQp;

class Frontend: Noncopyable {
public:
    Frontend(const std::string_view &backend_name, const std::string_view &dev_pci,
        int qp_num, size_t req_rb_cap, size_t resp_rb_cap);
    ~Frontend();

    void send_init_msg(int qp_num);
    void send_init_qp_msg(int qp_id,
        char *host_req_mem_addr, size_t host_req_mem_size,
        char *host_resp_mem_addr, size_t host_resp_mem_size,
        const ConstBytesView &req_export_desc, const ConstBytesView &resp_export_desc);
    void send_exit_msg();

    template<class CtrlMsgTypeRef> CtrlMsgTypeRef recv_ctrl_msg();
    size_t recv_new_conumser_id(); 

    FrontendQp *alloc_qp();
    void free_qp(FrontendQp *qp);


    doca_dev *dev() { return m_dev; }
    doca_pe *pe() { return m_pe; }
    doca_comch_connection *conn() { return m_conn; }

private:
    void send_ctrl_msg(ConstBytesView raw_msg);

    doca_dev *m_dev{};
    doca_pe *m_pe{};
    doca_comch_connection *m_conn{};
    std::unique_ptr<docacxx::comch::Client> m_ctrl_client{};
    
    int m_start_alloc_idx{};
    std::vector<std::unique_ptr<FrontendQp>> m_qps;

    // tmp var for callback
    bool m_is_ctrl_msg_sended{};
    std::optional<CtrlMsg::RefVariant> m_ctrl_msg_ref_var{};
    std::optional<size_t> m_new_consumer_id{};
};
}
