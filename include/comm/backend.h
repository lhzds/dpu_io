#pragma once

#include <doca_pe.h>
#include <docacxx/ctx/server.h>
#include <string_view>

#include "bytes.h"
#include "comm/dma_qp_mapping.h"
#include "doca_wrapper/queue_dma.h"
#include "format/ctrl_msg.h"
#include "io/io_engine.h"
#include "noncopyable.h"
#include "types.h"

namespace pulsedb {
/* 
 * 单线程负责控制路径 + 轮询异步的数据操作
 * (其中，dma数据操作卸载到硬件上，文件IO使用基于 iouring 的异步模型）
 * 问题：buf_inv 分配回收的开销？如何优雅地进行批量传输与空间分配回收？
 */
class Backend: Noncopyable {
public:
    Backend(const std::string_view &name, const std::string_view &dev_pci,
        const std::string_view &dev_rep_pci, std::unique_ptr<IoEngine> io_ctx,
        size_t dpu_dma_task_slot_num, size_t dpu_req_rb_cap, size_t dpu_resp_rb_cap);
    ~Backend();

    void run();

    size_t recv_new_conumser_id();

    doca_dev *dev() const { return m_dev; }
    doca_dev_rep *dev_rep() const { return m_dev_rep; }
    QueueDma *dma() const { return m_dma.get(); }
    doca_pe *pe() const { return m_pe; }
    doca_comch_connection *conn() const { return m_conn; }

private:
    void send_ctrl_msg(const ConstBytesView &raw_msg);
    void send_start_comm_msg();
    void send_ack_msg();
    void send_exit_msg();

    template<class CtrlMsgType> CtrlMsgType recv_ctrl_msg();

    void submit_request_to_io_engine(int qp_id, ReqRbMapping::DpuRbView &req_view);

    std::unique_ptr<IoEngine> m_io_ctx;

    bool m_is_exited{};
    doca_dev *m_dev{};
    doca_dev_rep *m_dev_rep{};
    doca_pe *m_pe{};
    doca_comch_connection *m_conn{};
    std::unique_ptr<docacxx::comch::Server> m_ctrl_server;

    std::unique_ptr<QueueDma> m_dma;
    std::vector<std::unique_ptr<DmaQpMapping>> m_qps;

    // tmp var for callback
    bool m_is_ctrl_msg_sended{};
    std::optional<CtrlMsg::RefVariant> m_ctrl_msg_variant{};
    std::optional<size_t> m_new_consumer_id{};
};

} // pulsedb
