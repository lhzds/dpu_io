#pragma once

#include <doca_pe.h>
#include <docacxx/ctx/server.h>
#include <string_view>

#include "bytes.h"
#include "../../../dpu/dma_qp_mapping.h"
#include "doca_wrapper/queue_dma.h"
#include "format.h"
#include "io/io_ctx.h"
#include "noncopyable.h"
#include "types.h"

namespace pulsedb {
class DmaQpMapping;

/* 
 * 单线程负责控制路径 + 轮询异步的数据操作
 * (其中，dma数据操作卸载到硬件上，文件IO操作使用基于spdk rte模型异步化的bolbfs）
 * 问题：buf_inv 分配回收的开销？如何优雅地进行批量传输与空间分配回收？
 */
class Backend: Noncopyable {
public:
    Backend(const std::string_view &name, const std::string_view &dev_pci,
        const std::string_view &dev_rep_pci);
    ~Backend();

    void run();

    void send_start_comm_msg();
    void send_ack_msg();
    void send_exit_msg();
    template<class CtrlMsgType> CtrlMsgType recv_ctrl_msg();
    size_t recv_new_conumser_id();

    doca_dev *dev() const { return m_dev; }
    doca_dev_rep *dev_rep() const { return m_dev_rep; }
    QueueDma *dma() const { return m_dma.get(); }
    doca_pe *pe() const { return m_pe; }
    doca_comch_connection *conn() const { return m_conn; }

private:
    void send_ctrl_msg(const ConstBytesView &raw_msg);

    std::unique_ptr<IoCtx> m_io_ctx;
    MsgQueue m_io_msg_queue;

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
    std::optional<CtrlMsg::Variant> m_ctrl_msg_variant{};
    std::optional<size_t> m_new_consumer_id{};
};
}
