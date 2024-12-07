#include "comm/backend.h"
#include "bytes.h"
#include "constant.h"
#include "dma_qp_mapping.h"
#include "docacxx/ctx/server.h"
#include "format.h"
#include "io/mem_io_mock.h"

#include <doca_dev.h>
#include <doca_pe.h>
#include <memory>
#include <spdlog/spdlog.h>

namespace pulsedb {
Backend::Backend(const std::string_view &name, const std::string_view &dev_pci,
    const std::string_view &dev_rep_pci) {
    m_io_ctx = std::make_unique<MemIoMock>();

    docacxx::check_derr(docacxx::open_doca_device_with_pci(dev_pci.data(), &m_dev),
        "failed to open dev");
    docacxx::check_derr(docacxx::open_doca_device_rep_with_pci(m_dev, DOCA_DEVINFO_REP_FILTER_NET,
        dev_rep_pci.data(), &m_dev_rep), "failed to open dev rep");
    docacxx::check_derr(doca_pe_create(&m_pe), "failed to create pe");

    m_dma = std::make_unique<QueueDma>(m_pe, m_dev, DPU_DMA_TASK_SLOT_NUM);
    m_dma->start_sync();

    m_ctrl_server = std::make_unique<docacxx::comch::Server>(name.data(), m_dev, m_dev_rep,
        m_pe, docacxx::comch::Server::Cfg{1}); // 消息同步传输，所以只分配一个task slot
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

    auto qp_num = recv_ctrl_msg<CtrlMsg::InitMsg>().header->qp_num;
    send_ack_msg();

    m_qps.reserve(qp_num);
    for (size_t i{}; i < qp_num; ++i) {
        auto init_qp_msg = recv_ctrl_msg<CtrlMsg::InitQpMsg>();
        send_ack_msg();
        auto *header = init_qp_msg.header;
        
        // 使用 1:1 大小的队列映射
        m_qps.emplace_back(std::make_unique<DmaQpMapping>(*this, m_io_ctx.get(), m_io_msg_queue,
            header->host_req_mem_size,
            ConstBytesView(init_qp_msg.host_req_mmap_desc, header->host_req_mmap_desc_size),
            header->host_req_mem_addr, header->host_req_mem_size,

            header->host_resp_mem_size,
            ConstBytesView(init_qp_msg.host_resp_mmap_desc, header->host_resp_mmap_desc_size),
            header->host_resp_mem_addr, header->host_resp_mem_size
        ));
    }
    spdlog::debug("create {} qps", qp_num);

    // 运行时只接受 exit 退出消息
    m_ctrl_server->set_recv_cb([&](doca_comch_connection *conn, uint8_t *data, uint32_t len) {
        CtrlMsg::Type msg_type;
        ConstBytesView bytes((char *)data, len);
        m_ctrl_msg_variant = CtrlMsg::read(bytes);
        std::visit([&](const auto &msg) {
            msg_type = msg.header->type;
        }, *m_ctrl_msg_variant);
        if (msg_type != CtrlMsg::Type::EXIT) {
            throw_with("wrong msg type");
        }
        spdlog::debug("receive exit msg");
        m_is_exited = true;
    });
    send_start_comm_msg();
}

Backend::~Backend() {
    spdlog::debug("backend start exiting");
    m_qps.clear();
    m_dma->clean_sync();
    m_ctrl_server->clean_sync();
    
    if (auto result = doca_pe_destroy(m_pe); result != DOCA_SUCCESS) {
        std::cerr << "failed to destroy pe: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_rep_close(m_dev_rep); result != DOCA_SUCCESS) {
        std::cerr << "failed to close dev rep: " << doca_error_get_descr(result) << std::endl;
    }
    if (auto result = doca_dev_close(m_dev); result != DOCA_SUCCESS) {
        std::cerr << "failed to close dev: " << doca_error_get_descr(result) << std::endl;
    }
}

void Backend::run() {
    std::function<void()> msg;
    while (not m_is_exited) {
        doca_pe_progress(m_pe);
        while (m_io_msg_queue.pop(msg)) {
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
    if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::AckMsg>) {
        spdlog::debug("receive ack msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::InitMsg>) {
        spdlog::debug("receive init msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::InitQpMsg>) {
        spdlog::debug("receive init_qp msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::ExitMsg>) {
        spdlog::debug("receive exit msg");
    } else if constexpr (std::is_same_v<CtrlMsgType, CtrlMsg::StartCommMsg>) {
        spdlog::debug("receive start_comm msg");
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
    spdlog::debug("send ack msg");
}

void Backend::send_start_comm_msg() {
    send_ctrl_msg(CtrlMsg::make_start_comm_msg());
    spdlog::debug("send start_comm msg");
}

size_t Backend::recv_new_conumser_id() {
    while (not m_new_consumer_id) {
        doca_pe_progress(m_pe);
    }
    auto ret = *m_new_consumer_id;
    m_new_consumer_id = std::nullopt;
    spdlog::debug("receive new consumer id={}", *m_new_consumer_id);
    return ret;
}
}
