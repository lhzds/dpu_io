#pragma once

#include "bytes.h"
#include "docacxx/ctx/consumer.h"
#include "docacxx/mem.h"
#include <memory>
#include <spdlog/spdlog.h>

namespace pulsedb {
class PassiveConsumer {
public:
    struct Cfg: docacxx::comch::Consumer::Cfg {
        size_t max_msg_size{128};
    };
    using ContextTaskCompleteCallback = std::function<void(const ConstBytesView &)>;

    PassiveConsumer(doca_comch_connection *peer_conn, doca_pe *pe, doca_pe *conn_pe, doca_dev *dev,
        const Cfg &cfg, ContextTaskCompleteCallback cb) {
        m_buf_inv = std::make_unique<docacxx::BufInventoryWithMmap>(dev,
            cfg.post_recv_task_slot_num * cfg.max_msg_size, cfg.post_recv_task_slot_num);
        m_consumer = std::make_unique<docacxx::comch::Consumer>(peer_conn, pe, conn_pe, m_buf_inv->mmap(), dev, cfg);
        // 完成解析消息，并自动提交新的 post_recv_task
        m_consumer->set_post_recv_cb([&, cb = std::move(cb)]
            (std::unique_ptr<docacxx::comch::Consumer::PostRecvTask> task) {
            assert(task && task->raw());
            auto *buf = task->buf();
            if (buf) {
                auto [data, size] = buf->get_doca_buf_data();
                cb({data, size});
                buf->clear_doca_buf_data();
            }
            
            auto ret = m_consumer->submit(std::move(task));
            assert(not ret);
        });
        m_consumer->set_post_recv_error_cb([](auto task) {
            auto s = task->status();
            SPDLOG_INFO("[Passive-Consumer] Release post_recv task: {}[{}]",
                doca_error_get_name(s), doca_error_get_descr(s));
        });
        m_consumer->start_sync();
        // 初始提交所有 post_recv_task
        for (size_t i{}; i < cfg.post_recv_task_slot_num; ++i) {
            m_bufs.emplace_back(m_buf_inv->get_buf(i * cfg.max_msg_size, cfg.max_msg_size));
            auto task = m_consumer->alloc_post_recv_task(*m_bufs.back(), nullptr);
            auto ret = m_consumer->submit(std::move(task));
            assert(not ret);
        }
    }

    void clean_sync() {
        m_consumer->clean_sync();
    }

    ~PassiveConsumer() {
        SPDLOG_INFO("[Passive-Consumer] Cleaning...");
        if (not m_consumer->is_cleaned()) {
            m_consumer->clean_sync();
        }
    }

private:
    std::unique_ptr<docacxx::BufInventoryWithMmap> m_buf_inv;
    std::vector<std::unique_ptr<docacxx::Buffer>> m_bufs;
    std::unique_ptr<docacxx::comch::Consumer> m_consumer;
};
}
