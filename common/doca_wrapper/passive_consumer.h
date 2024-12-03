#pragma once

#include "bytes.h"
#include "docacxx/ctx/consumer.h"
#include "docacxx/mem.h"
#include <memory>

namespace pulsedb {
class PassiveConsumer {
public:
    using Cfg = docacxx::comch::Consumer::Cfg;
    using ContextTaskCompleteCallback = std::function<void(const ConstBytesView &)>;

    PassiveConsumer(doca_comch_connection *peer_conn, doca_pe *pe, doca_pe *conn_pe, doca_dev *dev,
        const Cfg &cfg, ContextTaskCompleteCallback cb) {
        // 收发立即数，无需用到 doca_buf，所以 mmap 的 size 设为 0
        m_mmap = std::make_unique<docacxx::Mmap>(dev, 0, cfg.m_post_recv_task_slot_num);
        m_consumer = std::make_unique<docacxx::comch::Consumer>(peer_conn, pe, conn_pe, *m_mmap, dev, cfg);
        // 完成解析消息，并自动提交新的 post_recv_task
        m_consumer->set_post_recv_cb([&, cb = std::move(cb)]
            (std::unique_ptr<docacxx::comch::Consumer::PostRecvTask> task) {
            assert(task && task->raw());
            auto *imm_data = task->imm_data();
            auto imm_data_size = task->imm_data_size();
            cb({imm_data, imm_data_size});
            auto ret = m_consumer->submit(std::move(task));
            assert(not ret);
        });
        m_consumer->start_sync();
        // 初始提交所有 post_recv_task
        for (size_t i{}; i < cfg.m_post_recv_task_slot_num; ++i) {
            auto task = m_consumer->alloc_post_recv_task(nullptr);
            auto ret = m_consumer->submit(std::move(task));
            assert(not ret);
        }
    }

private:
    std::unique_ptr<docacxx::Mmap> m_mmap;
    std::unique_ptr<docacxx::comch::Consumer> m_consumer;
};
}
