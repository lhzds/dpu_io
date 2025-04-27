#pragma once

#include "bytes.h"
#include "docacxx/ctx/producer.h"
#include "docacxx/mem.h"
#include "util.h"
#include <doca_pe.h>
#include <queue>

namespace pulsedb {
class SyncSingleTaskProducer: public docacxx::comch::Producer {
public:
    SyncSingleTaskProducer(doca_comch_connection *peer_conn,
        doca_pe *pe, doca_dev *dev, size_t buf_size, size_t consumer_id)
        :Producer(peer_conn, pe, dev, Cfg{1}), m_consumer_id(consumer_id) {
        m_buf_inv = std::make_unique<docacxx::BufInventoryWithMmap>(dev, buf_size, 1);
        m_buf = m_buf_inv->get_buf(size_t(0), buf_size);

        Producer::set_send_cb([&](std::unique_ptr<Producer::SendTask> task) {
            m_is_sended = true;
            m_send_cb();
            m_task = std::move(task);
        });
        Producer::set_send_error_cb([&](std::unique_ptr<Producer::SendTask> task) {
            m_send_error_cb();
            m_task = std::move(task);
        });
    }

    char *buf_data() {
        return m_buf->data();
    }

    size_t buf_size() {
        return m_buf->size();
    }

    void send_sync(size_t buf_valid_size) {
        assert(buf_valid_size <= m_buf->size());
        if (not m_task) {
            m_task = alloc_send_task(*m_buf, m_consumer_id, nullptr);
        }
    
        m_is_sended = false;
        m_buf->set_doca_buf_data(buf_valid_size);
        while ((m_task = submit(std::move(m_task)))) {
            doca_pe_progress(pe());
        }
        while (not m_is_sended) {
            doca_pe_progress(pe());
        }
    }

    void set_send_cb(std::function<void()> cb) {
        m_send_cb = std::move(cb); 
    }
    void set_send_error_cb(std::function<void()> cb) {
        m_send_error_cb = std::move(cb);
    }

    void stop() override {
        m_task.reset();
        Producer::stop();
    }

private:
    using Producer::Cfg;
    using Producer::alloc_send_task;
    using Producer::submit;
    using Producer::set_send_cb;
    using Producer::set_send_error_cb;

    std::unique_ptr<docacxx::BufInventoryWithMmap> m_buf_inv;
    std::unique_ptr<docacxx::Buffer> m_buf;
    std::unique_ptr<Producer::SendTask> m_task;
    size_t m_consumer_id{};

    std::function<void()> m_send_cb {[] {}};
    std::function<void()> m_send_error_cb {[] { throw_with("SingleTaskProducer: Send error");}};

    bool m_is_sended{};
};

// 默认对端会提前分配好 post_recv 任务
class QueueSingleTaskProducer: public docacxx::comch::Producer {
public:
    QueueSingleTaskProducer(doca_comch_connection *peer_conn,
        doca_pe *pe, doca_dev *dev, size_t buf_size, size_t consumer_id)
        :Producer(peer_conn, pe, dev, Cfg{1}), m_consumer_id(consumer_id) {
        m_buf_inv = std::make_unique<docacxx::BufInventoryWithMmap>(dev, buf_size, 1);
        m_buf = m_buf_inv->get_buf(size_t(0), buf_size);

        Producer::set_send_cb([&](std::unique_ptr<Producer::SendTask> task) {
            m_send_cb();
            if (not m_data_wq.empty()) {
                prep_buf(m_data_wq.front());
                auto ret = submit(std::move(task));
                if (ret != nullptr) {
                    throw_with("QueueSingleTaskProducer: Submit failed");
                }
                m_data_wq.pop();
                return;
            }
            m_is_busy = false;
            m_task = std::move(task);
        });
        Producer::set_send_error_cb([&](std::unique_ptr<Producer::SendTask> task) {
            m_send_error_cb();
            m_is_busy = false;
            m_task = std::move(task);
        });
    }

    void submit_send_task(BytesBuffer data) {
        assert(data.size() <= m_buf->size());
        if (m_is_busy) {
            m_data_wq.emplace(std::move(data));
            return;
        }
        // context 要 start 后才能分配 task，所以没在构造函数里分配 task
        if (not m_task) {
            m_task = alloc_send_task(*m_buf, m_consumer_id, nullptr);
        }

        prep_buf(data);
        m_task = submit(std::move(m_task));
        if (m_task != nullptr) {
            throw_with("QueueSingleTaskProducer: Submit failed");
        }
        m_is_busy = true;
    }

    void set_send_cb(std::function<void()> cb) {
        m_send_cb = std::move(cb); 
    }
    void set_send_error_cb(std::function<void()> cb) {
        m_send_error_cb = std::move(cb);
    }

    void stop() override {
        m_task.reset();
        Producer::stop();
    }

private:
    void prep_buf(const ConstBytesView &data) {
        std::memcpy(m_buf->data(), data.data(), data.size());
        m_buf->set_doca_buf_data(data.size());
    }

    using Producer::Cfg;
    using Producer::alloc_send_task;
    using Producer::submit;
    using Producer::set_send_cb;
    using Producer::set_send_error_cb;

    std::unique_ptr<docacxx::BufInventoryWithMmap> m_buf_inv;
    std::unique_ptr<docacxx::Buffer> m_buf;
    std::unique_ptr<Producer::SendTask> m_task;
    size_t m_consumer_id{};

    std::function<void()> m_send_cb {[] {}};
    std::function<void()> m_send_error_cb {[] { throw_with("QueueSingleTaskProducer: Send error");}};

    bool m_is_busy{};
    std::queue<BytesBuffer> m_data_wq;
};
}
