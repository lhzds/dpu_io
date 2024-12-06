#pragma once

#include "docacxx/ctx/producer.h"

namespace pulsedb {
class SpecializedProducer: public docacxx::comch::Producer {
public:
    using Cfg = Producer::Cfg;

    SpecializedProducer(doca_comch_connection *peer_conn,
        doca_pe *pe, doca_dev *dev, const Cfg &cfg, size_t consumer_id)
        :Producer(peer_conn, pe, dev, cfg), m_consumer_id(consumer_id) {}

     std::unique_ptr<Producer::SendTask> alloc_send_task(uint8_t *imm_data,
        uint32_t imm_data_len, Producer::SendTask::CompleteCallback cb) {
        return alloc_send_task(imm_data, imm_data_len, m_consumer_id, std::move(cb));
    }

private:
    using Producer::alloc_send_task;

    size_t m_consumer_id;
};
}