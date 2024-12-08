#pragma once

#include <doca_comch.h>
#include <list>

#include "comm/backend.h"
#include "bytes.h"
#include "docacxx/mem.h"
#include "doca_wrapper/queue_dma.h"
#include "doca_wrapper/single_task_producer.h"
#include "doca_wrapper/passive_consumer.h"
#include "format.h"
#include "io/io_ctx.h"
#include "ring_buffer.h"
#include "types.h"

// 只负责数据路径
namespace pulsedb {
class Backend;

class DmaQpMapping {
public:
    class Msg {
    public:
        enum class Status {
            DmaInProgress,
            Processing,
            Done
        };

        Msg(Status status, RingBufferRep::Slice rb_slice)
            : m_status(status), m_rb_slice(rb_slice) {}

        Status status() const { return m_status; }

        void start_dma() { m_status = Status::DmaInProgress; }
        void start_process() { m_status = Status::Processing; }
        void done() { m_status = Status::Done; }

        const RingBufferRep::Slice &rb_slice() const { return m_rb_slice; }

    private:
        Status m_status;
        RingBufferRep::Slice m_rb_slice;
    };

    DmaQpMapping(Backend &backend, IoCtx *io_ctx, MsgQueue &m_io_msg_queue,
        size_t dpu_req_rb_cap, ConstBytesView host_req_mmap_desc, 
        char *host_req_mem_addr, size_t host_req_mem_size,
        size_t dpu_resp_rb_cap, ConstBytesView host_resp_mmap_desc,
        char *host_resp_mem_addr, size_t host_resp_mem_size);
    ~DmaQpMapping();

    void create_consumer(Backend &backend);
    void create_producer(Backend &backend);

private:
    void recv_h2d_rb_meta_msg(const ConstBytesView &raw_msg);
    void submit_new_reqs(size_t host_req_new_tail);
    void req_batch_dma_done_cb(std::list<Msg>::iterator req_batch_iter);
    void process_req(RingBufferRequest::Type type, RingBufferRequest::Variant &req_variant,
        RingBuffer::View resp_view, Msg &req_msg, Msg &resp_ms);
    void req_process_done_cb(Msg &req_msg, Msg &resp_msg);
    void resp_dma_done_cb(Msg &resp_msg);

    std::vector<QueueDma::MemcpySeg> make_memcpy_segs(RingBufferRep::Slice src, RingBufferRep::Slice dst);
    void send_host_req_rb_head();
    void send_host_resp_rb_tail();

    struct QueueData {
        QueueData(doca_dev *dev, size_t dpu_rb_size, ConstBytesView host_mmap_desc,
            char *host_mem_addr, size_t host_mem_size);

        // 因为用了结构化绑定，因此修改成员顺序时注意对结构化绑定顺序也进行调整
        RingBuffer dpu_rb{}; // 本地环形队列
        RingBufferRep host_rb_rep{}; // 远程环形队列的元信息
        std::list<Msg> dpu_msgs; // 记录处于消息队列中的所有不定长消息

        EventQueue msg_insert_wait_queue; // 存储因目前本地内存不足而等待后续插入的消息
        std::unique_ptr<docacxx::Mmap> dpu_mmap; // 本地内存
        std::unique_ptr<docacxx::Mmap> host_mmap; // 远程内存
    };

    QueueDma *m_dma{};
    IoCtx *m_io_ctx{};
    MsgQueue &m_io_msg_queue;

    QueueData m_req_mapping;
    QueueData m_resp_mapping;
    size_t m_host_resp_rb_dma_done_tail{};

    std::unique_ptr<PassiveConsumer> m_consumer;
    std::unique_ptr<SingleTaskProducer> m_producer;
    bool m_need_host_req_rb_head{};
    bool m_need_host_resp_rb_tail{};
    EventQueue m_resp_dma_wait_queue{};
};
}
