#pragma once

#include <doca_comch.h>
#include <list>

#include "bytes.h"
#include "docacxx/mem.h"
#include "doca_wrapper/queue_dma.h"
#include "doca_wrapper/producers.h"
#include "doca_wrapper/passive_consumer.h"
#include "format/rb_meta_msg.h"
#include "format/rb_req_resp_msg.h"
#include "ring_buffer.h"
#include "types.h"

namespace pulsedb {

class Backend;

void TEST_print_resp_rb_view(const ConstBytesView &data);
void TEST_print_resp_rb_view(const RingBuffer::View &v);

template<typename StautsEnum>
struct RbViewWithStatus {
public:
    StautsEnum status;
    RingBuffer::View inner;
    RbViewWithStatus(StautsEnum s, RingBuffer::View v)
        : status(s), inner(v) {}
};

class RbMapping {
public:    
    RbMapping(int id, doca_dev *dev, size_t dpu_rb_cap, ConstBytesView host_mmap_desc,
        char *host_rb_data, size_t host_rb_cap, QueueDma *dma)
        : m_id(id), m_dpu_rb(dpu_rb_cap), m_host_rb_rep(host_rb_cap), m_dma(dma) {
        m_dpu_mmap = std::make_unique<docacxx::Mmap>(dev, m_dpu_rb.data(), m_dpu_rb.cap());
        m_host_mmap = std::make_unique<docacxx::Mmap>(host_mmap_desc, dev, host_rb_data, m_host_rb_rep.cap());
    }

    size_t host_rb_cap() {
        return m_host_rb_rep.cap();
    }

    size_t dpu_rb_cap() {
        return m_dpu_rb.cap();
    }

    void set_producer(QueueSingleTaskProducer *producer) {
        m_producer = producer;
    }
    static std::vector<QueueDma::MemcpySeg> make_memcpy_segs(RingBufferRep::Slice src,
        RingBufferRep::Slice dst);

protected:
    int m_id{};

    RingBuffer m_dpu_rb{}; // 本地环形队列
    RingBufferRep m_host_rb_rep{}; // 远程环形队列的元信息（基于远端消息进行更新）
    std::unique_ptr<docacxx::Mmap> m_dpu_mmap; // 本地内存
    std::unique_ptr<docacxx::Mmap> m_host_mmap; // 远程内存

    QueueDma *m_dma{};
    QueueSingleTaskProducer *m_producer{};
};

class ReqRbMapping: public RbMapping {
public:
    struct DpuRbViewStatus {
        enum InnerStatus {
            kDmaInProgress,
            kProcessing,
            kInvalided
        } inner;
        bool is_host_cleaned{};
        DpuRbViewStatus(InnerStatus i): inner(i) {}
    };
    using RbMapping::RbMapping;
    using DpuRbView = RbViewWithStatus<DpuRbViewStatus>;
    using DmaDoneCb = std::function<void(int, DpuRbView &)>;

    ReqRbMapping(int id, doca_dev *dev, size_t dpu_rb_cap, ConstBytesView host_mmap_desc,
        char *host_rb_data, size_t host_rb_cap, QueueDma *dma, DmaDoneCb cb)
        : RbMapping(id, dev, dpu_rb_cap, host_mmap_desc,
            host_rb_data, host_rb_cap, dma), m_dma_done_cb(std::move(cb)) {}

    void read_async(size_t data_size) {
        SPDLOG_INFO("[Req-Rb#{}] Receive meta msg[NewSize: new_size={}, dpu_rb_free_size={}]",
            m_id, data_size, m_dpu_rb.free_size());
        auto hs = *m_host_rb_rep.write(data_size);
        // 用于判断 dpu_req_rb 是否有空间，否则放入等待队列
        if (m_dpu_rb.cap() < data_size) {
            throw_with("Req-Rb#{}: Too large to write", m_id);
        }
        auto cond = [&, data_size]() -> bool {
            return m_dpu_rb.free_size() >= data_size;
        };
        // 用于将 req 插入 req 环形队列
        auto action = [&, hs] {
            auto ds = *m_dpu_rb.rep().write(hs.size()); // 申请空间
            SPDLOG_INFO("[Req-Rb#{}] Start dmaing[Host->Dpu: src_offset={}, dest_offset={}, size={}]",
                m_id, hs.first_part().offset, ds.first_part().offset, ds.size());

            m_dpu_rb_views.emplace_back(DpuRbViewStatus::kDmaInProgress, m_dpu_rb.view(ds)); // 记录元数据
            auto &dpu_rb_view = m_dpu_rb_views.back();
            m_dma->post_memcpyv(*m_host_mmap, *m_dpu_mmap,
                make_memcpy_segs(hs, ds),
                [&, src_offset=hs.first_part().offset, dest_offset=ds.first_part().offset] {
                    SPDLOG_INFO("[Req-Rb#{}] Finish dmaing[Host->Dpu: src_offset={}, dest_offset={}, size={}]",
                        m_id, src_offset, dest_offset, dpu_rb_view.inner.size());

                    dpu_rb_view.status.inner = DpuRbViewStatus::kProcessing;
                    size_t new_host_cleaned_size{};
                    for (auto &v : m_dpu_rb_views) {
                        if (v.status.inner == DpuRbViewStatus::kDmaInProgress) {
                            break;
                        }
                        if (not v.status.is_host_cleaned) {
                            new_host_cleaned_size += v.inner.size();
                            v.status.is_host_cleaned = true;
                        }
                    }

                    SPDLOG_INFO("[Req-Rb#{}] Get the new reclaimed space of host req rb[size={}]", m_id, new_host_cleaned_size);
                    m_host_cleaned_size += new_host_cleaned_size;
                    try_send_cleaned_size();
                    if (m_dma_done_cb) {
                        m_dma_done_cb(m_id, dpu_rb_view); 
                    }
                });
        };
        m_dpu_rb_space_wait_eq.do_or_wait(std::move(cond), std::move(action));
    }

    void receive_needed_size(size_t size) {
        SPDLOG_INFO("[Req-Rb#{}] Receive meta msg[needed_space={}]", m_id, size);
        m_host_needed_size = size;
        try_send_cleaned_size();
    }

    void invalid_dpu_view(DpuRbView &dpu_rb_view) {
        auto dpu_rb_slice = m_dpu_rb.slice(dpu_rb_view.inner);
        SPDLOG_INFO("[Req-Rb#{}] Invalid dpu rb slice[offset={}, size={}]",
            m_id, dpu_rb_slice.first_part().offset, dpu_rb_slice.size());
        
        dpu_rb_view.status.inner = DpuRbViewStatus::kInvalided;
        size_t new_dpu_cleaned_size{};
        for (auto iter = m_dpu_rb_views.begin(); iter != m_dpu_rb_views.end();
            iter = m_dpu_rb_views.erase(iter)) {
            if (iter->status.inner != DpuRbViewStatus::kInvalided) {
                break;
            }
            new_dpu_cleaned_size += iter->inner.size();
        }
        
        SPDLOG_INFO("[Req-Rb#{}] Get the new reclaimed space of dpu req rb[size={}]", m_id, new_dpu_cleaned_size);
        m_dpu_rb.read(new_dpu_cleaned_size);
        // 触发 dpu 队列空间等待事件
        m_dpu_rb_space_wait_eq.try_trigger();
    }

private:
    void try_send_cleaned_size() {
        if (m_host_needed_size == 0) {
            return;
        }

        SPDLOG_INFO("[Req-Rb#{}] Try send meta msg[needed_size={}, cleaned_size={}]",
            m_id, m_host_needed_size, m_host_cleaned_size);
        if (m_host_cleaned_size >= m_host_needed_size) {
            // 大端/小端字节序？
            BytesBuffer data(sizeof(D2HRbMetaMsg));
            auto *d2h_rb_meta_msg = (D2HRbMetaMsg *)data.data();
            d2h_rb_meta_msg->type = D2HRbMetaMsg::Type::kReqRbCleanedSize;
            d2h_rb_meta_msg->payload.req_rb_cleaned_size = m_host_cleaned_size;
            // host 已经提前发出 post_recv 任务
            m_producer->submit_send_task(std::move(data));
            SPDLOG_INFO("[Req-Rb#{}] Send meta msg[cleaned_size={}]", m_id, m_host_cleaned_size);
            
            m_host_rb_rep.read(m_host_cleaned_size);
            m_host_cleaned_size = 0;
            m_host_needed_size = 0;
        }
    }

    // 因为用了结构化绑定，因此修改成员顺序时注意对结构化绑定顺序也进行调整
    size_t m_host_cleaned_size{}; // host head 增量
    size_t m_host_needed_size{}; // 决定是否发送增量

    std::list<DpuRbView> m_dpu_rb_views; // 记录处于消息队列中的所有不定长消息
    EventQueue m_dpu_rb_space_wait_eq; // 存储因目前本地队列空间不足而等待进行的事件

    DmaDoneCb m_dma_done_cb;
};

class RespRbMapping: public RbMapping {
public:
    enum class DpuRbViewStatus {
        kInit,
        kDmaInProgress,
        kDmaDone
    };
    using DpuRbView = RbViewWithStatus<DpuRbViewStatus>;
    RespRbMapping(int id, doca_dev *dev, size_t dpu_rb_cap, ConstBytesView host_mmap_desc,
        char *host_rb_data, size_t host_rb_cap, QueueDma *dma)
        : RbMapping(id, dev, dpu_rb_cap, host_mmap_desc,
            host_rb_data, host_rb_cap, dma), m_dmaing_host_rb_rep(host_rb_cap) {}

    void append(BytesBuffer data) {
        SPDLOG_INFO("[Resp-Rb#{}] Try to append data to dpu rb[data_size={}, dpu_rb_head={},"
                    "dpu_rb_tail={}, dpu_rb_free_size={}]",
                    m_id, data.size(), m_dpu_rb.head(), m_dpu_rb.tail(), m_dpu_rb.free_size());
        if (m_dpu_rb.cap() < data.size()) {
            throw_with("Resp-Rb#{}: Too large to write", m_id);
        }
        auto cond = [&, data_size=data.size()] {
            return m_dpu_rb.free_size() >= data_size;
        };
        auto action = [&, d = std::move(data)] {
            auto dpu_rb_view = *m_dpu_rb.reserve_space(d.size());
            dpu_rb_view.write(d);
            m_dpu_rb_views.emplace_back(DpuRbViewStatus::kInit, dpu_rb_view);
            start_dma(m_dpu_rb_views.back());
        };

        m_dpu_rb_space_wait_eq.do_or_wait(std::move(cond), std::move(action));
    }    

    void receive_wait_stat(size_t processed_size, size_t wait_size) {
        SPDLOG_INFO("[Resp-Rb#{}] Receive meta msg[WaitStat: processed={}, wait_size={}]",
            m_id, processed_size, wait_size);
        m_host_rb_rep.read(processed_size);
        m_dmaing_host_rb_rep.read(processed_size);
        // 执行 host 队列空间等待任务
        m_host_rb_space_wait_eq.try_trigger();

        m_host_wait_size = wait_size;
        try_send_new_size();
    }

private:
    void start_dma(DpuRbView &dpu_rb_view) {
        auto data_size = dpu_rb_view.inner.size();
        auto ds = m_dpu_rb.slice(dpu_rb_view.inner);
        SPDLOG_INFO("[Resp-Rb#{}] Append data, try to start dmaing[Dpu->Host: src_offset={}, "
            "data_size={}, host_rb_free_size={}]",
            m_id, ds.first_part().offset, data_size,
            m_dmaing_host_rb_rep.free_size());
        if (m_dmaing_host_rb_rep.cap() < data_size) {
            throw_with("Host-Resp-Rb#{}: To large to write", m_id);
        }
        auto cond = [&, data_size]() -> bool {
            return m_dmaing_host_rb_rep.free_size() >= data_size;
        };
        auto action = [&, ds, data_size] {
            auto hs = *m_dmaing_host_rb_rep.write(data_size);
            SPDLOG_INFO("[Resp-Rb#{}] Start dmaing[Dpu->Host: src_offset={}, dest_offset={}, size={}]",
                m_id, ds.first_part().offset, hs.first_part().offset, ds.size());

            // TODO: 添加 DMA size 的最小阈值
            dpu_rb_view.status = DpuRbViewStatus::kDmaInProgress;
            m_dma->post_memcpyv(*m_dpu_mmap, *m_host_mmap,
                make_memcpy_segs(ds, hs), 
                [&, src_offset = ds.first_part().offset, dest_offset= hs.first_part().offset] {
                    SPDLOG_INFO("[Resp-Rb#{}] Finish dmaing[Dpu->Host: src_offset={}, dest_offset={}, size={}]",
                        m_id, src_offset, dest_offset, dpu_rb_view.inner.size());
                    dpu_rb_view.status = DpuRbViewStatus::kDmaDone;
                    size_t new_size{};
                    for (auto iter = m_dpu_rb_views.begin(); iter != m_dpu_rb_views.end();
                        iter = m_dpu_rb_views.erase(iter)) {
                        if (iter->status != DpuRbViewStatus::kDmaDone) {
                            break;
                        }
                        new_size += iter->inner.size();
                    }
                    SPDLOG_INFO("[Resp-Rb#{}] Get the new space for (dpu rb to reclaim / host rb to read)[size={}]",
                        m_id, new_size);
                    m_host_new_size += new_size;
                    m_dpu_rb.read(new_size);
                    try_send_new_size();
                    m_dpu_rb_space_wait_eq.try_trigger();
                });
        };
        m_host_rb_space_wait_eq.do_or_wait(std::move(cond), std::move(action));
    }

    void try_send_new_size() {
        // TODO: 或许可以添加发送的最小阈值控制对端处理的粒度
        if (m_host_wait_size == 0) {
            return;
        }
        size_t host_rb_free_size = m_host_rb_rep.free_size() / sizeof(IoRespMsg) * sizeof(IoRespMsg);
        size_t target = std::min(host_rb_free_size, m_host_wait_size);
        SPDLOG_INFO("[Resp-Rb#{}] Try send meta msg[target={}, new_size={}]",
            m_id, target, m_host_new_size);
        if (m_host_new_size >= target) {
            BytesBuffer data(sizeof(D2HRbMetaMsg));
            auto *d2h_rb_meta_msg = (D2HRbMetaMsg *)data.data();
            d2h_rb_meta_msg->type = D2HRbMetaMsg::Type::kRespRbNewSize;
            d2h_rb_meta_msg->payload.resp_rb_new_size = target;
            // host 已经提前发出 post_recv 任务
            m_producer->submit_send_task(std::move(data));
            SPDLOG_INFO("[Resp-Rb#{}] Send meta msg[new_size={}]", m_id, target);

            m_host_rb_rep.write(target);
            m_host_new_size -= target;
            m_host_wait_size -= target;
        }
    }

    // 因为用了结构化绑定，因此修改成员顺序时注意对结构化绑定顺序也进行调整
    size_t m_host_new_size{};
    size_t m_host_wait_size{};
    RingBufferRep m_dmaing_host_rb_rep;

    std::list<DpuRbView> m_dpu_rb_views; // 记录处于消息队列中的所有不定长消息
    EventQueue m_dpu_rb_space_wait_eq; // 存储因目前本地队列空间不足而等待进行的事件
    EventQueue m_host_rb_space_wait_eq; // 存储因远端队列空间不足而等待的事件
};

class DmaQpMapping {
public:
    using DmaDoneCb = ReqRbMapping::DmaDoneCb;

    DmaQpMapping(Backend &backend, int id,
        size_t dpu_req_rb_cap, ConstBytesView host_req_mmap_desc, 
        char *host_req_rb_data, size_t host_req_rb_cap,
        size_t dpu_resp_rb_cap, ConstBytesView host_resp_mmap_desc,
        char *host_resp_rb_data, size_t host_resp_rb_cap,
        ReqRbMapping::DmaDoneCb req_dma_done_cb);
    ~DmaQpMapping();

    void create_consumer(Backend &backend);
    void create_producer(Backend &backend);

    void finish_req(ReqRbMapping::DpuRbView &m_req_view, BytesBuffer data);

private:
    void recv_h2d_rb_meta_msg(const ConstBytesView &raw_msg);

    int m_id{};

    ReqRbMapping m_req_rb_mapping;
    RespRbMapping m_resp_rb_mapping;

    std::unique_ptr<PassiveConsumer> m_consumer;
    std::unique_ptr<QueueSingleTaskProducer> m_producer;
};

} // namespace pulsedb
