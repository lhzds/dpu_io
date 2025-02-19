#pragma once

#include <doca_comch.h>
#include <list>

#include "bytes.h"
#include "docacxx/mem.h"
#include "doca_wrapper/queue_dma.h"
#include "doca_wrapper/single_task_producer.h"
#include "doca_wrapper/passive_consumer.h"
#include "format/rb_meta_msg.h"
#include "ring_buffer.h"
#include "types.h"

namespace pulsedb {

class Backend;

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
        char *host_mem_addr, size_t host_mem_size, QueueDma *dma)
        : m_id(id), m_dpu_rb(dpu_rb_cap), m_host_rb_rep(host_mem_size), m_dma(dma) {
        m_dpu_mmap = std::make_unique<docacxx::Mmap>(dev, m_dpu_rb.data(), dpu_rb_cap);
        m_host_mmap = std::make_unique<docacxx::Mmap>(host_mmap_desc, dev, host_mem_addr, host_mem_size);
    }

    void set_producer(SingleTaskProducer *producer) {
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
    SingleTaskProducer *m_producer{};
};

class ReqRbMapping: public RbMapping {
public:
    enum class DpuRbViewStatus {
        kDmaInProgress,
        kProcessing,
        kInvalided
    };
    using RbMapping::RbMapping;
    using DpuRbView = RbViewWithStatus<DpuRbViewStatus>;
    using DmaDoneCb = std::function<void(int, DpuRbView &)>;

    void set_dma_done_cb(DmaDoneCb cb) {
        m_dma_done_cb = std::move(cb);
    }

    void read_async(size_t size) {
        SPDLOG_INFO("receive new_size={} from host_req_rb", size);
        auto hs = *m_host_rb_rep.write(size);
        // 用于判断 dpu_req_rb 是否有空间，否则放入等待队列
        auto cond = [&, expected_size = size]() -> bool { 
            return m_dpu_rb.free_size() >= expected_size;
        };
        // 用于将 req 插入 req 环形队列
        auto action = [&, hs] {
            auto ds = *m_dpu_rb.rep().write(hs.size()); // 申请空间
            SPDLOG_INFO("start dma to copy new data to dpu_req_rb[start_offset={}, size={}]",
                ds.first_part().offset, ds.size());

            m_dpu_rb_views.emplace_back(DpuRbViewStatus::kDmaInProgress, m_dpu_rb.view(ds)); // 记录元数据
            auto &dpu_rb_view = m_dpu_rb_views.back();
            m_dma->post_memcpyv(*m_host_mmap, *m_dpu_mmap,
                make_memcpy_segs(hs, ds),
                [&] {
                    auto ds = m_dpu_rb.slice(dpu_rb_view.inner);
                    SPDLOG_INFO("req msg dma done[start_offset={}, size={}]",
                        ds.first_part().offset, ds.size());

                    dpu_rb_view.status = DpuRbViewStatus::kProcessing;
                    for (const auto &v : m_dpu_rb_views) {
                        if (v.status == DpuRbViewStatus::kDmaInProgress) {
                            break;
                        }
                        m_host_size_cleaned += v.inner.size();
                    }
                    try_send_cleaned_size();
                    if (m_dma_done_cb) {
                        m_dma_done_cb(m_id, dpu_rb_view); 
                    }
                });
        };

        m_dpu_rb_space_wait_eq.do_or_wait(std::move(cond), std::move(action));
    }

    void receive_needed_size(size_t size) {
        SPDLOG_INFO("receive needed_space={} from host_req_rb", size);
        m_host_needed_size = size;
        try_send_cleaned_size();
    }

    void invalid_dpu_view(DpuRbView &dpu_rb_view) {
        auto dpu_rb_slice = m_dpu_rb.slice(dpu_rb_view.inner);
        SPDLOG_INFO("dpu_req_rb slice[offset={}, size={}] is invalided",
            dpu_rb_slice.first_part().offset, dpu_rb_slice.size());
        
        dpu_rb_view.status = DpuRbViewStatus::kInvalided;
        for (auto iter = m_dpu_rb_views.begin(); iter != m_dpu_rb_views.end();
            iter = m_dpu_rb_views.erase(iter)) {
            if (iter->status != DpuRbViewStatus::kInvalided) {
                break;
            }
            m_dpu_rb.read(iter->inner.size());
        }
        
        // 触发 dpu 队列空间等待事件
        m_dpu_rb_space_wait_eq.try_trigger();
    }

private:
    void try_send_cleaned_size() {
        if (m_host_needed_size > 0 && m_host_size_cleaned >= m_host_needed_size) {
            // 大端/小端字节序？
            auto *d2h_rb_meta_msg = (D2HRbMetaMsg *)m_producer->buf_data();
            d2h_rb_meta_msg->type = D2HRbMetaMsg::Type::kReqRbCleanedSize;
            d2h_rb_meta_msg->payload.req_rb_cleaned_size = m_host_size_cleaned;
            auto ret = m_producer->try_submit_send_task(sizeof(*d2h_rb_meta_msg));
            assert(ret);
            SPDLOG_INFO("send cleaned_size={} to host_req_rb", m_host_size_cleaned);
            
            m_host_rb_rep.read(m_host_size_cleaned);
            m_host_size_cleaned = 0;
            m_host_needed_size = 0;
        }
    }

    // 因为用了结构化绑定，因此修改成员顺序时注意对结构化绑定顺序也进行调整
    size_t m_host_size_cleaned{}; // host head 增量
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
        char *host_mem_addr, size_t host_mem_size, QueueDma *dma)
        : RbMapping(id, dev, dpu_rb_cap, host_mmap_desc,
            host_mem_addr, host_mem_size, dma), m_dmaing_host_rb_rep(host_mem_size) {}

    void append(BytesBuffer data) {
        SPDLOG_INFO("try to append data[size={}] to dpu_resp_rb[free_space={}]",
            data.size(), m_dpu_rb.free_size());
        auto cond = [&, expected_size=data.size()] {
            return m_dpu_rb.free_size() >= expected_size;
        };
        auto action = [&, data = std::move(data)] {
            auto dpu_rb_view = *m_dpu_rb.reserve_space(data.size());
            m_dpu_rb_views.emplace_back(DpuRbViewStatus::kInit, dpu_rb_view);
            start_dma(m_dpu_rb_views.back());
        };

        m_dpu_rb_space_wait_eq.do_or_wait(std::move(cond), std::move(action));
    }    

    void receive_wait_stat(size_t processed_size, size_t wait_size) {
        SPDLOG_INFO("receive wait_stat[processed={}, wait_size={}]",
            processed_size, wait_size);
        m_host_rb_rep.read(processed_size);
        m_dmaing_host_rb_rep.read(processed_size);
        // 执行 host 队列空间等待任务
        m_host_rb_space_wait_eq.try_trigger();

        m_host_wait_size = wait_size;
        try_send_size_to_add();
    }

private:
    void start_dma(DpuRbView &dpu_rb_view) {
        SPDLOG_INFO("try to start dma to transfer data[size={}] "
            "to host_resp_rb[free_space={}]",
            dpu_rb_view.inner.size(), m_dmaing_host_rb_rep.free_size());
        auto cond = [&, expected_size = dpu_rb_view.inner.size()]() -> bool {
            return m_dmaing_host_rb_rep.free_size() >= expected_size;
        };
        auto action = [&] {
            auto hs = *m_dmaing_host_rb_rep.write(dpu_rb_view.inner.size());
            // TODO: 添加 DMA size 的最小阈值
            dpu_rb_view.status = DpuRbViewStatus::kDmaInProgress;
            auto ds = m_dpu_rb.slice(dpu_rb_view.inner);
            m_dma->post_memcpyv(*m_dpu_mmap, *m_host_mmap,
                make_memcpy_segs(ds, hs), 
                [&] {
                    SPDLOG_INFO("success transferring data[size={}] to host_resp_rb",
                        dpu_rb_view.inner.size());
                    dpu_rb_view.status = DpuRbViewStatus::kDmaDone;
                    for (auto iter = m_dpu_rb_views.begin(); iter != m_dpu_rb_views.end();
                        iter = m_dpu_rb_views.erase(iter)) {
                        if (iter->status != DpuRbViewStatus::kDmaDone) {
                            break;
                        }
                        m_dma_done_size += iter->inner.size();
                    }
                    try_send_size_to_add();
                    m_dpu_rb_space_wait_eq.try_trigger();
                });
        };

        m_host_rb_space_wait_eq.do_or_wait(std::move(cond), std::move(action));
    }

    void try_send_size_to_add() {
        // TODO: 或许可以添加发送的最小阈值控制对端处理的粒度
        if (m_host_wait_size == 0) {
            return;
        }
        size_t target = std::min(m_host_rb_rep.free_size(), m_host_wait_size);
        if (m_dma_done_size >= target) {
            auto *d2h_rb_meta_msg = (D2HRbMetaMsg *)m_producer->buf_data();
            d2h_rb_meta_msg->type = D2HRbMetaMsg::Type::kRespRbNewSize;
            d2h_rb_meta_msg->payload.resp_rb_new_size = target;
            auto ret = m_producer->try_submit_send_task(sizeof(*d2h_rb_meta_msg));
            assert(ret);
            SPDLOG_INFO("send dma_done_size={} to host_resp_rb", target);

            m_host_rb_rep.write(target);
            m_dma_done_size -= target;
            m_host_wait_size -= target;
        }
    }

    // 因为用了结构化绑定，因此修改成员顺序时注意对结构化绑定顺序也进行调整
    size_t m_dma_done_size{};
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
        char *host_req_mem_addr, size_t host_req_mem_size,
        size_t dpu_resp_rb_cap, ConstBytesView host_resp_mmap_desc,
        char *host_resp_mem_addr, size_t host_resp_mem_size,
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
    std::unique_ptr<SingleTaskProducer> m_producer;
};

} // namespace pulsedb
