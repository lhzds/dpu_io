#pragma once

#include <atomic>
#include <chrono>
#include <doca_dev.h>
#include <doca_types.h>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "docacxx/ctx/dma.h"
#include "docacxx/mem.h"
#include "docacxx/task_context.hpp"
#include "part_iterator.h"
#include "util.h"

namespace pulsedb {
class QueueDma: public docacxx::Dma {
public:
    using CustomMemcpyCb = std::function<void()>;
    struct MemcpySeg {
        size_t src_offset;
        size_t dst_offset;
        size_t size;
    };
    struct StatsSnapshot {
        uint64_t total_bytes{};
        uint64_t total_tasks{};
        uint64_t inflight_tasks{};
        uint64_t queued_tasks{};
        uint64_t memcpy_ns_total{};
        uint64_t memcpy_cnt{};
        uint64_t memcpy_ns_max{};
    };

    QueueDma(doca_pe *pe, doca_dev *dev, size_t memcpy_task_slot_num)
        :Dma(pe, dev, Cfg{memcpy_task_slot_num}),
        m_buf_inv(dev, 2 * memcpy_task_slot_num), m_memcpy_ctx(memcpy_task_slot_num) {
        set_memcpy_cb(m_memcpy_ctx.make_pipeline_submitter());
    }

    StatsSnapshot stats_snapshot() const {
        return StatsSnapshot{
            m_stats_total_bytes.load(std::memory_order_relaxed),
            m_stats_total_tasks.load(std::memory_order_relaxed),
            m_stats_inflight_tasks.load(std::memory_order_relaxed),
            m_stats_queued_tasks.load(std::memory_order_relaxed),
            m_stats_memcpy_ns_total.load(std::memory_order_relaxed),
            m_stats_memcpy_cnt.load(std::memory_order_relaxed),
            m_stats_memcpy_ns_max.load(std::memory_order_relaxed)
        };
    }

    void reset_stats() {
        m_stats_total_bytes.store(0, std::memory_order_relaxed);
        m_stats_total_tasks.store(0, std::memory_order_relaxed);
        m_stats_inflight_tasks.store(0, std::memory_order_relaxed);
        m_stats_queued_tasks.store(0, std::memory_order_relaxed);
        m_stats_memcpy_ns_total.store(0, std::memory_order_relaxed);
        m_stats_memcpy_cnt.store(0, std::memory_order_relaxed);
        m_stats_memcpy_ns_max.store(0, std::memory_order_relaxed);
    }

    void post_memcpy(docacxx::Mmap &src_mmap, size_t src_offset, docacxx::Mmap &dst_mmap, 
        size_t dst_offset, size_t size, std::function<void()> cb) {
        if (size > cap().max_buf_size) {
            PartIterator part_iter(size, cap().max_buf_size);
            auto *heap_cb = new std::function<void()>(std::move(cb));
            std::shared_ptr<std::function<void()>> shared_cb(heap_cb);
            
            while (part_iter.has_next()) {
                auto [part_offset, part_size] = part_iter.next();
                bool will_queue = m_memcpy_ctx.processing_task_cnt() >= m_memcpy_ctx.task_slot_num();
                if (will_queue) {
                    m_stats_queued_tasks.fetch_add(1, std::memory_order_relaxed);
                }
                m_memcpy_ctx.submit([&, size, shared_cb, src_offset, dst_offset,
                                     part_offset = part_offset, part_size = part_size, will_queue] {
                    if (will_queue) {
                        m_stats_queued_tasks.fetch_add(-1, std::memory_order_relaxed);
                    }
                    m_stats_inflight_tasks.fetch_add(1, std::memory_order_relaxed);
                    auto start_ts = std::chrono::steady_clock::now();
                    auto src = m_buf_inv.get_buf(src_mmap, src_offset + part_offset, part_size).release();
                    src->set_doca_buf_data(part_size);
                    auto dst = m_buf_inv.get_buf(dst_mmap, dst_offset + part_offset, part_size).release();

                    auto task = alloc_memcpy_task(*src, *dst,
                        [this, src, dst, shared_cb, part_size, start_ts](const MemcpyTask &) {
                        delete src;
                        delete dst;
                        m_stats_total_bytes.fetch_add(part_size, std::memory_order_relaxed);
                        m_stats_total_tasks.fetch_add(1, std::memory_order_relaxed);
                        m_stats_inflight_tasks.fetch_add(-1, std::memory_order_relaxed);
                        auto end_ts = std::chrono::steady_clock::now();
                        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
                        if (ns > 0) {
                            m_stats_memcpy_ns_total.fetch_add(
                                static_cast<uint64_t>(ns), std::memory_order_relaxed);
                            m_stats_memcpy_cnt.fetch_add(1, std::memory_order_relaxed);
                            auto prev_max = m_stats_memcpy_ns_max.load(std::memory_order_relaxed);
                            while (ns > static_cast<int64_t>(prev_max) &&
                                   !m_stats_memcpy_ns_max.compare_exchange_weak(
                                       prev_max, static_cast<uint64_t>(ns), std::memory_order_relaxed)) {
                            }
                        }
                        if (shared_cb.use_count() == 1) {
                            shared_cb->operator()();
                        }
                    });
                    auto ret = submit(std::move(task));
                    if (ret != nullptr) {
                        pulsedb::throw_with("QueueDma: Submit failed");
                    }
                });
            }

            return;
        }

        bool will_queue = m_memcpy_ctx.processing_task_cnt() >= m_memcpy_ctx.task_slot_num();
        if (will_queue) {
            m_stats_queued_tasks.fetch_add(1, std::memory_order_relaxed);
        }
        m_memcpy_ctx.submit([&, src_offset, dst_offset, size, cb = std::move(cb), will_queue] {
            if (will_queue) {
                m_stats_queued_tasks.fetch_add(-1, std::memory_order_relaxed);
            }
            m_stats_inflight_tasks.fetch_add(1, std::memory_order_relaxed);
            auto start_ts = std::chrono::steady_clock::now();
            auto src = m_buf_inv.get_buf(src_mmap, src_offset, size).release();
            src->set_doca_buf_data(size);
            auto dst = m_buf_inv.get_buf(dst_mmap, dst_offset, size).release();
            auto task = alloc_memcpy_task(*src, *dst,
                [this, src, dst, size, cb = std::move(cb), start_ts](const MemcpyTask &) {
                delete src;
                delete dst;
                m_stats_total_bytes.fetch_add(size, std::memory_order_relaxed);
                m_stats_total_tasks.fetch_add(1, std::memory_order_relaxed);
                m_stats_inflight_tasks.fetch_add(-1, std::memory_order_relaxed);
                auto end_ts = std::chrono::steady_clock::now();
                auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
                if (ns > 0) {
                    m_stats_memcpy_ns_total.fetch_add(
                        static_cast<uint64_t>(ns), std::memory_order_relaxed);
                    m_stats_memcpy_cnt.fetch_add(1, std::memory_order_relaxed);
                    auto prev_max = m_stats_memcpy_ns_max.load(std::memory_order_relaxed);
                    while (ns > static_cast<int64_t>(prev_max) &&
                           !m_stats_memcpy_ns_max.compare_exchange_weak(
                               prev_max, static_cast<uint64_t>(ns), std::memory_order_relaxed)) {
                    }
                }
                cb();
            });
            auto ret = submit(std::move(task));
            if (ret != nullptr) {
                throw_with("QueueDma: Submit failed");
            }
        });
    }

    void post_memcpyv(docacxx::Mmap &src_mmap, docacxx::Mmap &dst_mmap,
        const std::vector<MemcpySeg> &memcpy_segs, std::function<void()> cb) {
        auto *heap_cb = new std::function<void()>(std::move(cb));
        std::shared_ptr<std::function<void()>> shared_cb(heap_cb);
        for (const auto &seg : memcpy_segs) {
            post_memcpy(src_mmap, seg.src_offset, dst_mmap, seg.dst_offset, seg.size, [shared_cb] {
                if (shared_cb.use_count() == 1) { // 确保只有在 memcpy 成功时才会调用 cb
                    shared_cb->operator()();
                }
            });
        }
    }

private:
    using Dma::Cfg;
    using Dma::alloc_memcpy_task;
    using Dma::submit;
    using Dma::set_memcpy_cb;
    using Dma::set_memcpy_error_cb;

    docacxx::BufInventory m_buf_inv;
    docacxx::TaskContext<MemcpyTask> m_memcpy_ctx;
    std::atomic<uint64_t> m_stats_total_bytes{};
    std::atomic<uint64_t> m_stats_total_tasks{};
    std::atomic<uint64_t> m_stats_inflight_tasks{};
    std::atomic<uint64_t> m_stats_queued_tasks{};
    std::atomic<uint64_t> m_stats_memcpy_ns_total{};
    std::atomic<uint64_t> m_stats_memcpy_cnt{};
    std::atomic<uint64_t> m_stats_memcpy_ns_max{};
};
}
