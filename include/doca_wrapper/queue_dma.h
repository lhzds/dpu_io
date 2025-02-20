#pragma once

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

namespace pulsedb {
class QueueDma: public docacxx::Dma {
public:
    using CustomMemcpyCb = std::function<void()>;
    struct MemcpySeg {
        size_t src_offset;
        size_t dst_offset;
        size_t size;
    };

    QueueDma(doca_pe *pe, doca_dev *dev, size_t memcpy_task_slot_num)
        :m_pe(pe), Dma(pe, dev, Cfg{memcpy_task_slot_num}),
        m_buf_inv(dev, 2 * memcpy_task_slot_num), m_memcpy_ctx(memcpy_task_slot_num) {
        set_memcpy_cb(m_memcpy_ctx.make_pipeline_submitter());
    }

    void post_memcpy(docacxx::Mmap &src_mmap, size_t src_offset, docacxx::Mmap &dst_mmap, 
        size_t dst_offset, size_t size, std::function<void()> cb) {
        if (size > cap().max_buf_size) {
            PartIterator part_iter(size, cap().max_buf_size);
            auto *heap_cb = new std::function<void()>(std::move(cb));
            std::shared_ptr<std::function<void()>> shared_cb(heap_cb);
            
            while (part_iter.has_next()) {
                auto [part_offset, part_size] = part_iter.next();
                m_memcpy_ctx.submit([&, size, shared_cb, src_offset, dst_offset, part_offset, part_size] {
                    auto src = m_buf_inv.get_buf(src_mmap, src_offset + part_offset, part_size).release();
                    src->set_doca_buf_data(size);
                    auto dst = m_buf_inv.get_buf(dst_mmap, dst_offset + part_offset, part_size).release();

                    auto task = alloc_memcpy_task(*src, *dst, [src, dst, shared_cb](const MemcpyTask &) {
                        if (shared_cb.use_count() == 1) {
                            shared_cb->operator()();
                        }
                        delete src;
                        delete dst;
                    });
                    submit(std::move(task));
                });
            }

            return;
        }

        m_memcpy_ctx.submit([&, src_offset, dst_offset, size, cb = std::move(cb)] {
            auto src = m_buf_inv.get_buf(src_mmap, src_offset, size).release();
            src->set_doca_buf_data(size);
            auto dst = m_buf_inv.get_buf(dst_mmap, dst_offset, size).release();
            auto task = alloc_memcpy_task(*src, *dst, [=, cb = std::move(cb)](const MemcpyTask &) {
                delete src;
                delete dst;
                cb();
            });
            submit(std::move(task));
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
    using Dma::alloc_memcpy_task;
    using Dma::submit;

    doca_pe *m_pe;
    docacxx::BufInventory m_buf_inv;
    docacxx::TaskContext<MemcpyTask> m_memcpy_ctx;
};
}
