#pragma once

#include <doca_dev.h>
#include <doca_types.h>
#include <functional>
#include <memory>
#include <utility>

#include "docacxx/ctx/dma.h"
#include "docacxx/mem.h"
#include "docacxx/task_context.h"
#include "part_iterator.h"

namespace pulsedb {
class QueueDma: public docacxx::Dma {
public:
    using CustomMemcpyCb = std::function<void()>;

    QueueDma(doca_pe *pe, doca_dev *dev, size_t memcpy_task_slot_num)
        :m_pe(pe), docacxx::Dma(pe, dev, docacxx::Dma::Cfg{memcpy_task_slot_num}),
        m_buf_inv(dev, 2 * memcpy_task_slot_num), m_memcpy_ctx(memcpy_task_slot_num) {
        docacxx::Dma::set_memcpy_cb(m_memcpy_ctx.make_pipeline_submitter());
    }

    void post_memcpy(docacxx::Mmap &src_mmap, docacxx::Mmap &dst_mmap, 
        size_t offset, size_t size, std::function<void()> cb) {
        if (size > cap().m_max_buf_size) {
            PartIterator part_iter(size, cap().m_max_buf_size);
            auto part_cnt = std::make_shared<size_t>();
            while (part_iter.has_next()) {
                auto [part_offset, part_size] = part_iter.next();
                ++(*part_cnt);
                m_memcpy_ctx.submit([&, part_cnt] {
                    auto src = m_buf_inv.get_buf(src_mmap, part_offset, part_size).release();
                    auto dst = m_buf_inv.get_buf(dst_mmap, part_offset, part_size).release();
                    docacxx::Dma::alloc_memcpy_task(*src, *dst, [&, cb = std::move(cb)] {
                        delete src;
                        delete dst;
                        if ((--(*part_cnt)) == 0) {
                            cb();
                        }
                    });
                });
            }
        }

        m_memcpy_ctx.submit([&] {
            auto src = m_buf_inv.get_buf(src_mmap, offset, size).release();
            auto dst = m_buf_inv.get_buf(dst_mmap, offset, size).release();
            docacxx::Dma::alloc_memcpy_task(*src, *dst, [&, cb = std::move(cb)] {
                delete src;
                delete dst;
                cb();
            });
        });
    }

private:
    using docacxx::Dma::alloc_memcpy_task;
    using docacxx::Dma::submit;

    doca_pe *m_pe;
    docacxx::BufInventory m_buf_inv;
    TaskContext m_memcpy_ctx;
};
}
