#pragma once

#include <atomic>
#include <chrono>
#include <bits/types/struct_iovec.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <spdlog/spdlog.h>
#include <thread>
#include <unistd.h>

#include "constant.h"
#include "io/sst_pool.h"
#include "io_engine.h"

namespace pulsedb {

// public 为前台线程接口，参考 doca pe 设计，非线程安全
class UringEngine: public IoEngine {
public:
    struct StatsSnapshot {
        uint64_t write_bytes{};
        uint64_t write_ops{};
        uint64_t range_sync_ops{};
        uint64_t sync_close_ops{};
        uint64_t uring_cqe_backlog{};
        uint64_t cq_backlog{};
        uint64_t write_ns_total{};
        uint64_t write_cnt{};
        uint64_t write_ns_max{};
        uint64_t range_sync_ns_total{};
        uint64_t range_sync_cnt{};
        uint64_t range_sync_ns_max{};
        uint64_t sync_close_ns_total{};
        uint64_t sync_close_cnt{};
        uint64_t sync_close_ns_max{};
        uint64_t sync_close_wait_ns_total{};
        uint64_t sync_close_wait_cnt{};
        uint64_t sync_close_wait_ns_max{};
    };

    UringEngine(uint32_t queue_depth, std::unique_ptr<SstPool> sst_pool)
        : m_uring(new io_uring), m_sst_pool(std::move(sst_pool)) {
        if (auto ret = io_uring_queue_init(queue_depth, m_uring, 0); ret < 0) {
            throw_with("UringEngine: Failed to create init uring queue: {}", strerror(errno));
        }

        m_poller = std::thread([&] {
            poll();
        });
    }
    virtual ~UringEngine() {
        m_is_killed = true;
        if (m_poller.joinable()) {
            m_poller.join();
        }
        io_uring_queue_exit(m_uring);
        delete m_uring;
    }

    StatsSnapshot stats_snapshot() const {
        return StatsSnapshot{
            m_stats_write_bytes.load(std::memory_order_relaxed),
            m_stats_write_ops.load(std::memory_order_relaxed),
            m_stats_range_sync_ops.load(std::memory_order_relaxed),
            m_stats_sync_close_ops.load(std::memory_order_relaxed),
            static_cast<uint64_t>(m_uring_cqe_cnt.load(std::memory_order_relaxed)),
            m_stats_cq_size.load(std::memory_order_relaxed),
            m_stats_write_ns_total.load(std::memory_order_relaxed),
            m_stats_write_cnt.load(std::memory_order_relaxed),
            m_stats_write_ns_max.load(std::memory_order_relaxed),
            m_stats_range_sync_ns_total.load(std::memory_order_relaxed),
            m_stats_range_sync_cnt.load(std::memory_order_relaxed),
            m_stats_range_sync_ns_max.load(std::memory_order_relaxed),
            m_stats_sync_close_ns_total.load(std::memory_order_relaxed),
            m_stats_sync_close_cnt.load(std::memory_order_relaxed),
            m_stats_sync_close_ns_max.load(std::memory_order_relaxed),
            m_stats_sync_close_wait_ns_total.load(std::memory_order_relaxed),
            m_stats_sync_close_wait_cnt.load(std::memory_order_relaxed),
            m_stats_sync_close_wait_ns_max.load(std::memory_order_relaxed)
        };
    }

    void reset_stats() {
        m_stats_write_bytes.store(0, std::memory_order_relaxed);
        m_stats_write_ops.store(0, std::memory_order_relaxed);
        m_stats_range_sync_ops.store(0, std::memory_order_relaxed);
        m_stats_sync_close_ops.store(0, std::memory_order_relaxed);
        m_stats_cq_size.store(0, std::memory_order_relaxed);
        m_stats_write_ns_total.store(0, std::memory_order_relaxed);
        m_stats_write_cnt.store(0, std::memory_order_relaxed);
        m_stats_write_ns_max.store(0, std::memory_order_relaxed);
        m_stats_range_sync_ns_total.store(0, std::memory_order_relaxed);
        m_stats_range_sync_cnt.store(0, std::memory_order_relaxed);
        m_stats_range_sync_ns_max.store(0, std::memory_order_relaxed);
        m_stats_sync_close_ns_total.store(0, std::memory_order_relaxed);
        m_stats_sync_close_cnt.store(0, std::memory_order_relaxed);
        m_stats_sync_close_ns_max.store(0, std::memory_order_relaxed);
        m_stats_sync_close_wait_ns_total.store(0, std::memory_order_relaxed);
        m_stats_sync_close_wait_cnt.store(0, std::memory_order_relaxed);
        m_stats_sync_close_wait_ns_max.store(0, std::memory_order_relaxed);
    }

    static constexpr size_t direct_alignment() noexcept {
        return kDirectAlignment;
    }

    void write_async(size_t file_number, size_t offset, int base_size_multiplier,
        const RingBuffer::View &src, CallbackFn *cb, bool direct_if_open) override {
        SPDLOG_INFO("[Io-Engine] Issue request[Write: file_number={}, offset={}, size={}]",
            file_number, offset, src.size());
        FileCtx *ctx{};
        auto iter = m_writing_files.find(file_number);
        if (iter == m_writing_files.end()) {
            ctx = init_file_ctx(file_number, base_size_multiplier, direct_if_open);
        } else {
            ctx = iter->second.get();
        }
        if (ctx->is_direct != direct_if_open) {
            throw_with("UringEngine: Mixed direct mode on file_number={}", file_number);
        }
        if (ctx->is_direct) {
            validate_direct_write(offset, src);
        }

        ++ctx->write_cnt;
        auto *io_data = new IoData;
        io_data->op = Op::kWrite;
        io_data->ctx = ctx;
        io_data->cb = cb;
        io_data->submit_ts = std::chrono::steady_clock::now();
        issue_write(ctx->os_fd, offset, src, io_data);
    }

    void range_sync_async(size_t file_number, size_t offset, size_t size,
        bool wait_before, CallbackFn *cb) override {
        auto iter = m_writing_files.find(file_number);
        assert(iter != m_writing_files.end()); // write 最先提交，ctx 必然已被创建
        FileCtx *ctx = iter->second.get();
        if (ctx->is_direct) {
            if (cb != nullptr) {
                // 回调函数由调用线程执行，直接返回
                cb->operator()(file_number, 0);
            }
            return;
        }

        ++ctx->range_sync_cnt;
        auto *io_data = new IoData;
        io_data->op = Op::kRangeSync;
        io_data->ctx = ctx;
        io_data->cb = cb;
        io_data->submit_ts = std::chrono::steady_clock::now();
        issue_range_sync(ctx->os_fd, offset, size, wait_before, io_data);
    }

    void sync_close_async(size_t file_number, bool use_fsync, CallbackFn *cb) override {
        auto iter = m_writing_files.find(file_number);
        assert(iter != m_writing_files.end()); // write 最先提交，ctx 必然已被创建
        FileCtx *ctx = iter->second.get();

        auto &sync_close_ctx = ctx->sync_close_ctx;
        sync_close_ctx.is_valid = true;
        if (ctx->is_direct) {
            sync_close_ctx.use_fsync = true; // force fsync for durability
        } else {
            sync_close_ctx.use_fsync = use_fsync;
        }
        sync_close_ctx.cb = cb;
        try_issue_sync_close(ctx);
    }

    bool query() override {
        IoData *io_data{};
        if (not m_cq.pop(io_data)) {
            return false;
        }
        m_stats_cq_size.fetch_add(-1, std::memory_order_relaxed);

        auto &ctx = io_data->ctx;
        int file_number = ctx->file_number;
        if (io_data->op == Op::kWrite) {
            --ctx->write_cnt;
            m_stats_write_bytes.fetch_add(static_cast<uint64_t>(io_data->res), std::memory_order_relaxed);
            m_stats_write_ops.fetch_add(1, std::memory_order_relaxed);
            record_latency(io_data->submit_ts, m_stats_write_ns_total,
                m_stats_write_cnt, m_stats_write_ns_max);
            try_issue_sync_close(ctx);
            if (io_data->cb != nullptr) {
                io_data->cb->operator()(file_number, io_data->res);
            }
        } else if (io_data->op == Op::kRangeSync) {
            --ctx->range_sync_cnt;
            m_stats_range_sync_ops.fetch_add(1, std::memory_order_relaxed);
            record_latency(io_data->submit_ts, m_stats_range_sync_ns_total,
                m_stats_range_sync_cnt, m_stats_range_sync_ns_max);
            try_issue_sync_close(ctx);
            if (io_data->cb != nullptr) {
                io_data->cb->operator()(file_number, io_data->res);
            }
        } else if (io_data->op == Op::kSyncClose) {
            // sync 和 close 完成顺序不固定，最后一个完成的调用生成回复
            auto &sync_close_ctx = io_data->ctx->sync_close_ctx;
            if ((++sync_close_ctx.seen_cqe_cnt) == 2) {
                m_stats_sync_close_ops.fetch_add(1, std::memory_order_relaxed);
                if (sync_close_ctx.issue_started) {
                    record_latency(sync_close_ctx.issue_ts, m_stats_sync_close_ns_total,
                        m_stats_sync_close_cnt, m_stats_sync_close_ns_max);
                    sync_close_ctx.issue_started = false;
                }
                auto *cb = sync_close_ctx.cb;
                sync_close_ctx.cb = nullptr;
                io_data->res = ctx->slot_id;
                m_writing_files.erase(file_number);
                if (cb != nullptr) {
                    cb->operator()(file_number, io_data->res);
                }
            }
        } else {
            throw_with("UringEngine: Unexpected op type");
        }

        delete io_data;
        return true;
    }

    void delete_file(size_t file_number) override {
        // 可能删除没有创建的 SST 文件
        if (not m_sst_pool->has_slot(file_number)) {
            return;
        }
        m_sst_pool->free_slot(file_number);
    }

private:    
    void record_latency(const std::chrono::steady_clock::time_point &start_ts,
        std::atomic<uint64_t> &total,
        std::atomic<uint64_t> &cnt,
        std::atomic<uint64_t> &max) {
        auto end_ts = std::chrono::steady_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_ts - start_ts).count();
        if (ns <= 0) {
            return;
        }
        total.fetch_add(static_cast<uint64_t>(ns), std::memory_order_relaxed);
        cnt.fetch_add(1, std::memory_order_relaxed);
        auto prev_max = max.load(std::memory_order_relaxed);
        while (ns > static_cast<int64_t>(prev_max) &&
               !max.compare_exchange_weak(
                   prev_max, static_cast<uint64_t>(ns), std::memory_order_relaxed)) {
        }
    }

    struct FileCtx{
        size_t file_number{}; // 从 1 开始
        int slot_id{-1}; // 从 0 开始
        int os_fd{-1};
        bool is_direct{};
        int write_cnt{};
        int range_sync_cnt{};

        int in_flight_ops() const {
            return write_cnt + range_sync_cnt;
        }

        struct SyncCloseCtx {
            bool is_valid{};
            int seen_cqe_cnt{};
            bool use_fsync{};
            CallbackFn *cb{};
            bool wait_started{};
            std::chrono::steady_clock::time_point wait_start_ts{};
            bool issue_started{};
            std::chrono::steady_clock::time_point issue_ts{};
        } sync_close_ctx{};
    };
    struct IoData {
        Op op;
        int res{};
        FileCtx *ctx{};
        CallbackFn *cb{};
        iovec iovs[2]{};
        std::chrono::steady_clock::time_point submit_ts{};
    };
    using CompleteQueue = boost::lockfree::spsc_queue<IoData *,
        boost::lockfree::capacity<kIoEngineCqDepth>>;

    FileCtx *init_file_ctx(size_t file_number, int base_size_multiplier, bool direct_if_open) {
        auto ctx = std::make_unique<FileCtx>();
        ctx->file_number = file_number;
        ctx->is_direct = direct_if_open;

        auto slot_id = m_sst_pool->alloc_slot(file_number, base_size_multiplier);
        if (not slot_id.has_value()) {
            throw_with("No free slots");
        }
        ctx->slot_id = *slot_id;

        auto sst_name = m_sst_pool->slot_file_name(*slot_id);
        int open_flags = O_WRONLY;
        if (direct_if_open) {
            open_flags |= O_DIRECT;
        }
        ctx->os_fd = open(sst_name.c_str(), open_flags);
        if (ctx->os_fd < 0) {
            throw_with("UringEngine: Failed to open {}: {}", sst_name, strerror(errno));
        }

        auto [iter, success] = m_writing_files.emplace(file_number, std::move(ctx));
        assert(success);
        return iter->second.get();
    }

    void issue_write(int fd, size_t offset, const RingBuffer::View &src, IoData *io_data) {
        io_uring_sqe *sqe = io_uring_get_sqe(m_uring);
        if (sqe == nullptr) {
            throw_with("UringEngine: Failed to get write sqe: {}", strerror(errno));
        }

        io_data->iovs[0].iov_base = src.first_part().data();
        io_data->iovs[0].iov_len = src.first_part().size();
        if (src.second_part().has_value()) {
            io_data->iovs[1].iov_base = src.second_part()->data();
            io_data->iovs[1].iov_len = src.second_part()->size();
            io_uring_prep_writev(sqe, fd, io_data->iovs, 2, offset);
        } else {
            io_uring_prep_writev(sqe, fd, io_data->iovs, 1, offset);
        }
        sqe->user_data = reinterpret_cast<uint64_t>(io_data);
        
        if (auto ret = io_uring_submit(m_uring); ret < 0) {
            throw_with("UringEngine: Failed to submit write: {}", strerror(errno));
        }
        m_uring_cqe_cnt.fetch_add(1);
    }

    void issue_range_sync(int fd, size_t offset, size_t size,
                          bool wait_before, IoData *io_data) {
        io_uring_sqe *sqe = io_uring_get_sqe(m_uring);
        if (sqe == nullptr) {
            throw_with("UringEngine: Failed to get range sync sqe: {}", strerror(errno));
        }

        if (wait_before) {
            io_uring_prep_sync_file_range(sqe, fd, size + offset, 0,
                SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE);
        } else {
            io_uring_prep_sync_file_range(sqe, fd, size, offset,
                SYNC_FILE_RANGE_WRITE);
        }
        sqe->user_data = reinterpret_cast<uint64_t>(io_data);

        if (auto ret = io_uring_submit(m_uring); ret < 0) {
            throw_with("UringEngine: Failed to submit range sync: {}", strerror(errno));
        }
        m_uring_cqe_cnt.fetch_add(1);
    }

    void try_issue_sync_close(FileCtx *ctx) {
        auto &sync_close_ctx = ctx->sync_close_ctx;
        if (not sync_close_ctx.is_valid) {
            return;
        }
        if (ctx->in_flight_ops() > 0) {
            if (!sync_close_ctx.wait_started) {
                sync_close_ctx.wait_started = true;
                sync_close_ctx.wait_start_ts = std::chrono::steady_clock::now();
            }
            return;
        }
        if (ctx->in_flight_ops() == 0) {
            SPDLOG_INFO("[Io-Engine] Issue request[SyncClose: file_number={}]",
                ctx->file_number);
            sync_close_ctx.issue_started = true;
            sync_close_ctx.issue_ts = std::chrono::steady_clock::now();
            if (sync_close_ctx.wait_started) {
                auto now_ts = std::chrono::steady_clock::now();
                auto wait_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now_ts - sync_close_ctx.wait_start_ts).count();
                if (wait_ns > 0) {
                    m_stats_sync_close_wait_ns_total.fetch_add(
                        static_cast<uint64_t>(wait_ns), std::memory_order_relaxed);
                    m_stats_sync_close_wait_cnt.fetch_add(1, std::memory_order_relaxed);
                    auto prev_max = m_stats_sync_close_wait_ns_max.load(std::memory_order_relaxed);
                    while (wait_ns > static_cast<int64_t>(prev_max) &&
                           !m_stats_sync_close_wait_ns_max.compare_exchange_weak(
                               prev_max, static_cast<uint64_t>(wait_ns), std::memory_order_relaxed)) {
                    }
                }
                sync_close_ctx.wait_started = false;
            }
            auto *sync_data = new IoData;
            sync_data->op = Op::kSyncClose;
            sync_data->ctx = ctx;
            auto *close_data = new IoData;
            close_data->op = Op::kSyncClose;
            close_data->ctx = ctx;
            issue_sync_close(ctx->os_fd, sync_close_ctx.use_fsync,
                sync_data, close_data);
        }
    }

    void issue_sync_close(int fd, bool use_fsync, IoData *sync_data, IoData *close_data) {
        io_uring_sqe *sync_sqe = io_uring_get_sqe(m_uring);
        if (sync_sqe == nullptr) {
            throw_with("UringEngine: Failed to get sync sqe: {}", strerror(errno));
        }
        io_uring_sqe_set_flags(sync_sqe, IOSQE_IO_LINK);
        if (use_fsync) {
            io_uring_prep_fsync(sync_sqe, fd, 0);
        } else {
            io_uring_prep_fsync(sync_sqe, fd, IORING_FSYNC_DATASYNC);
        }
        sync_sqe->user_data = reinterpret_cast<uint64_t>(sync_data);

        io_uring_sqe *close_sqe = io_uring_get_sqe(m_uring);
        if (close_sqe == nullptr) {
            throw_with("UringEngine: Failed to get close sqe: {}", strerror(errno));
        }
        io_uring_prep_close(close_sqe, fd);
        close_sqe->user_data = reinterpret_cast<uint64_t>(close_data);

        if (auto ret = io_uring_submit(m_uring); ret < 0) {
            throw_with("UringEngine: Failed to submit sync & close: {}", strerror(errno));
        }
        m_uring_cqe_cnt.fetch_add(2); // fetch_add 原子加法
    }

    void poll() {
        io_uring_cqe *cqe{};
        int req_cnt{};

        while (((req_cnt = m_uring_cqe_cnt.load()) > 0) // 先获取最新的 req_cnt
                || not m_is_killed) {
            if (req_cnt == 0) {
                continue;
            }

            if (auto ret = io_uring_wait_cqe(m_uring, &cqe); ret < 0) {
                throw_with("UringEngine: Wait cqe: {}", strerror(errno));
            }
            if (cqe->res < 0) {
                throw_with("UringEngine: Io error: {}", strerror(-cqe->res));
            }
            int res = cqe->res;
            auto *io_data = (IoData *)cqe->user_data;
            io_uring_cqe_seen(m_uring, cqe);
            m_uring_cqe_cnt.fetch_add(-1);

            io_data->res = res;
            while (not m_cq.push(io_data)) {
                std::this_thread::yield();
            }
            m_stats_cq_size.fetch_add(1, std::memory_order_relaxed);
        }
    }

    CompleteQueue m_cq;

    std::thread m_poller;
    std::atomic<bool> m_is_killed{};
    std::atomic<int> m_uring_cqe_cnt{}; // 用于保证 uring 可以正常退出

    std::atomic<uint64_t> m_stats_write_bytes{};
    std::atomic<uint64_t> m_stats_write_ops{};
    std::atomic<uint64_t> m_stats_range_sync_ops{};
    std::atomic<uint64_t> m_stats_sync_close_ops{};
    std::atomic<uint64_t> m_stats_cq_size{};
    std::atomic<uint64_t> m_stats_write_ns_total{};
    std::atomic<uint64_t> m_stats_write_cnt{};
    std::atomic<uint64_t> m_stats_write_ns_max{};
    std::atomic<uint64_t> m_stats_range_sync_ns_total{};
    std::atomic<uint64_t> m_stats_range_sync_cnt{};
    std::atomic<uint64_t> m_stats_range_sync_ns_max{};
    std::atomic<uint64_t> m_stats_sync_close_ns_total{};
    std::atomic<uint64_t> m_stats_sync_close_cnt{};
    std::atomic<uint64_t> m_stats_sync_close_ns_max{};
    std::atomic<uint64_t> m_stats_sync_close_wait_ns_total{};
    std::atomic<uint64_t> m_stats_sync_close_wait_cnt{};
    std::atomic<uint64_t> m_stats_sync_close_wait_ns_max{};

    io_uring *m_uring{};
    std::unique_ptr<SstPool> m_sst_pool;
    std::unordered_map<size_t, std::unique_ptr<FileCtx>> m_writing_files;

    static constexpr size_t kDirectAlignment = kDefaultDirectAlignment;

    static bool is_aligned_size(size_t value) {
        return (value % kDirectAlignment) == 0;
    }

    static bool is_aligned_ptr(const void *ptr) {
        return (reinterpret_cast<uintptr_t>(ptr) % kDirectAlignment) == 0;
    }

    static void validate_direct_write(size_t offset, const RingBuffer::View &src) {
        if (!is_aligned_size(offset)) {
            throw_with("UringEngine: Direct write offset not aligned: {}", offset);
        }
        const auto &first = src.first_part();
        if (first.size() > 0) {
            if (!is_aligned_ptr(first.data())) {
                throw_with("UringEngine: Direct write buffer not aligned");
            }
            if (!is_aligned_size(first.size())) {
                throw_with("UringEngine: Direct write buffer size not aligned");
            }
        }
        if (src.second_part().has_value()) {
            const auto &second = *src.second_part();
            if (second.size() > 0) {
                if (!is_aligned_ptr(second.data())) {
                    throw_with("UringEngine: Direct write buffer not aligned");
                }
                if (!is_aligned_size(second.size())) {
                    throw_with("UringEngine: Direct write buffer size not aligned");
                }
            }
        }
    }
};

} // namespace pulsedb
