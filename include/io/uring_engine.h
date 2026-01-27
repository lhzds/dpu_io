#pragma once

#include <bits/types/struct_iovec.h>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <spdlog/spdlog.h>
#include <thread>
#include <unistd.h>

#include "io/sst_pool.h"
#include "io_engine.h"

namespace pulsedb {

// public 为前台线程接口，参考 doca pe 设计，非线程安全
class UringEngine: public IoEngine {
public:
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

    void write_async(size_t file_number, size_t offset, int base_size_multiplier,
        const RingBuffer::View &src, CallbackFn *cb) override {
        SPDLOG_INFO("[Io-Engine] Issue request[Write: file_number={}, offset={}, size={}]",
            file_number, offset, src.size());
        FileCtx *ctx{};
        auto iter = m_writing_files.find(file_number);
        if (iter == m_writing_files.end()) {
            ctx = init_file_ctx(file_number, base_size_multiplier);
        } else {
            ctx = iter->second.get();
        }

        ++ctx->write_cnt;
        auto *io_data = new IoData;
        io_data->op = Op::kWrite;
        io_data->ctx = ctx;
        io_data->cb = cb;
        issue_write(ctx->os_fd, offset, src, io_data);
    }

    void range_sync_async(size_t file_number, size_t offset, size_t size,
        bool wait_before, CallbackFn *cb) override {
        auto iter = m_writing_files.find(file_number);
        assert(iter != m_writing_files.end()); // write 最先提交，ctx 必然已被创建
        FileCtx *ctx = iter->second.get();

        ++ctx->range_sync_cnt;
        auto *io_data = new IoData;
        io_data->op = Op::kRangeSync;
        io_data->ctx = ctx;
        io_data->cb = cb;
        issue_range_sync(ctx->os_fd, offset, size, wait_before, io_data);
    }

    void sync_close_async(size_t file_number, bool use_fsync, CallbackFn *cb) override {
        auto iter = m_writing_files.find(file_number);
        assert(iter != m_writing_files.end()); // write 最先提交，ctx 必然已被创建
        FileCtx *ctx = iter->second.get();

        auto &sync_close_ctx = ctx->sync_close_ctx;
        sync_close_ctx.is_valid = true;
        sync_close_ctx.use_fsync = use_fsync;
        sync_close_ctx.cb = cb;
        try_issue_sync_close(ctx);
    }

    bool query() override {
        IoData *io_data{};
        if (not m_cq.pop(io_data)) {
            return false;
        }

        auto &ctx = io_data->ctx;
        int file_number = ctx->file_number;
        if (io_data->op == Op::kWrite) {
            --ctx->write_cnt;
            try_issue_sync_close(ctx);
            if (io_data->cb != nullptr) {
                io_data->cb->operator()(file_number, io_data->res);
            }
        } else if (io_data->op == Op::kRangeSync) {
            --ctx->range_sync_cnt;
            try_issue_sync_close(ctx);
            if (io_data->cb != nullptr) {
                io_data->cb->operator()(file_number, io_data->res);
            }
        } else if (io_data->op == Op::kSyncClose) {
            // sync 和 close 完成顺序不固定，最后一个完成的调用生成回复
            auto &sync_close_ctx = io_data->ctx->sync_close_ctx;
            if ((++sync_close_ctx.seen_cqe_cnt) == 2) {
                io_data->res = ctx->slot_id;
                m_writing_files.erase(file_number);
                if (sync_close_ctx.cb != nullptr) {
                    sync_close_ctx.cb->operator()(file_number, io_data->res);
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
    struct FileCtx{
        size_t file_number{}; // 从 1 开始
        int slot_id{-1}; // 从 0 开始
        int os_fd{-1};
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
        } sync_close_ctx{};
    };
    struct IoData {
        Op op;
        int res{};
        FileCtx *ctx{};
        CallbackFn *cb{};
        iovec iovs[2]{};
    };
    using CompleteQueue = boost::lockfree::spsc_queue<IoData *,
        boost::lockfree::capacity<kIoEngineCqDepth>>;

    FileCtx *init_file_ctx(size_t file_number, int base_size_multiplier) {
        auto ctx = std::make_unique<FileCtx>();
        ctx->file_number = file_number;

        auto slot_id = m_sst_pool->alloc_slot(file_number, base_size_multiplier);
        if (not slot_id.has_value()) {
            throw_with("No free slots");
        }
        ctx->slot_id = *slot_id;

        auto sst_name = m_sst_pool->slot_file_name(*slot_id);
        ctx->os_fd = open(sst_name.c_str(), O_WRONLY);
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
        if (ctx->in_flight_ops() == 0 && ctx->sync_close_ctx.is_valid) {
            SPDLOG_INFO("[Io-Engine] Issue request[SyncClose: file_number={}]",
                ctx->file_number);
            auto *sync_data = new IoData;
            sync_data->op = Op::kSyncClose;
            sync_data->ctx = ctx;
            auto *close_data = new IoData;
            close_data->op = Op::kSyncClose;
            close_data->ctx = ctx;
            issue_sync_close(ctx->os_fd, ctx->sync_close_ctx.use_fsync,
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
        }
    }

    CompleteQueue m_cq;

    std::thread m_poller;
    std::atomic<bool> m_is_killed{};
    std::atomic<int> m_uring_cqe_cnt{}; // 用于保证 uring 可以正常退出

    io_uring *m_uring{};
    std::unique_ptr<SstPool> m_sst_pool;
    std::unordered_map<size_t, std::unique_ptr<FileCtx>> m_writing_files;
};

} // namespace pulsedb
