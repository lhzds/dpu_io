#pragma once

#include <bits/types/struct_iovec.h>
#include <cstring>
#include <fcntl.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <spdlog/spdlog.h>
#include <thread>
#include <unistd.h>

#include "io/sst_pool.h"
#include "io_ctx.h"

namespace pulsedb {
class UringCtx: public IoCtx {
public:
    UringCtx(size_t queue_depth, std::unique_ptr<SstPool> sst_pool)
        : m_uring(new io_uring), m_sst_pool(std::move(sst_pool)) {
        if (auto ret = io_uring_queue_init(queue_depth, m_uring, 0); ret < 0) {
            throw_with(fmt::format("Failed to create init uring queue: ", strerror(errno)));
        }

        m_poller = std::thread([&] {
            poll();
        });
    }
    virtual ~UringCtx() {
        m_is_killed = true;
        if (m_poller.joinable()) {
            m_poller.join();
        }
        io_uring_queue_exit(m_uring);
        delete m_uring;
    }

    void write_async(size_t file_number, size_t offset, size_t base_size_multiplier,
        const RingBuffer::View &src, CallbackFn *cb) override {
        FileCtx *ctx{};
        auto iter = m_writing_files.find(file_number);
        if (iter == m_writing_files.end()) {
            ctx = init_write_ctx(file_number, base_size_multiplier);
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

    void sync_close_async(size_t file_number, bool use_fsync, CallbackFn *cb) override {
        auto iter = m_writing_files.find(file_number);
        assert(iter != m_writing_files.end());
        FileCtx *ctx = iter->second.get();
        
        // 先产生 sync_close_req，再查询 write_cnt，避免执行丢失
        // 保证前台线程和后台线程至少有一个提交 sync_close_req
        ctx->sync_close_req = new FileCtx::SyncCloseArg{use_fsync, cb};
        try_issue_sync_close(ctx);
    }

private:    
    struct FileCtx{
        struct SyncCloseArg {
            bool use_fsync{};
            CallbackFn *cb{};
        };
        size_t file_number{};
        int os_fd{-1};
        std::atomic<int> write_cnt{};
        std::atomic<SyncCloseArg *> sync_close_req{};
    };
    struct IoData {
        Op op;
        FileCtx *ctx{};
        CallbackFn *cb{};
        iovec iovs[2]{};
    };


    FileCtx *init_write_ctx(size_t file_number, size_t base_size_multiplier) {
        auto ctx = std::make_unique<FileCtx>();
        ctx->file_number = file_number;

        auto slot_id = m_sst_pool->alloc_slot(file_number, base_size_multiplier);
        assert(slot_id.has_value());
        auto sst_name = m_sst_pool->slot_file_name(*slot_id);
        ctx->os_fd = open(sst_name.c_str(), O_WRONLY);
        if (ctx->os_fd < 0) {
            throw_with("Failed to open {}: {}", sst_name, strerror(errno));
        }

        auto [iter, success] = m_writing_files.emplace(file_number, std::move(ctx));
        assert(success);
        return iter->second.get();
    }

    void try_issue_sync_close(FileCtx *ctx) {
        if (ctx->write_cnt == 0) {
            auto &arg = *ctx->sync_close_req;
            auto *io_data = new IoData;
            io_data->op = Op::kSyncClose;
            io_data->ctx = ctx;
            io_data->cb = arg.cb;
            issue_sync_close(ctx->os_fd, arg.use_fsync, io_data);
        }
    }

    void issue_write(int fd, size_t offset, const RingBuffer::View &src, IoData *io_data) {
        io_uring_sqe *sqe = io_uring_get_sqe(m_uring);
        if (sqe == nullptr) {
            throw_with(fmt::format("Failed to get sqe: ", strerror(errno)));
        }

        sqe->user_data = reinterpret_cast<uint64_t>(io_data);
        io_data->iovs[0].iov_base = src.first_part().data();
        io_data->iovs[0].iov_len = src.first_part().size();
        if (src.second_part().has_value()) {
            io_data->iovs[1].iov_base = src.second_part()->data();
            io_data->iovs[1].iov_len = src.second_part()->size();
            io_uring_prep_writev(sqe, fd, io_data->iovs, 2, offset);
        } else {
            io_uring_prep_writev(sqe, fd, io_data->iovs, 1, offset);
        }
        
        if (auto ret = io_uring_submit(m_uring); ret < 0) {
            throw_with(fmt::format("Failed to submit uring: ", strerror(errno)));
        }
    }

    void issue_sync_close(int fd, bool use_fsync, IoData *io_data) {
        io_uring_sqe *sqe1 = io_uring_get_sqe(m_uring);
        if (sqe1 == nullptr) {
            throw_with(fmt::format("Failed to get sqe1: ", strerror(errno)));
        }
        io_uring_sqe_set_flags(sqe1, IOSQE_IO_LINK);
        if (use_fsync) {
            io_uring_prep_fsync(sqe1, fd, 0);
        } else {
            io_uring_prep_fsync(sqe1, fd, IORING_FSYNC_DATASYNC);
        }

        io_uring_sqe *sqe2 = io_uring_get_sqe(m_uring);
        if (sqe2 == nullptr) {
            throw_with(fmt::format("Failed to get sqe2: ", strerror(errno)));
        }
        sqe2->user_data = reinterpret_cast<uint64_t>(io_data);
        io_uring_prep_close(sqe2, fd);

        if (auto ret = io_uring_submit(m_uring); ret < 0) {
            throw_with(fmt::format("Failed to submit uring: ", strerror(errno)));
        }
    }

    void poll() {
        io_uring_cqe *cqe{};
        while (not m_is_killed) {
            if (auto ret = io_uring_wait_cqe(m_uring, &cqe); ret < 0) {
                throw_with(fmt::format("Wait cqe: {}", strerror(errno)));
            }

            std::unique_ptr<IoData> io_data;
            io_data.reset((IoData *)cqe->user_data);
            io_uring_cqe_seen(m_uring, cqe);

            if (io_data->cb != nullptr) {
                io_data->cb->operator()(cqe->res);
            }

            auto &ctx = io_data->ctx;
            if (io_data->op == Op::kWrite) {
                --ctx->write_cnt;
                if (ctx->write_cnt == 0) {
                    try_issue_sync_close(ctx);
                }
            } else if (io_data->op == Op::kSyncClose) {
                m_writing_files.erase(ctx->file_number);
            } else {
                throw_with("unexpected op type");
            }

        }
    }

    std::thread m_poller;
    std::atomic<bool> m_is_killed{};

    io_uring *m_uring{};
    std::unique_ptr<SstPool> m_sst_pool;
    std::map<size_t, std::unique_ptr<FileCtx>> m_writing_files;
};

} // namespace pulsedb
