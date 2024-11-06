#include <cstdlib>
#include <fmt/format.h>
#include <functional>
#include <iostream>
#include <optional>
#include <spdk/bdev.h>
#include <spdk/blob.h>
#include <spdk/blobfs.h>
#include <spdk/blob_bdev.h>
#include <spdk/event.h>
#include <stdexcept>
#include <thread>
#include <threads.h>

#include "bytes.h"
#include "noncopyable.h"
#include "spdk/thread.h"

template<class BlobFsSpdkCtx>
class BlobFsTemplate: public Noncopyable {
public:
    using Handle = spdk_file *;
    using OpCompleteCb = std::function<void(int)>;
    using OpCompleteWithHandleCb = std::function<void(Handle, int)>;

    BlobFsTemplate(std::string bdev_name, const std::string_view &fs_name,
        const std::string_view &json_config_file): m_bdev_name(std::move(bdev_name)) {

        spdk_app_opts_init(&m_opts, sizeof(m_opts));
        m_opts.name = fs_name.data();
        m_opts.json_config_file = json_config_file.data();
        m_opts.tpoint_group_mask = "0x80";
        m_opts.shutdown_cb = shutdown_cb;
        // m_opts.print_level = SPDK_LOG_DEBUG;

        m_worker = std::thread([&] {
            auto ret = spdk_app_start(&m_opts, init_fs, this);
            if (ret) {
                m_is_error = true;
                return;
            }

            spdk_app_fini();
        });

        while (not m_is_ready && not m_is_error);
        if (m_is_error) {
            throw std::runtime_error("init fs error");
        }
    }

    ~BlobFsTemplate() {
        if (m_spdk_ctx.fs) {
            spdk_fs_iter iter = spdk_fs_iter_first(m_spdk_ctx.fs);
            while (iter) {
                Handle file = spdk_fs_iter_get_file(iter);
                close(file);
                iter = spdk_fs_iter_next(iter);
            }
        }

        spdk_app_start_shutdown();
        m_worker.join();
    }

    static void set_cache_size(size_t mb) {
        spdk_fs_set_cache_size(mb);
    }

    static size_t get_cache_size_mb() {
        return spdk_fs_get_cache_size();
    }

    std::optional<Handle> open(const std::string_view &name, uint32_t flags) {
        Handle file{};
        auto ret = spdk_fs_open_file(m_spdk_ctx.fs, get_raw_thread_ctx(),
            name.data(), flags, &file);
        if (ret) {
            return {};
        }
        return file;
    }

    int close(Handle file) {
        return spdk_file_close(file, get_raw_thread_ctx());
    }

    int write(Handle file, const BytesView &dst, size_t offset) {
        return spdk_file_write(file, get_raw_thread_ctx(), dst.data(),
            offset, dst.size());
    }

    int read(Handle file, const ConstBytesView &src, size_t offset) {
        return spdk_file_read(file, get_raw_thread_ctx(), const_cast<char *>(src.data()),
            offset, src.size());
    }

    int fsync(Handle file) {
        return spdk_file_sync(file, get_raw_thread_ctx());
    }

    int remove(const std::string_view &filename) {
        return spdk_fs_delete_file(m_spdk_ctx.fs, get_raw_thread_ctx(), filename.data());
    }

    void open_async(const std::string_view &name, uint32_t flags, OpCompleteWithHandleCb cb) {
        auto *heap_cb = new OpCompleteWithHandleCb(std::move(cb));
        spdk_fs_open_file_async(m_spdk_ctx.fs, name.data(), flags, call_op_complete_with_handle_cb, heap_cb);
    }

    void close_async(Handle file, OpCompleteCb cb) {
        auto *heap_cb = new OpCompleteCb(std::move(cb));
        spdk_file_close_async(file, call_op_complete_cb, heap_cb);
    }

    void write_async(Handle file, const BytesView &dst, size_t offset, OpCompleteCb cb) {
        auto *heap_cb = new OpCompleteCb(std::move(cb));
        spdk_file_write_async(file, get_raw_io_channel(), dst.data(), offset,
            dst.size(), call_op_complete_cb, heap_cb);
    }

    void read_async(Handle file, const ConstBytesView &src, size_t offset, OpCompleteCb cb) {
        auto *heap_cb = new OpCompleteCb(std::move(cb));
        spdk_file_read_async(file, get_raw_io_channel(), const_cast<char *>(src.data()), offset,
            src.size(), call_op_complete_cb, heap_cb);
    }

    void fsync_async(Handle file, OpCompleteCb cb) {
        auto *heap_cb = new OpCompleteCb(std::move(cb));
        spdk_file_sync_async(file, get_raw_io_channel(), call_op_complete_cb, heap_cb);
    }

private:
    // for sync op
    class ThreadCtx: public Noncopyable {
    public:
        explicit ThreadCtx(spdk_filesystem *fs) {
            m_inner = spdk_fs_alloc_thread_ctx(fs);
        }
        ~ThreadCtx() {
            if (m_inner) {
                spdk_fs_free_thread_ctx(m_inner);
            }
        }

        spdk_fs_thread_ctx *raw() const noexcept { return m_inner; }

    private:
        spdk_fs_thread_ctx *m_inner{};
    };

    // for async op
    class IoChannel: public Noncopyable {
    public:
        explicit IoChannel(spdk_filesystem *fs) {
            m_inner = spdk_fs_alloc_io_channel(fs);
        }
        ~IoChannel() {
            if (m_inner) {
                spdk_fs_free_io_channel(m_inner);
            }
        }

        spdk_io_channel *raw() const noexcept { return m_inner; }

    private:
        spdk_io_channel *m_inner{};
    };

    static void base_bdev_event_cb(spdk_bdev_event_type type, spdk_bdev *, void *) {
        std::cerr << "Unsupported bdev event" << type << std::endl;;
    }

    static void send_request_fn(fs_request_fn fn, void *arg) {
        spdk_thread_send_msg(m_spdk_ctx.app_thread, fn, arg);
    }

    static void load_fs_cb(void *self, spdk_filesystem *fs, int fserrno) {
        auto *blobfs = (BlobFsTemplate *)(self);
        if (fserrno == 0) {
            m_spdk_ctx.fs = fs;
        }
        blobfs->m_is_ready = true;
    }

    static void init_fs(void *self) {
        auto *blobfs = (BlobFsTemplate *)(self);
        auto ret = spdk_bdev_create_bs_dev_ext(blobfs->m_bdev_name.data(),
            base_bdev_event_cb, nullptr, &blobfs->m_bs_dev);
        if (ret != 0) {
            std::cerr << "Could not create blob bdev" << std::endl;
            spdk_app_stop(0);
            exit(EXIT_FAILURE);
        }

        m_spdk_ctx.app_thread = spdk_get_thread();
        spdk_fs_load(blobfs->m_bs_dev, send_request_fn, load_fs_cb, self);
    }

    static void shutdown_cb() {
        if (m_spdk_ctx.fs) {
            spdk_fs_unload(m_spdk_ctx.fs, unload_fs_cb, nullptr);
        } else {
            unload_fs_cb(nullptr, 0);
        }
    }

    static void unload_fs_cb(void *, int fserrno) {
        assert(fserrno == 0);
        spdk_app_stop(0);
    }

    static void call_op_complete_cb(void *raw_cb, int fserrno) {
        auto *cb = (OpCompleteCb *)(raw_cb);
        cb->operator()(fserrno);
        delete cb;
    }

    static void call_op_complete_with_handle_cb(void *raw_cb, Handle f, int fserrno) {
        auto *cb = (OpCompleteWithHandleCb *)(raw_cb);
        cb->operator()(f, fserrno);
        delete cb;
    }

    spdk_io_channel *get_raw_io_channel() {
        thread_local static IoChannel io_channel(m_spdk_ctx.fs);
        return io_channel.raw(); 
    }

    spdk_fs_thread_ctx *get_raw_thread_ctx() {
        thread_local static ThreadCtx thread_ctx(m_spdk_ctx.fs);
        return thread_ctx.raw();
    }

    spdk_app_opts m_opts{};
    std::string m_bdev_name{};
    spdk_bs_dev *m_bs_dev{};

    std::thread m_worker{};
    static BlobFsSpdkCtx m_spdk_ctx;

    std::atomic<bool> m_is_ready{};
    std::atomic<bool> m_is_error{};
};

template<class BlobFsSpdkCtx>
BlobFsSpdkCtx BlobFsTemplate<BlobFsSpdkCtx>::m_spdk_ctx = {};
