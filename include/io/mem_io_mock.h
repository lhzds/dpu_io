#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <thread>

#include "bytes.h"
#include "io_ctx.h"

namespace pulsedb {
class MemIoMock: public IoCtx {
public:
    MemIoMock() {
        m_worker = std::thread([&] {
            bg_work();
        });
    }
    virtual ~MemIoMock() {
        m_is_killed = true;
        m_cv.notify_one();
        m_worker.join();
    }

    virtual void open_async(const std::string_view &filename, uint32_t open_flags, OpenCb cb) override {
        std::unique_lock lck(m_mutex);
        m_task_queue.emplace([&, name = std::string(filename), cb = std::move(cb)] {
            if (m_meta.count(name) == 0) {
                m_meta[name] = next_file_id();
                m_mem_files.emplace_back();
                m_mem_files.back().is_open = true;
            }
            cb(0, m_meta[name]);
        });
        m_cv.notify_one();
    }

    virtual void write_async(size_t file_id, size_t offset, std::vector<ConstBytesView> iov, WriteCb cb) override {
        std::unique_lock lck(m_mutex);
        if (file_id >= m_next_file_id || not m_mem_files[file_id].is_open) {
            throw_with("file is not open");
        }
        if (offset != m_mem_files[file_id].size()) {
            throw_with("append-only");
        }
        m_task_queue.emplace([&, file_id, cb = std::move(cb), iov = std::move(iov)] {
            for (const auto &v : iov) {
                m_mem_files[file_id].append(ConstBytesView(v));
            }
            cb(0);
        });
        m_cv.notify_one();
    }

    virtual void read_async(size_t file_id, size_t offset, std::vector<BytesView> iov, ReadCb cb) override {
        std::unique_lock lck(m_mutex);
        if (file_id >= m_next_file_id || not m_mem_files[file_id].is_open) {
            throw_with("file is not open");
        }
        if (offset >= m_mem_files[file_id].size()) {
            throw_with("read out-of-range");
        }
        m_task_queue.emplace([&, file_id, offset, cb = std::move(cb), iov = std::move(iov)] {
            ConstBytesView file_view = m_mem_files[file_id].content.subspan(offset);
            size_t total_read_size{};
            for (const auto &v : iov) {
                if (file_view.size() == 0) {
                    break;
                }
                size_t read_size = std::min(v.size(), file_view.size());
                total_read_size += read_size;
                std::memcpy(v.data(), file_view.data(), read_size);
                file_view = file_view.subspan(read_size);
            }
            cb(0, total_read_size);
        });
        m_cv.notify_one();
    }

    virtual void fsync_async(size_t file_id, FsyncCb cb) override {
        std::unique_lock lck(m_mutex);
        if (file_id >= m_next_file_id || not m_mem_files[file_id].is_open) {
            throw_with("file is not open");
        }
        m_task_queue.emplace([cb = std::move(cb)] {
            cb(0);
        });
        m_cv.notify_one();
    }

    virtual void close_async(size_t file_id,CloseCb cb) override {
        std::unique_lock lck(m_mutex);
        if (file_id >= m_next_file_id) {
            throw_with("file doesn't exist");
        }
        m_task_queue.emplace([&, file_id, cb = std::move(cb)] {
            m_mem_files[file_id].is_open = false;
            cb(0);
        });
        m_cv.notify_one();
    }

private:
    size_t next_file_id() {
        return m_next_file_id++;
    }

    void bg_work() {
        std::unique_lock lck(m_mutex);
        while (not m_is_killed) {
            m_cv.wait(lck, [&] { return not m_task_queue.empty() || m_is_killed; });
            while (not m_task_queue.empty()) {
                auto &task = m_task_queue.front();
                task();
                m_task_queue.pop();
            }
        }
    }

    size_t m_next_file_id{};

    std::thread m_worker;
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::atomic<bool> m_is_killed{};
    std::queue<std::function<void()>> m_task_queue;

    struct MemFile {
        bool is_open{};
        BytesBuffer content;
        void append(const ConstBytesView &chunk) {
            content.append(chunk);
        }
        size_t size() {
            return content.size();
        }
    };
    std::map<std::string, size_t> m_meta;
    std::vector<MemFile> m_mem_files;
};
} // namespace pulsedb
