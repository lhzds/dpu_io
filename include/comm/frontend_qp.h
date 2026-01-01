#pragma once

#include <memory>

#include "bytes.h"
#include "doca_wrapper/passive_consumer.h"
#include "doca_wrapper/producers.h"
#include "docacxx/mem.h"
#include "format/rb_req_resp_msg.h"
#include "frontend.h"
#include "noncopyable.h"
#include "ring_buffer.h"

namespace pulsedb {
class Frontend;

// 非线程安全
class FrontendQp: Noncopyable {
public:
    std::atomic_bool is_busy{};

    FrontendQp(Frontend &frontend, int id, size_t req_rb_cap, size_t resp_rb_cap);
    ~FrontendQp();

    void create_producer(Frontend &frontend);
    void create_consumer(Frontend &frontend);

    bool is_preping_write() const {
        return m_cur_write_req_size > 0;
    }
    size_t cur_write_size() const {
        return m_cur_write_req_size;
    }
    void prep_write(size_t file_number, size_t offset, int base_size_multiplier = 1);
    void append(const ConstBytesView &content);
    void post_write();
    void post_range_sync(size_t file_number, size_t offset, size_t size, bool wait_before);
    void post_sync_close(size_t file_number, bool use_fsync);

    std::vector<std::pair<int, int>> wait_all();

    int id() const { return m_id; }

    RingBuffer &req_rb_ref() { return m_req_rb; }
    docacxx::Mmap &req_mmap_ref() { return *m_req_mmap; }
    RingBuffer &resp_rb_ref() { return m_resp_rb; }
    docacxx::Mmap &resp_mmap_ref() { return *m_resp_mmap; }

private:
    template <typename WriteReqFunc, typename... Args, typename Callback>
    void post_request(WriteReqFunc write_req_func, Callback callback, Args... args);

    void send_req_rb_new_size(size_t new_size);
    void send_req_rb_needed_size(size_t needed_size);
    void send_wait_stat(size_t wait_size, size_t processed_size);

    void free_req_rb_space(size_t needed_size);
    void wait_new_resps(size_t wait_size, size_t processed_size);

    doca_pe *m_pe{};
    int m_id{-1};

    int m_sync_close_cnt{}; // 当前仍未完成的请求数
    size_t m_cur_write_req_size{};
    std::optional<RingBuffer::View> m_reserved_header_space;
    IoReqMsg::WriteHeader m_cache_header;

    RingBuffer m_req_rb;
    RingBuffer m_resp_rb;
    std::unique_ptr<docacxx::Mmap> m_req_mmap;
    std::unique_ptr<docacxx::Mmap> m_resp_mmap;
    std::unique_ptr<SyncSingleTaskProducer> m_producer;

    std::optional<size_t> m_req_rb_cleaned_size{};
    std::optional<size_t> m_resp_rb_new_size{};
    std::unique_ptr<PassiveConsumer> m_consumer;
};
}
