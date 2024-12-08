#pragma once

#include <any>
#include <cstdint>
#include <functional>
#include <memory>

#include "bytes.h"
#include "doca_wrapper/passive_consumer.h"
#include "docacxx/ctx/consumer.h"
#include "docacxx/mem.h"
#include "doca_wrapper/single_task_producer.h"
#include "frontend.h"
#include "noncopyable.h"
#include "ring_buffer.h"

namespace pulsedb {
class Frontend;

class FrontendQp: Noncopyable {
public:
    using OpenCallback = std::function<void(int32_t error_code, size_t file_id)>;
    using CloseCallback = std::function<void(int32_t error_code)>;
    using ReadCallback = std::function<void(int32_t error_code, std::optional<RingBuffer::View> content)>;
    using WriteCallback = std::function<void(int32_t error_code)>;
    using FsyncCallback = std::function<void(int32_t error_code)>;

    FrontendQp(Frontend &frontend, size_t id, size_t req_rb_cap, size_t resp_rb_cap);
    ~FrontendQp();

    void create_producer(Frontend &frontend);
    void create_consumer(Frontend &frontend);

    void post_open(const std::string_view &filename, uint32_t open_flags, OpenCallback callback);
    void post_close(size_t file_id, CloseCallback callback);
    void post_read(size_t file_id, size_t offset, size_t size, ReadCallback callback);
    void post_write(size_t file_id, size_t offset, const ConstBytesView &content, WriteCallback callback);
    void post_fsync(size_t file_id, FsyncCallback callback);

    void poll_wait();
    void poll_wait(size_t n);

    RingBuffer &req_rb_ref() { return m_req_rb; }
    docacxx::Mmap &req_mmap_ref() { return *m_req_mmap; }
    RingBuffer &resp_rb_ref() { return m_resp_rb; }
    docacxx::Mmap &resp_mmap_ref() { return *m_resp_mmap; }

private:
    template <typename WriteReqFunc, typename... Args, typename Callback>
    void post_request(WriteReqFunc write_req_func, Callback callback, Args... args);

    void send_host_req_rb_tail();
    void send_host_resp_rb_head();
    void update_host_req_rb_head();
    void update_host_resp_rb_tail();

    void recv_d2h_rb_meta_msg(const ConstBytesView &raw_msg);
    size_t find_free_req_slot();

    doca_pe *m_pe{};

    struct RequestSlot {
        bool m_is_free{true};
        std::any m_callback{};
    };

    size_t m_id{};
    size_t m_pending_req_cnt{}; // 当前仍未完成的请求数
    std::vector<RequestSlot> m_req_slots;

    RingBuffer m_req_rb;
    RingBuffer m_resp_rb;
    std::unique_ptr<docacxx::Mmap> m_req_mmap;
    std::unique_ptr<docacxx::Mmap> m_resp_mmap;
    std::unique_ptr<SingleTaskProducer> m_producer;
    std::unique_ptr<PassiveConsumer> m_consumer;

    bool m_is_d2h_rb_meta_msg_recved{};
    D2HRbMetaMsg m_d2h_rb_meta_msg{};
};
}
