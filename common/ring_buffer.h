#pragma once

#include "bytes.h"
#include <bit>
#include <cstring>
#include <functional>

class RingBufferRep {
public:
    RingBufferRep() {}
    RingBufferRep(size_t cap) {
        cap = std::bit_ceil(cap);
        m_cap = cap;
        m_mask = m_cap - 1;
    }

    void write(size_t n) {
        m_tail = (m_tail + n) & m_mask;
        m_size += n;
    }

    void read(size_t n) {
        m_head  = (m_head + n) & m_mask;
        m_size -= n;
    }

    size_t cap() const noexcept {
        return m_cap;
    }

    size_t size() const noexcept {
        return m_size;
    }

    size_t free_size() const noexcept {
        return m_cap - m_size;
    }

    size_t empty() const noexcept {
        return m_size == 0;
    }

    size_t head() const noexcept {
        return m_head;
    }

    size_t tail() const noexcept {
        return m_tail;
    }

    void set_head(size_t head) noexcept {
        m_head = head;
        m_size = (m_tail - m_head) % m_cap;
    }

    void set_tail(size_t tail) noexcept {
        m_tail = tail;
        m_size = (m_tail - m_head) % m_cap;
    }

private:
    size_t m_head{};
    size_t m_tail{};
    size_t m_size{};
    size_t m_cap{};
    size_t m_mask{};
};

class RingBuffer {
public:
    using ReadCallback = std::function<void(const std::vector<ConstBytesView> &)>;

    RingBuffer() = default;
    RingBuffer(RingBuffer &&) = default;
    RingBuffer &operator=(RingBuffer &&) = default;

    explicit RingBuffer(const RingBuffer &) = default;

    // cap 对齐到 2 的 N 次方
    RingBuffer(size_t cap): m_rep(cap) {
        m_buf.resize(m_rep.cap());
    }

    bool write(const ConstBytesView &src) {
        if (free_size() < src.size()) {
            return false;
        }
        write_impl(src);
        return true;
    }

    bool write(const std::vector<ConstBytesView> &src) {
        size_t total_size{};
        for (auto view : src) {
            total_size += view.size();
        }
        if (free_size() < total_size) {
            return false;
        }
        for (auto view : src) {
            write_impl(view);
        }
        return true;
    }

    // 最多读取 dst.size() 字节的数据
    size_t read(const BytesView &dst) {
        if (empty()) {
            return 0;
        }
        size_t read_size = std::min(dst.size(), size());
        if (size_t first_size = cap() - head(); first_size < read_size) {
            std::memcpy(dst.data(), data() + head(), first_size);
            size_t second_size = read_size - first_size;
            std::memcpy(dst.data() + first_size, data(), second_size);
        } else {
            std::memcpy(dst.data(), data() + head(), read_size);
        }
        
        m_rep.read(read_size);
        return read_size;
    }

    size_t read(size_t n, const ReadCallback &callback) {
        if (empty()) {
            return 0;
        }

        std::vector<ConstBytesView> gather_list;
        size_t read_size = std::min(n, size());
        if (size_t first_size = cap() - head(); first_size < read_size) {
            gather_list.emplace_back(data() + head(), first_size);
            size_t second_size = read_size - first_size;
            gather_list.emplace_back(data(), second_size);
        } else {
            gather_list.emplace_back(data() + head(), read_size);
        }
        
        if (callback) {
            callback(gather_list);
        } 

        m_rep.read(read_size);
        return read_size;
    }

    size_t cap() const noexcept {
        return m_rep.cap();
    }

    size_t size() const noexcept {
        return m_rep.size();
    }

    size_t free_size() const noexcept {
        return m_rep.size();
    }

    size_t empty() const noexcept {
        return m_rep.empty();
    }

    const char *data() const noexcept {
        return m_buf.data();
    }

    char *data() noexcept {
        return m_buf.data();
    }

    size_t head() const noexcept {
        return m_rep.head();
    }

    size_t tail() const noexcept {
        return m_rep.tail();
    }

    void set_head(size_t head) noexcept {
        m_rep.set_head(head);
    }

    void set_tail(size_t tail) noexcept {
        m_rep.set_tail(tail);
    }

private:
    void write_impl(const ConstBytesView &src) {
        if (size_t first_size = cap() - tail(); first_size < src.size()) {
            std::memcpy(data() + tail(), src.data(), first_size);
            size_t second_size = src.size() - first_size;
            std::memcpy(data(), src.data() + first_size, second_size);
        } else {
            std::memcpy(data() + tail(), src.data(), src.size());
        }

        m_rep.write(src.size());
    }

    RingBufferRep m_rep;
    BytesBuffer m_buf;
};
