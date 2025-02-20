#pragma once

#include "bytes.h"
#include "types.h"

#include <bit>
#include <cassert>
#include <cstring>
#include <functional>
#include <optional>

namespace pulsedb {
class RingBufferRep {
public:
    class Slice {
    public:
        explicit Slice(Interval first_part): m_first_part(first_part) {}
        Slice(Interval first_part, Interval second_part)
            : m_first_part(first_part), m_second_part(second_part) {}

        const Interval &first_part() const { return m_first_part; }
        const std::optional<Interval> &second_part() const { return m_second_part; }

        size_t size() const {
            auto ret = m_first_part.size;
            if (m_second_part.has_value()) {
                ret += m_second_part->size;
            }
            return ret;
        }

        Slice cut(size_t n) {
            assert(n <= size());

            if (n <= m_first_part.size) {
                return Slice(m_first_part.shrink_forward(n));
            }
            auto second_part_cut_size = n - m_first_part.size;
            Slice ret(m_first_part, m_second_part->shrink_forward(second_part_cut_size));
            m_first_part = *m_second_part;
            m_second_part = std::nullopt;
            return ret;
        }

    private:
        Interval m_first_part;
        std::optional<Interval> m_second_part{};
    };

    RingBufferRep() {}
    RingBufferRep(size_t cap) {
        m_cap = std::bit_ceil(cap);
        m_mask = m_cap - 1;
    }

    std::optional<Slice> write(size_t n) {
        if (free_size() < n) {
            return {};
        }

        std::optional<Slice> ret;
        if (size_t first_size = cap() - tail(); first_size < n) {
            size_t second_size = n - first_size;
            ret = Slice({tail(), first_size}, {0, second_size});
        } else {
            ret = Slice({tail(), n});
        }

        m_tail = (m_tail + n) & m_mask;
        m_size += n;
        return ret;
    }

    // 最多读取 n 字节
    Slice read(size_t n) {
        size_t read_size = std::min(n, size());

        std::optional<Slice> ret;
        if (size_t first_size = cap() - head(); first_size < read_size) {
            size_t second_size = read_size - first_size;
            ret = Slice({head(), first_size}, {0, second_size});
        } else {
            ret = Slice({head(), read_size});
        }

        m_head  = (m_head + n) & m_mask;
        m_size -= n;
        return *ret;
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

    Slice to_slice() const noexcept {
        if (m_head <= m_tail) {
            return Slice({m_head, m_size});
        }
        return Slice({m_head, m_cap - m_head}, {0, m_tail});
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
    class View {
    public:
        explicit View(BytesView first_part): m_first_part(first_part) {}
        View(BytesView first_part, BytesView second_part)
            : m_first_part(first_part), m_second_part(second_part) {}

        const BytesView &first_part() const { return m_first_part; }
        const std::optional<BytesView> &second_part() const { return m_second_part; }
        
        size_t size() const {
            auto ret = m_first_part.size();
            if (m_second_part.has_value()) {
                ret += m_second_part->size();
            }
            return ret;
        }

        BytesBuffer merge() const {
            BytesBuffer buf;
            buf.append(ConstBytesView(m_first_part));
            if (m_second_part) {
                buf.append(ConstBytesView(*m_second_part));
            }
            return buf;
        }

        void merge(const BytesView &dst) const {
            assert(dst.size() >= size());
            std::memcpy(dst.data(), m_first_part.data(), m_first_part.size());
            if (m_second_part) {
                std::memcpy(dst.data() + m_first_part.size(), m_second_part->data(), m_second_part->size());
            }
        }

        View cut(size_t n) {
            assert(n <= size());

            if (n <= m_first_part.size()) {
                auto ret = m_first_part.subspan(0, n);
                m_first_part = m_first_part.subspan(n, m_first_part.size());
                return View(ret);
            }

            auto second_part_cut_size = n - m_first_part.size();
            auto second_part_cut = m_second_part->subspan(0, second_part_cut_size);

            View ret(m_first_part, second_part_cut);
            m_first_part = m_second_part->subspan(second_part_cut_size, m_second_part->size());
            m_second_part = std::nullopt;
            return ret;
        }

        // 可以搭配 reserve_space() 使用
        bool write(const ConstBytesView &src) {
            if (size() < src.size()) {
                return false;
            }

            if (m_first_part.size() <= src.size()) {
                std::memcpy(m_first_part.data(), src.data(), src.size());
            } else {
                std::memcpy(m_first_part.data(), src.data(), m_first_part.size());
                auto residual = src.subspan(m_first_part.size());
                std::memcpy(m_second_part->data(), residual.data(), residual.size());
            }
            return true;
        }

    private:
        BytesView m_first_part;
        std::optional<BytesView> m_second_part{};
    };

    class ConstView {
    public:
        explicit ConstView(ConstBytesView first_part): m_first_part(first_part) {}
        ConstView(ConstBytesView first_part, ConstBytesView second_part)
            : m_first_part(first_part), m_second_part(second_part) {}

        const ConstBytesView first_part() const { return m_first_part; }
        const std::optional<ConstBytesView> &second_part() const { return m_second_part; }

        size_t size() const {
            auto ret = m_first_part.size();
            if (m_second_part.has_value()) {
                ret += m_second_part->size();
            }
            return ret;
        }

        BytesBuffer merge() const {
            BytesBuffer buf;
            buf.append(ConstBytesView(m_first_part));
            if (m_second_part) {
                buf.append(ConstBytesView(*m_second_part));
            }
            return buf;
        }

        void merge(const BytesView &dst) const {
            assert(dst.size() >= size());
            std::memcpy(dst.data(), m_first_part.data(), m_first_part.size());
            if (m_second_part) {
                std::memcpy(dst.data() + m_first_part.size(), m_second_part->data(), m_second_part->size());
            }
        }

        ConstView cut(size_t n) {
            assert(n <= size());

            if (n <= m_first_part.size()) {
                auto ret = m_first_part.subspan(0, n);
                m_first_part = m_first_part.subspan(n, m_first_part.size());
                return ConstView(ret);
            }

            auto second_part_cut_size = n - m_first_part.size();
            auto second_part_cut = m_second_part->subspan(0, second_part_cut_size);

            ConstView ret(m_first_part, second_part_cut);
            m_first_part = m_second_part->subspan(second_part_cut_size, m_second_part->size());
            m_second_part = std::nullopt;
            return ret;
        }

    private:
        ConstBytesView m_first_part;
        std::optional<ConstBytesView> m_second_part{};
    };

    using ReadCallback = std::function<void(const ConstView&)>;

    RingBuffer() = default;
    RingBuffer(RingBuffer &&) = default;
    RingBuffer &operator=(RingBuffer &&) = default;

    explicit RingBuffer(const RingBuffer &) = default;

    // cap 对齐到 2 的 N 次方
    RingBuffer(size_t cap): m_rep(cap) {
        m_buf.resize(m_rep.cap());
    }

    bool write(const ConstBytesView &src) {
        auto slice = m_rep.write(src.size());
        if (not slice) {
            return false;
        }
        write_impl(src, *slice);
        return true;
    }

    bool write(const std::vector<ConstBytesView> &srcs) {
        size_t total_size{};
        for (auto view : srcs) {
            total_size += view.size();
        }
        if (free_size() < total_size) {
            return false;
        }
        for (auto src : srcs) {
            auto slice = m_rep.write(src.size());
            write_impl(src, *slice);
        }
        return true;
    }

    std::optional<View> reserve_space(size_t n) {
        if (free_size() < n) {
            return {};
        }
        return view(*m_rep.write(n));
    }

    // 最多读取 dst.size() 字节的数据
    size_t read(const BytesView &dst) {
        if (empty() || dst.size() == 0) {
            return 0;
        }

        auto slice = m_rep.read(dst.size());
        auto [first_offset, first_size] = slice.first_part();
        std::memcpy(dst.data(), data() + first_offset, first_size);

        if (slice.second_part().has_value()) {
            auto [second_offset, second_size] = *slice.second_part();
            std::memcpy(dst.data(), data() + second_offset, second_size);
        }
        
        return slice.size();
    }

    // 最多读取 n 字节的数据
    size_t read(size_t n, ReadCallback callback = nullptr) {
        if (empty()) {
            return 0;
        }

        auto slice = m_rep.read(n);
        if (callback) {
            callback(const_view(slice));
        } 

        return slice.size();
    }

    size_t cap() const noexcept {
        return m_rep.cap();
    }

    size_t size() const noexcept {
        return m_rep.size();
    }

    size_t free_size() const noexcept {
        return m_rep.free_size();
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

    const RingBufferRep &rep() const {
        return m_rep;
    }

    RingBufferRep &rep() {
        return m_rep;
    }

    View to_view() {
        return view(m_rep.to_slice());
    }

    ConstView to_const_view() const {
        return const_view(m_rep.to_slice());
    }

    View view(const RingBufferRep::Slice &slice) {
        auto [first_offset, first_size] = slice.first_part();
        BytesView first_part = {data() + first_offset, first_size};

        if (slice.second_part().has_value()) {
            auto [second_offset, second_size] = *slice.second_part();
            BytesView second_part = {data() + second_offset, second_size};
            return {first_part, second_part};
        }
        return View(first_part);
    }

    ConstView const_view(const RingBufferRep::Slice &slice) const {
        auto [first_offset, first_size] = slice.first_part();
        ConstBytesView first_part(data() + first_offset, first_size);
        
        if (slice.second_part().has_value()) {
            auto [second_offset, second_size] = *slice.second_part();
            ConstBytesView second_part = {data() + second_offset, second_size};
            return {first_part, second_part};
        }
        return ConstView(first_part);
    }

    RingBufferRep::Slice slice(const View &view) const {
        auto first_addr = view.first_part().data();
        auto first_size = view.first_part().size();
        Interval first_part(first_addr - data(), first_size);

        if (view.second_part().has_value()) {
            auto second_size = view.second_part()->size();
            auto second_addr = view.second_part()->data();
            Interval second_part(second_addr - data(), second_size);
            return {first_part, second_part};
        }
        return RingBufferRep::Slice(first_part);
    }

    template<typename ObjType> ObjType read_obj_copy() {
        ObjType obj;
        read({(char *)&obj, sizeof(obj)});
        return obj;
    }

private:
    void write_impl(const ConstBytesView &src, RingBufferRep::Slice slice) {
        auto [first_offset, first_size] = slice.first_part();
        std::memcpy(data() + first_offset, src.data(), first_size);

        if (slice.second_part().has_value()) {
            auto [second_offset, second_size] = *slice.second_part();
            std::memcpy(data(), src.data() + first_size, second_size);
        }
    }

    RingBufferRep m_rep;
    BytesBuffer m_buf;
};

} // namespace pulsedb
