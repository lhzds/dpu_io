#pragma once

#include "noncopyable.h"
#include <array>
#include <stdexcept>
#include <string_view>
#include <vector>

#pragma once

#include <array>
#include <stdexcept>
#include <string_view>
#include <vector>

class ConstBytesView {
public:
    ConstBytesView(const std::string &str): m_data(str.data()), m_size(str.size()) {}
    ConstBytesView(std::string_view sv): m_data(sv.data()), m_size(sv.size()) {}
    ConstBytesView(char const *data, size_t size): m_data(data), m_size(size) {}

    const char *data() const noexcept { return m_data; }
    size_t size() const noexcept { return m_size; }

    const char *begin() const noexcept { return data(); }
    const char *end() const noexcept { return data() + size(); }

    ConstBytesView subspan(size_t start,
                             size_t len = static_cast<size_t>(-1)) const {
        if (start > size()) {
            throw std::out_of_range("bytes_const_view::subspan");
        }
        if (len > size() - start) {
            len = size() - start;
        }
        return {data() + start, len};
    }

    operator std::string_view() const noexcept {
        return std::string_view{data(), size()};
    }

private:
    const char *m_data{};
    size_t m_size{};
};

class BytesView {
public:
    BytesView(std::string &str): m_data(str.data()), m_size(str.size()) {}
    BytesView(char *data, size_t size): m_data(data), m_size(size) {}

    char *data() const noexcept { return m_data; }
    size_t size() const noexcept { return m_size; }

    char *begin() const noexcept { return data(); }
    char *end() const noexcept { return data() + size(); }

    BytesView subspan(size_t start, size_t len) const {
        if (start > size()) {
            throw std::out_of_range("bytes_view::subspan");
        }
        if (len > size() - start) {
            len = size() - start;
        }
        return {data() + start, len};
    }

    operator ConstBytesView() const noexcept { return ConstBytesView{data(), size()}; }
    operator std::string_view() const noexcept { return std::string_view{data(), size()}; }

private:
    char *m_data{};
    size_t m_size{};
};

class BytesBuffer {
public:
    BytesBuffer() = default;
    BytesBuffer(BytesBuffer &&) = default;
    BytesBuffer &operator=(BytesBuffer &&) = default;
    explicit BytesBuffer(BytesBuffer const &) = default;

    explicit BytesBuffer(size_t n) : m_data(n) {}

    const char *data() const noexcept {
        return m_data.data();
    }

    char *data() noexcept {
        return m_data.data();
    }

    size_t size() const noexcept {
        return m_data.size();
    }

    const char *begin() const noexcept {
        return data();
    }

    char *begin() noexcept {
        return data();
    }

    const char *end() const noexcept {
        return data() + size();
    }

    char *end() noexcept {
        return data() + size();
    }

    ConstBytesView subspan(size_t start, size_t len) const {
        return operator ConstBytesView().subspan(start, len);
    }

    BytesView subspan(size_t start, size_t len) {
        return operator BytesView().subspan(start, len);
    }

    operator ConstBytesView() const noexcept {
        return ConstBytesView{m_data.data(), m_data.size()};
    }

    operator BytesView() noexcept {
        return BytesView{m_data.data(), m_data.size()};
    }

    operator std::string_view() const noexcept {
        return std::string_view{m_data.data(), m_data.size()};
    }

    void append(ConstBytesView chunk) {
        m_data.insert(m_data.end(), chunk.begin(), chunk.end());
    }

    void append(std::string_view chunk) {
        m_data.insert(m_data.end(), chunk.begin(), chunk.end());
    }

    void append(const char *start, size_t len) {
        m_data.insert(m_data.end(), start, start + len);
    }

    template <size_t N>
    void append_literial(char const (&literial)[N]) {
        append(std::string_view{literial, N - 1});
    }

    void clear() {
        m_data.clear();
    }

    void resize(size_t n) {
        m_data.resize(n);
    }

    void reserve(size_t n) {
        m_data.reserve(n);
    }

private:
    std::vector<char> m_data;
};

template <size_t N>
class StaticBytesBuffer {
public:
    char const *data() const noexcept {
        return m_data.data();
    }

    char *data() noexcept {
        return m_data.data();
    }

    static constexpr size_t size() noexcept {
        return N;
    }

    operator ConstBytesView() const noexcept {
        return ConstBytesView{m_data.data(), N};
    }

    operator BytesView() noexcept {
        return BytesView{m_data.data(), N};
    }

    operator std::string_view() const noexcept {
        return std::string_view{m_data.data(), m_data.size()};
    }

private:
    std::array<char, N> m_data;
};

// 尽可能 in-place 读取数据，减少拷贝
class BytesStream {
public:
    BytesStream(ConstBytesView view) {
        m_views.emplace_back(view);
        m_now_bytes = view.size();
    }
    BytesStream(const std::vector<ConstBytesView> views) {
        m_views = views;
        if (not views.empty()) {
            m_now_bytes = views.size();
        }
    };

    ConstBytesView read(size_t n) {
        if (empty()) {
            throw std::runtime_error("run out of all bytes");
        }
        // 如果要读取的数据在内存上连续
        if (n < m_now_bytes) {
            size_t used_bytes = m_views[m_now].size() - m_now_bytes;
            ConstBytesView ret(m_views[m_now].data() + used_bytes, m_now_bytes);
            m_now_bytes -= n;

            if (m_now_bytes == 0) {
                next_view();
            }

            return ret;
        }

        // 如果不连续，则将数据进行组装
        m_cached.clear();
        while (not empty() || n) {
            size_t read_size = std::min(m_now_bytes, n);
            size_t used_bytes = m_views[m_now].size() - m_now_bytes;
            m_cached.append(m_views[m_now].data() + used_bytes, read_size);
            
            m_now_bytes -= read_size;
            n -= read_size;
            next_view();
        }

        return m_cached;
    } 

    bool empty() {
        return m_now == m_views.size();
    }

private:
    void next_view() {
        ++m_now;
        if (not empty()) {
            m_now_bytes = m_views[m_now].size();
            return;
        }
        m_now_bytes = 0;
    }

    size_t m_now{};
    size_t m_now_bytes{};
    BytesBuffer m_cached;
    std::vector<ConstBytesView> m_views;
};
