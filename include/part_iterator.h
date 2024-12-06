#pragma once

#include <cstddef>
#include <stdexcept>
#include <utility>

class PartIterator {
public:
    PartIterator(size_t total_size, size_t part_size)
        :m_total_size{total_size}, m_part_size{part_size}, m_next_part{} {}
    
    std::pair<size_t, size_t> next() {
        if (not has_next()) {
            throw std::out_of_range("no more part");
        }
        auto offset = m_next_part * m_part_size;
        ++m_next_part;
        auto len = (m_total_size - offset) < m_part_size ? m_total_size - offset : m_part_size;
        return {offset, len};
    }

    bool has_next() const {
        return m_next_part * m_part_size < m_total_size;
    }

private:
    size_t m_total_size{};
    size_t m_part_size{};
    size_t m_next_part{};
};
