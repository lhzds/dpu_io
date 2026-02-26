#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <memory>

#include "util.h"

namespace pulsedb {

class AlignedBuffer {
public:
    AlignedBuffer() = default;
    AlignedBuffer(size_t size, size_t alignment = 1) {
        reset(size, alignment);
    }
    AlignedBuffer(AlignedBuffer &&) noexcept = default;
    AlignedBuffer &operator=(AlignedBuffer &&) noexcept = default;

    AlignedBuffer(const AlignedBuffer &other) {
        copy_from(other);
    }
    AlignedBuffer &operator=(const AlignedBuffer &other) {
        if (this == &other) {
            return *this;
        }
        copy_from(other);
        return *this;
    }

    void reset(size_t size, size_t alignment = 1) {
        validate_alignment(alignment);
        m_size = size;
        m_alignment = alignment;
        if (m_size == 0) {
            m_data.reset();
            return;
        }

        m_data = alloc_aligned(m_size, m_alignment);
        std::memset(m_data.get(), 0, m_size);
    }

    void fill_zero() {
        if (m_data) {
            std::memset(m_data.get(), 0, m_size);
        }
    }

    const char *data() const noexcept {
        return m_data.get();
    }

    char *data() noexcept {
        return m_data.get();
    }

    size_t size() const noexcept {
        return m_size;
    }

    size_t alignment() const noexcept {
        return m_alignment;
    }

private:
    using Storage = std::unique_ptr<char, decltype(&std::free)>;

    static void validate_alignment(size_t alignment) {
        if (alignment == 0 || !std::has_single_bit(alignment)) {
            throw_with("AlignedBuffer: Invalid alignment={}", alignment);
        }
    }

    static Storage alloc_aligned(size_t size, size_t alignment) {
        if (alignment <= alignof(std::max_align_t)) {
            auto *ptr = static_cast<char *>(std::malloc(size));
            if (ptr == nullptr) {
                throw_with("AlignedBuffer: Failed to alloc buffer[size={}]", size);
            }
            return Storage(ptr, &std::free);
        }
        void *ptr{};
        if (auto ret = posix_memalign(&ptr, alignment, size); ret != 0) {
            throw_with("AlignedBuffer: Failed to alloc aligned buffer[alignment={}, size={}]",
                       alignment, size);
        }
        return Storage(static_cast<char *>(ptr), &std::free);
    }

    void copy_from(const AlignedBuffer &other) {
        m_size = other.m_size;
        m_alignment = other.m_alignment;
        if (m_size == 0) {
            m_data.reset();
            return;
        }
        m_data = alloc_aligned(m_size, m_alignment);
        std::memcpy(m_data.get(), other.data(), m_size);
    }

    size_t m_size{};
    size_t m_alignment{1};
    Storage m_data{nullptr, &std::free};
};

} // namespace pulsedb
