#pragma once

#include "ring_buffer.h"
#include "callonce_fn.hpp"
#include <functional>

namespace pulsedb {
class IoCtx {
public:
    enum Op {
        kWrite,
        kSyncClose
    };

    using CallbackFn = CallonceFn<std::function<void(int res)>>;
    virtual ~IoCtx() = default;

    virtual void write_async(size_t file_number, size_t offset, size_t base_size_multiplier, const RingBuffer::View &src,
        CallbackFn *cb = nullptr) = 0;
    virtual void sync_close_async(size_t file_number, bool use_fsync, CallbackFn *cb = nullptr) = 0;
};

} // namespace pulsedb
