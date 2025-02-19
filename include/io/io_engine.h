#pragma once

#include "ring_buffer.h"
#include "callonce_fn.hpp"
#include <functional>

namespace pulsedb {

class IoEngine {
public:
    enum Op {
        kWrite,
        kSyncClose
    };

    using CallbackFn = CallonceFn<std::function<void(size_t file_number, int res)>>;
    virtual ~IoEngine() = default;

    virtual void write_async(size_t file_number, size_t offset, int base_size_multiplier, const RingBuffer::View &src,
        CallbackFn *cb = nullptr) = 0;
    virtual void sync_close_async(size_t file_number, bool use_fsync, CallbackFn *cb = nullptr) = 0;
    virtual bool query() = 0;
};

} // namespace pulsedb
