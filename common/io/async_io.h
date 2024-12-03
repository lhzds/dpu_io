#pragma once
#include <functional>
#include <string_view>

#include "bytes.h"

class AsyncIoCtx {
public:
    using OpCompleteCb = std::function<void()>;

    void open_async() {
        
    }

    void close_async(OpCompleteCb cb) {
        
    }

    void write_async(const BytesView &dst, size_t offset, OpCompleteCb cb) {

    }

    void read_async(const ConstBytesView &src, size_t offset, OpCompleteCb cb) {
        
    }

    void fsync_async(OpCompleteCb cb) {
        
    }
};