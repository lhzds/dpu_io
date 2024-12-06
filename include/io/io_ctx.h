#pragma once
#include <functional>
#include <string_view>

#include "bytes.h"

namespace pulsedb {
class IoCtx {
public:
    using OpenCb = std::function<void(int32_t error_code, size_t file_id)>;
    using ReadCb = std::function<void(int32_t error_code, size_t read_size)>;
    using WriteCb = std::function<void(int32_t error_code)>;
    using FsyncCb = std::function<void(int32_t error_code)>;
    using CloseCb = std::function<void(int32_t error_code)>;

    virtual ~IoCtx() = default;

    virtual void open_async(const std::string_view &filename, uint32_t open_flags, OpenCb cb) {
        throw_with("unimplemented"); 
    }

    virtual void read_async(size_t file_id, size_t offset, std::vector<BytesView> iov, ReadCb cb) {
        throw_with("unimplemented");      
    }

    virtual void write_async(size_t file_id, size_t offset, std::vector<ConstBytesView> iov, WriteCb cb) {
        throw_with("unimplemented");      
    }

    virtual void fsync_async(size_t file_id, FsyncCb cb) {
        throw_with("unimplemented");
    }

    virtual void close_async(size_t file_id, CloseCb cb) {
        throw_with("unimplemented");      
    }
};
} // namespace pulsedb