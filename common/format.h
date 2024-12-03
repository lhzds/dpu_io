#pragma once

#include <cassert>
#include <cstddef>
#include <variant>

#include "bytes.h"
#include "ring_buffer.h"
#include "util.h"

// 控制路径消息，不需要环形队列同步传输
// 构造消息时会发生拷贝，读消息时无拷贝
struct CtrlMsg {
    enum class Type {
        INIT,
        INIT_QP,
        EXIT,
        ACK
    };

    struct CommonHeader {
        Type type{};
    };
    struct InitHeader: CommonHeader {
        uint32_t qp_num{};
    };
    struct InitQpHeader: CommonHeader {
        size_t qp_id{};
        
        char *host_req_mem_addr{};
        size_t host_req_mem_size{};
        char *host_resp_mem_addr{};
        size_t host_resp_mem_size{};

        size_t host_req_mmap_desc_size{};
        size_t host_resp_mmap_desc_size{};
    };
    struct ExitHeader: CommonHeader {};
    struct AckHeader: CommonHeader {};

    struct InitMsg {
        InitHeader *header{};
    };
    struct InitQpMsg {
        InitQpHeader *header{};
        const char *host_req_mmap_desc{};
        const char *host_resp_mmap_desc{};
    };
    struct ExitMsg {
        ExitHeader *header{};
    };
    struct AckMsg {
        AckHeader *header{};
    };
    using Variant = std::variant<InitMsg, InitQpMsg, ExitMsg, AckMsg>;

    static BytesBuffer make_init_msg(size_t qp_num) {
        BytesBuffer msg(sizeof(InitHeader));
        auto *header = (InitHeader *)msg.data();
        header->type = Type::INIT;
        header->qp_num = qp_num;
        return msg;
    }

    static BytesBuffer make_init_qp_msg(size_t qp_id,
        char *host_req_mem_addr, size_t host_req_mem_size,
        char *host_resp_mem_addr, size_t host_resp_mem_size,
        const ConstBytesView &host_req_mmap_desc,
        const ConstBytesView &host_resp_mmap_desc) {
        BytesBuffer msg(sizeof(InitQpHeader) + host_req_mem_size + host_resp_mem_size);
        BytesView dst(msg);

        auto *header = (InitQpHeader *)dst.data();
        header->type = Type::INIT_QP;
        header->qp_id = qp_id;
        header->host_req_mem_addr = host_req_mem_addr;
        header->host_req_mem_size = host_req_mem_size;
        header->host_resp_mem_addr = host_resp_mem_addr;
        header->host_resp_mem_size = host_resp_mem_size;
        header->host_req_mmap_desc_size = host_req_mmap_desc.size();
        header->host_resp_mmap_desc_size = host_resp_mmap_desc.size();
        dst = dst.subspan(sizeof(InitQpHeader));
  
        std::memcpy(dst.data(), host_req_mmap_desc.data(), host_req_mmap_desc.size());
        dst = dst.subspan(host_req_mmap_desc.size());

        std::memcpy(dst.data(), host_resp_mmap_desc.data(), host_resp_mmap_desc.size());
        return msg;
    }

    static BytesBuffer make_exit_msg() {
        BytesBuffer msg(sizeof(ExitHeader));
        auto *header = (ExitHeader *)msg.data();
        header->type = Type::EXIT;
        return msg;
    }

    static BytesBuffer make_ack_msg() {
        BytesBuffer msg(sizeof(AckHeader));
        auto *header = (AckHeader *)msg.data();
        header->type = Type::ACK;
        return msg;
    }

    static Variant read(ConstBytesView src) {
        if (src.size() < sizeof(Type)) {
            throw_with("wrong size to read ctrl msg");
        }

        auto *msg_type = (Type *)src.data();
        src.subspan(sizeof(Type));

        switch (*msg_type) {
            case Type::INIT: {
                if (src.size() < sizeof(InitHeader)) {
                    throw_with("wrong size to read init ctrl msg");
                }
                InitMsg init_msg;
                init_msg.header = (InitHeader *)src.data();
                return init_msg;
            }
            case Type::INIT_QP: {
                if (src.size() < sizeof(InitQpHeader)) {
                    throw_with("wrong size to read init qp ctrl msg");
                }
                InitQpMsg init_qp_msg;
                init_qp_msg.header = (InitQpHeader *)src.data();
                src = src.subspan(sizeof(InitQpHeader));

                if (src.size() < init_qp_msg.header->host_req_mmap_desc_size) {
                    throw_with("wrong size to read init qp ctrl msg");
                }
                init_qp_msg.host_req_mmap_desc = src.data();
                src = src.subspan(init_qp_msg.header->host_req_mmap_desc_size);

                if (src.size() < init_qp_msg.header->host_resp_mmap_desc_size) {
                    throw_with("wrong size to read init qp ctrl msg");
                }
                init_qp_msg.host_resp_mmap_desc = src.data();
                return init_qp_msg;
            }
            case Type::EXIT: {
                if (src.size() < sizeof(ExitHeader)) {
                    throw_with("wrong size to read exit ctrl msg");
                }
                ExitMsg exit_msg;
                exit_msg.header = (ExitHeader *)src.data();
                return exit_msg;
            }
            case Type::ACK: {
                if (src.size() < sizeof(AckHeader)) {
                    throw_with("wrong size to read ack ctrl msg");
                }
                AckMsg ack_msg;
                ack_msg.header = (AckHeader *)src.data();
                return ack_msg;
            }
        }
    }
};

struct RingBufferResponse {
    enum class Type {
        OPEN,
        READ,
        WRITE,
        FSYNC,
        CLOSE
    };

    struct CommonHeader {
        Type type;
        int32_t error_code;
        size_t req_id;
    };
    struct OpenHeader: CommonHeader {
        size_t file_id;
    };
    struct ReadHeader: CommonHeader {
        size_t actual_read_size;
    };
    struct WriteHeader: CommonHeader {};
    struct FsyncHeader: CommonHeader {};
    struct CloseHeader: CommonHeader {};

    // 如果内存不连续，那么将完整拷贝一份到堆中
    // (Read Response 读到的数据除外，这部分数据向量化接口 I/O)
    struct OpenResponse {
        bool is_owner{};
        OpenHeader *header{};

        size_t size() const {
            return sizeof(OpenHeader);
        }
        ~OpenResponse() {
            if (is_owner) {
                delete header;
            }
        }
    };
    struct ReadResponse {
        bool is_owner{};
        ReadHeader *header{};
        std::optional<RingBuffer::View> content;

        size_t size() const {
            size_t ret = sizeof(ReadHeader);
            if (content) {
                ret += content->size();
            }
            return ret;
        }
        ~ReadResponse() {
            if (is_owner) {
                delete header;
            }
        }
    };
    struct WriteResponse {
        bool is_owner{};
        WriteHeader *header{};

        size_t size() const {
            return sizeof(WriteHeader);
        }
        ~WriteResponse() {
            if (is_owner) {
                delete header;
            }
        }
    };
    struct FsyncResponse {
        bool is_owner{};
        FsyncHeader *header{};

        size_t size() const {
            return sizeof(FsyncHeader);
        }
        ~FsyncResponse() {
            if (is_owner) {
                delete header;
            }
        }
    };
    struct CloseResponse {
        bool is_owner{};
        CloseHeader *header{};

        size_t size() const {
            return sizeof(CloseHeader);
        }
        ~CloseResponse() {
            if (is_owner) {
                delete header;
            }
        }
    };
    using Variant = std::variant<OpenResponse, ReadResponse, WriteResponse, FsyncResponse, CloseResponse>;

    static Variant read(RingBuffer::View &src) {
        Type resp_type{};
        {
            auto copy = src;
            auto common_header = copy.cut(sizeof(CommonHeader));
            if (common_header.second_part()) {
                auto merged_common_header = common_header.merge();
                resp_type = ((CommonHeader *)merged_common_header.data())->type;
            } else {
                resp_type = ((CommonHeader *)common_header.first_part().data())->type;
            }
        }

        switch (resp_type) {
            case Type::OPEN: {
                OpenResponse resp;
                ref_or_copy_resp_header(src, resp);
                return resp;
            }
            case Type::READ: {
                ReadResponse resp;
                ref_or_copy_resp_header(src, resp);
                resp.content = src.cut(resp.header->actual_read_size);
                return resp;
            }
            case Type::WRITE: {
                WriteResponse resp;
                ref_or_copy_resp_header(src, resp);
                return resp;
            }
            case Type::FSYNC: {
                FsyncResponse resp;
                ref_or_copy_resp_header(src, resp);
                return resp;
            }
            case Type::CLOSE: {
                CloseResponse resp;
                ref_or_copy_resp_header(src, resp);
                return resp;
            }
        }
    }

private:
    template<class Response>
    static void ref_or_copy_resp_header(RingBuffer::View &src, Response &resp) {
        using Header =  std::remove_reference_t<decltype(*resp.header)>;
        auto header_rb_slice = src.cut(sizeof(Header));
        char *stream;
        if (header_rb_slice.second_part()) {
            stream = new char[header_rb_slice.size()];
            header_rb_slice.merge({stream, header_rb_slice.size()});
            resp.is_owner = true;
        } else {
            stream = header_rb_slice.first_part().data();
            resp.is_owner = false;
        }
        resp.header = (Header *)stream;
    }
};

// 数据路径消息，基于环形队列做中转异步传输
// 构造消息时会发生拷贝，读消息时无拷贝
struct RingBufferRequest {
    enum class Type {
        OPEN,
        READ,
        WRITE,
        FSYNC,
        CLOSE
    };

    struct CommonHeader {
        Type type;
        size_t req_id;
    };
    struct OpenHeader: CommonHeader {
        uint32_t filename_size;
        uint32_t open_flags;
    };
    struct ReadHeader: CommonHeader {
        size_t file_id;
        size_t offset;
        size_t read_size;
    };
    struct WriteHeader: CommonHeader {
        size_t file_id;
        size_t offset;
        size_t write_size;
    };
    struct FsyncHeader: CommonHeader {
        size_t file_id;
    };
    struct CloseHeader: CommonHeader {
        size_t file_id;
    };

    // 如果内存不连续，那么将完整拷贝一份到堆中
    // (Write Request 要写入的数据，这部分将调用 writev 接口向量化写入)
    struct OpenRequest {
        bool is_owner{};
        OpenHeader *header{};
        char *filename{};
        size_t estimated_resp_size() const {
            return sizeof(RingBufferResponse::OpenHeader);
        }

        ~OpenRequest() {
            delete header;
            delete[] filename;
        }
    };
    struct ReadRequest {
        bool is_owner{};
        ReadHeader *header{};
        size_t estimated_resp_size() const {
            return sizeof(RingBufferResponse::ReadHeader) + header->read_size;
        }

        ~ReadRequest() {
            delete header;
        }
    };
    struct WriteRequest {
        bool is_owner{};
        WriteHeader *header;
        std::optional<RingBuffer::View> content;
        size_t estimated_resp_size() const {
            return sizeof(RingBufferResponse::WriteHeader);
        }

        ~WriteRequest() {
            delete header;
        }
    };
    struct FsyncRequest {
        bool is_owner{};
        FsyncHeader *header;
        size_t estimated_resp_size() const {
            return sizeof(RingBufferResponse::FsyncHeader);
        }

        ~FsyncRequest() {
            delete header;
        }
    };
    struct CloseRequest {
        bool is_owner{};
        CloseHeader *header;
        size_t estimated_resp_size() const {
            return sizeof(RingBufferResponse::CloseHeader);
        }

        ~CloseRequest() {
            delete header;
        }
    };
    using Variant = std::variant<OpenRequest, ReadRequest, WriteRequest, FsyncRequest, CloseRequest>;

    static bool write_open_request(RingBuffer &rb, size_t req_id, const std::string_view &filename, uint32_t open_flags) {
        if (rb.free_size() < sizeof(OpenHeader) + filename.size()) {
            return false;
        }
        OpenHeader header;
        header.type = Type::OPEN;
        header.req_id = req_id;
        header.filename_size = filename.size();
        header.open_flags = open_flags;
        rb.write({(const char *)&header, sizeof(header)});
        rb.write(filename);
        return true;
    }

    static bool write_write_request(RingBuffer &rb, size_t req_id, size_t file_id, const ConstBytesView &content) {
        if (rb.free_size() < sizeof(WriteHeader) + content.size()) {
            return false;
        }
        WriteHeader header;
        header.type = Type::WRITE;
        header.req_id = req_id;
        header.file_id = file_id;
        header.write_size = content.size();
        rb.write({(const char *)&header, sizeof(header)});
        rb.write(content);
        return true;
    }

    static bool write_read_request(RingBuffer &rb, size_t req_id, size_t file_id, size_t offset, size_t read_size) {
        if (rb.free_size() < sizeof(ReadHeader)) {
            return false;
        }
        ReadHeader header;
        header.type = Type::READ;
        header.req_id = req_id;
        header.file_id = file_id;
        header.offset = offset;
        header.read_size = read_size;
        rb.write({(const char *)&header, sizeof(header)});
        return true;
    }

    static bool write_fsync_request(RingBuffer &rb, size_t req_id, size_t file_id) {
        if (rb.free_size() < sizeof(FsyncHeader)) {
            return false;
        }
        FsyncHeader header;
        header.type = Type::FSYNC;
        header.req_id = req_id;
        header.file_id = file_id;
        rb.write({(const char *)&header, sizeof(header)});
        return true;
    }

    static bool write_close_request(RingBuffer &rb, size_t req_id, size_t file_id) {
        if (rb.free_size() < sizeof(CloseHeader)) {
            return false;
        }
        CloseHeader header;
        header.type = Type::CLOSE;
        header.req_id = req_id;
        header.file_id = file_id;
        rb.write({(const char *)&header, sizeof(header)});
        return true;
    }

    static Variant read(RingBuffer::View &src) {
        Type req_type{};
        {
            auto copy = src;
            auto common_header = copy.cut(sizeof(CommonHeader));
            if (common_header.second_part()) {
                auto merged_common_header = common_header.merge();
                req_type = ((CommonHeader *)merged_common_header.data())->type;
            } else {
                req_type = ((CommonHeader *)common_header.first_part().data())->type;
            }
        }

        switch (req_type) {
            case Type::OPEN: {
                OpenRequest req;
                ref_or_copy_req_header(src, req);
                auto filename_rb_slice = src.cut(req.header->filename_size);
                if (filename_rb_slice.second_part()) {
                    req.filename = new char[filename_rb_slice.size()];
                    filename_rb_slice.merge({req.filename, filename_rb_slice.size()});
                } else {
                    req.filename = filename_rb_slice.first_part().data();
                }
                return req;
            }
            case Type::READ: {
                ReadRequest req;
                ref_or_copy_req_header(src, req);
                return req;
            }
            case Type::WRITE: {
                WriteRequest req;
                ref_or_copy_req_header(src, req);
                req.content = src.cut(req.header->write_size);
                return req;
            }
            case Type::FSYNC: {
                FsyncRequest req;
                ref_or_copy_req_header(src, req);
                return req;
            }
            case Type::CLOSE: {
                CloseRequest req;
                ref_or_copy_req_header(src, req);
                return req;
            }
        }
    }

    static RingBuffer::View read_view(RingBuffer::View &src) {
        Type req_type{};
        {
            auto copy = src;
            auto common_header = copy.cut(sizeof(CommonHeader));
            if (common_header.second_part()) {
                auto merged_common_header = common_header.merge();
                req_type = ((CommonHeader *)merged_common_header.data())->type;
            } else {
                req_type = ((CommonHeader *)common_header.first_part().data())->type;
            }
        }

        switch (req_type) {
            case Type::OPEN: {
                auto copy = src;
                OpenRequest req;
                ref_or_copy_req_header(copy, req);
                return src.cut(sizeof(OpenHeader) + req.header->filename_size);
            }
            case Type::READ: {
                return src.cut(sizeof(ReadHeader));
            }
            case Type::WRITE: {
                auto copy = src;
                WriteRequest req;
                ref_or_copy_req_header(copy, req);
                return src.cut(sizeof(WriteHeader) + req.header->write_size);
            }
            case Type::FSYNC: {
                return src.cut(sizeof(FsyncHeader));
            }
            case Type::CLOSE: {
                return src.cut(sizeof(CloseHeader));
            }
        }
    }

private:
    template<class Request>
    static void ref_or_copy_req_header(RingBuffer::View &src, Request &req) {
        using Header =  std::remove_reference_t<decltype(*req.header)>;
        auto header_rb_slice = src.cut(sizeof(Header));
        char *stream;
        if (header_rb_slice.second_part()) {
            stream = new char[header_rb_slice.size()];
            header_rb_slice.merge({stream, header_rb_slice.size()});
            req.is_owner = true;
        } else {
            stream = header_rb_slice.first_part().data();
            req.is_owner = false;
        }
        req.header = (Header *)stream;
    }
};

struct H2DRbMetaMsg {
    enum class Type {
        NEW_HOST_REQ_RB_TAIL,
        REQUIRE_HOST_REQ_RB_HEAD,
        NEW_HOST_RESP_RB_HEAD,
        REQUIRE_HOST_RESP_RB_TAIL
    } type;
    union Payload {
        size_t new_host_req_rb_tail;
        size_t host_req_rb_head;
        size_t new_host_resp_rb_head;
        size_t host_resp_rb_tail;
    } payload;
};

struct D2HRbMetaMsg {
    enum class Type {
        NEW_HOST_REQ_RB_HEAD,
        NEW_HOST_RESP_RB_TAIL
    } type;
    union Payload {
        size_t host_req_rb_head;
        size_t host_resp_rb_tail;
    } payload;
};
