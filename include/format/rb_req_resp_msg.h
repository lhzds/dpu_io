#pragma once

#include <cassert>
#include <cstddef>
#include <memory>
#include <variant>

#include "bytes.h"
#include "io/io_engine.h"
#include "noncopyable.h"
#include "ring_buffer.h"
#include "util.h"

namespace pulsedb {

// 消息空间小，直接进行拷贝
struct IoRespMsg {
    using Type = IoEngine::Op;

    Type type;
    size_t file_number;
    int res; // SyncClose 成功后，res 记录 slot_id

    static BytesBuffer make(Type type, size_t file_number, int res) {
        BytesBuffer buf(sizeof(IoRespMsg));
        auto *msg = (IoRespMsg *)buf.data();
        msg->type = type;
        msg->file_number = file_number;
        msg->res = res;
        return buf;
    }

    static IoRespMsg read_from(RingBuffer &rb) {
        return rb.read_obj_copy<IoRespMsg>();
    }
};

// 数据路径消息，基于环形队列做中转异步传输
// 读消息时优先原地读
struct IoReqMsg {
    using Type = IoEngine::Op;

    struct CommonHeader {
        Type type;
        size_t file_number;
    };

    struct WriteHeader: CommonHeader {
        int base_size_multiplier;
        size_t offset;
        size_t size;
    };
    struct SyncCloseHeader: CommonHeader {
        bool use_fsync;
    };

    // 如果内存不连续，那么将完整拷贝一份到堆中
    // (Write Request 要写入的数据，这部分将调用 writev 接口向量化写入)
    struct WriteMsgRbRef: public Noncopyable  {
        WriteHeader *header{};
        std::optional<RingBuffer::View> content;
        ~WriteMsgRbRef() {
            if (m_owner_flag & 1) {
                delete header;
            }
        }

        size_t size() const {
            auto ret = sizeof(WriteHeader);
            if (content) {
                ret += content->size();
            }
            return ret;
        }

        void be_header_owner() {
            m_owner_flag &= 1;
        }

    private:
        uint8_t m_owner_flag{}; // 1: header
    };
    struct SyncCloseMsgRbRef: public Noncopyable  {
        SyncCloseHeader *header{};
        ~SyncCloseMsgRbRef() {
            if (m_owner_flag & 1) {
                delete header;
            }
        }

        size_t size() const {
            return sizeof(SyncCloseHeader);
        }

        void be_header_owner() {
            m_owner_flag &= 1;
        }

    private:
        uint8_t m_owner_flag{}; // 1: header
    };

    // 引入所有权机制，只有不需要时才进行析构
    using RefVariant = std::variant<std::unique_ptr<SyncCloseMsgRbRef>, std::unique_ptr<WriteMsgRbRef>>;
    template<typename ReqType>
    static std::unique_ptr<ReqType> get_req_ptr(RefVariant& variant) {
        return std::move(std::get<std::unique_ptr<ReqType>>(variant));
    }

    static void init_write_req_header(WriteHeader &header, size_t file_number, size_t offset, int base_size_multiplier) {
        header.type = Type::kWrite;
        header.file_number = file_number;
        header.offset = offset;
        header.size = 0;
        header.base_size_multiplier = base_size_multiplier;
    }

    static BytesBuffer make_sync_close_req(size_t file_number, bool use_fsync) {
        BytesBuffer req(sizeof(SyncCloseHeader));
        auto *header = (SyncCloseHeader *)req.data();
        header->type = Type::kSyncClose;
        header->file_number = file_number;
        header->use_fsync = use_fsync;
        return req;
    }

    static RefVariant parse_from(RingBuffer::View src) {
        return read_from(src);
    }

    static RefVariant read_from(RingBuffer::View &src) {
        Type req_type = read_req_type(src);

        switch (req_type) {
            case Type::kWrite: {
                auto req = std::make_unique<WriteMsgRbRef>();
                ref_or_copy_req_header(src, *req);
                req->content = src.cut(req->header->size);
                return req;
            }
            case Type::kSyncClose: {
                auto req = std::make_unique<SyncCloseMsgRbRef>();
                ref_or_copy_req_header(src, *req);
                return req;
            }
            default:
                throw_with("IoReqMsg: Unexpected type");
        }
    }

private:
    static Type read_req_type(RingBuffer::View src) {
        Type req_type{};
        auto common_header = src.cut(sizeof(CommonHeader));
        if (common_header.second_part()) {
            auto merged_common_header = common_header.merge();
            req_type = ((CommonHeader *)merged_common_header.data())->type;
        } else {
            req_type = ((CommonHeader *)common_header.first_part().data())->type;
        }
        return req_type;
    }

    template<class RequestRef>
    static void ref_or_copy_req_header(RingBuffer::View &src, RequestRef &req) {
        using Header =  std::remove_reference_t<decltype(*req.header)>;
        auto header_rb_slice = src.cut(sizeof(Header));
        char *stream;
        if (header_rb_slice.second_part()) {
            stream = new char[header_rb_slice.size()];
            header_rb_slice.merge({stream, header_rb_slice.size()});
            req.be_header_owner();
        } else {
            stream = header_rb_slice.first_part().data();
        }
        req.header = (Header *)stream;
    }
};

} // namesapce pulsedb
