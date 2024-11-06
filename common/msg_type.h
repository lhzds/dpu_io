#pragma once

#include "bytes.h"
#include <cassert>
#include <cstddef>
#include <stdexcept>

template<typename T>
static inline T *parse(ConstBytesView view) {
    if (sizeof(T) != view.size()) {
        throw std::runtime_error("parsing failed: size mismatch");
    }
    return (T *)view.data();
}

struct CtrlMsg {
    enum class Type {
        INIT,
        INIT_QP, // send qp id and export desc 
        EXIT,
        ACK
    };

    struct CommonHeader {
        Type m_type;
    };

    struct InitHeader {
        size_t m_qp_num;
    };

    struct InitQpHeader {
        size_t m_qp_id;
        
        void *m_req_mem_addr;
        size_t m_req_mem_size;
        void *m_resp_mem_addr;
        size_t m_resp_mem_size;

        size_t m_req_export_desc_len;
        size_t m_resp_export_desc_len;
    };

    constexpr static size_t common_header_size() {
        return sizeof(Type);
    }

    constexpr static size_t init_header_size() {
        return sizeof(CommonHeader) + sizeof(InitHeader);
    }

    constexpr static size_t init_qp_header_size() {
        return sizeof(CommonHeader) + sizeof(InitQpHeader);
    }

    constexpr static size_t exit_header_size() {
        return sizeof(CommonHeader);
    }

    static BytesBuffer make_init_msg(size_t qp_num) {
        BytesBuffer msg;
        auto msg_type = Type::INIT;
        BytesBuffer().append((const char *)(&msg_type), sizeof(Type));

        InitHeader init_msg_header;
        init_msg_header.m_qp_num = qp_num;
        BytesBuffer().append((const char *)(&init_msg_header), sizeof(InitHeader));

        return msg;
    }

    static BytesBuffer make_init_qp_msg(size_t qp_id,
        void *m_req_mem_addr, size_t m_req_mem_size,
        void *m_resp_mem_addr, size_t m_resp_mem_size,
        const ConstBytesView &req_export_desc,
        const ConstBytesView &resp_export_desc) {
        BytesBuffer msg;
        auto msg_type = Type::INIT_QP;
        BytesBuffer().append((const char *)(&msg_type), sizeof(Type));

        InitQpHeader init_qp_header;
        init_qp_header.m_qp_id = qp_id;
        init_qp_header.m_req_mem_addr = m_req_mem_addr;
        init_qp_header.m_req_mem_size = m_req_mem_size;
        init_qp_header.m_resp_mem_addr = m_resp_mem_addr;
        init_qp_header.m_resp_mem_size = m_resp_mem_size;
        init_qp_header.m_req_export_desc_len = req_export_desc.size();
        init_qp_header.m_resp_export_desc_len = resp_export_desc.size();
        BytesBuffer().append((const char *)(&init_qp_header), sizeof(InitQpHeader));
        
        BytesBuffer().append(req_export_desc);
        BytesBuffer().append(resp_export_desc);
        return msg;
    }

    static BytesBuffer make_exit_msg() {
        BytesBuffer msg;
        auto msg_type = Type::EXIT;
        BytesBuffer().append((const char *)(&msg_type), sizeof(Type));
        return msg;
    }

    static BytesBuffer make_ack_msg() {
        BytesBuffer msg;
        auto msg_type = Type::ACK;
        BytesBuffer().append((const char *)(&msg_type), sizeof(Type));
        return msg;
    }
};

struct RequestMsg {
    enum class Type {
        READ,
        WRITE,
        FSYNC
    };

    struct Header {
        Type m_type;
        size_t m_qp_id;
        size_t m_req_id;
        size_t m_content_len;
    };

    constexpr static size_t header_size() {
        return sizeof(Header);
    };

    static BytesBuffer make_header(Type type, size_t qp_id, size_t req_id, size_t size) {
        BytesBuffer msg;
        msg.append((const char *)(&type), sizeof(Type));

        Header header;
        header.m_qp_id = qp_id;
        header.m_content_len = size;
        header.m_req_id = req_id;
        msg.append((const char *)(&header), sizeof(Header));

        return msg;
    };
};

struct ResponseMsg {
    struct Header {
        size_t m_error_code;
        size_t m_req_id;
        size_t m_content_len;
    };

    constexpr static size_t header_size() {
        return sizeof(Header);
    }

    static BytesBuffer make_header(size_t error_code, size_t req_id, size_t size) {
        BytesBuffer msg;
        Header header;
        header.m_error_code = error_code;
        header.m_content_len = size;
        header.m_req_id = req_id;
        msg.append((const char *)(&header), sizeof(Header));

        return msg;
    }
};

struct H2DBufMetaMsg {
    enum class Type {
        REQ_QUEUE_TAIL,
        RESP_QUEUE_HEAD,
        NEED_REQ_QUEUE_HEAD,
        NEED_RESP_QUEUE_TAIL
    };

    Type m_type;
    union {
        size_t m_req_queue_tail;
        size_t m_resp_queue_head;
    } m_payload;
};

struct D2HBufMetaMsg {
    enum class Type {
        REQ_QUEUE_HEAD,
        RESP_QUEUE_TAIL
    };

    Type m_type;
    union {
        size_t m_req_queue_head;
        size_t m_resp_queue_tail;
    } m_payload;
};
