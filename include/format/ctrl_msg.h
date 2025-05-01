#pragma once

#include <cassert>
#include <cstddef>
#include <variant>

#include "bytes.h"
#include "util.h"

namespace pulsedb {

// 控制路径消息，不需要环形队列同步传输
// 构造消息时会发生拷贝，读消息时无拷贝
struct CtrlMsg {
    enum class Type {
        kInit,
        kINitQp,
        kStartComm,
        kDeleteFile,
        kExit,
        kAck
    };

    struct CommonHeader {
        Type type{};
    };
    struct InitHeader: CommonHeader {
        int qp_num{};
    };
    struct InitQpHeader: CommonHeader {
        int qp_id{-1};
        
        char *host_req_rb_data{};
        size_t host_req_rb_cap{};
        char *host_resp_rb_data{};
        size_t host_resp_rb_cap{};

        size_t host_req_mmap_desc_size{};
        size_t host_resp_mmap_desc_size{};
    };
    struct StartCommHeader: CommonHeader {};
    struct DeleteFileHeader: CommonHeader {
        size_t file_number;
    };
    struct ExitHeader: CommonHeader {};
    struct AckHeader: CommonHeader {};

    struct InitMsgRef {
        InitHeader *header{};
    };
    struct InitQpMsgRef {
        InitQpHeader *header{};
        const char *host_req_mmap_desc{};
        const char *host_resp_mmap_desc{};
    };
    struct StartCommMsgRef {
        StartCommHeader *header{};
    };
    struct DeleteFileMsgRef {
        DeleteFileHeader *header{};
    };
    struct ExitMsgRef {
        ExitHeader *header{};
    };
    struct AckMsgRef {
        AckHeader *header{};
    };
    using RefVariant = std::variant<
        InitMsgRef,
        InitQpMsgRef,
        StartCommMsgRef,
        DeleteFileMsgRef,
        ExitMsgRef,
        AckMsgRef
    >;

    static BytesBuffer make_init_msg(int qp_num) {
        BytesBuffer buf(sizeof(InitHeader));
        auto *header = (InitHeader *)buf.data();
        header->type = Type::kInit;
        header->qp_num = qp_num;
        return buf;
    }

    static BytesBuffer make_init_qp_msg(int qp_id,
        char *host_req_rb_data, size_t host_req_rb_cap,
        char *host_resp_rb_data, size_t host_resp_rb_cap,
        const ConstBytesView &host_req_mmap_desc,
        const ConstBytesView &host_resp_mmap_desc) {
        BytesBuffer buf(sizeof(InitQpHeader) + host_req_mmap_desc.size()
            + host_resp_mmap_desc.size());
        BytesView dst(buf);

        auto *header = (InitQpHeader *)dst.data();
        header->type = Type::kINitQp;
        header->qp_id = qp_id;
        header->host_req_rb_data = host_req_rb_data;
        header->host_req_rb_cap = host_req_rb_cap;
        header->host_resp_rb_data = host_resp_rb_data;
        header->host_resp_rb_cap = host_resp_rb_cap;
        header->host_req_mmap_desc_size = host_req_mmap_desc.size();
        header->host_resp_mmap_desc_size = host_resp_mmap_desc.size();
        dst = dst.subspan(sizeof(InitQpHeader));
  
        std::memcpy(dst.data(), host_req_mmap_desc.data(), host_req_mmap_desc.size());
        dst = dst.subspan(host_req_mmap_desc.size());

        std::memcpy(dst.data(), host_resp_mmap_desc.data(), host_resp_mmap_desc.size());
        return buf;
    }

    static BytesBuffer make_start_comm_msg() {
        BytesBuffer buf(sizeof(StartCommHeader));
        auto *header = (StartCommHeader *)buf.data();
        header->type = Type::kStartComm;
        return buf;
    }

    static BytesBuffer make_delete_file_msg(size_t file_number) {
        BytesBuffer buf(sizeof(DeleteFileHeader));
        auto *header = (DeleteFileHeader *)buf.data();
        header->type = Type::kDeleteFile;
        header->file_number = file_number;
        return buf;
    }

    static BytesBuffer make_exit_msg() {
        BytesBuffer buf(sizeof(ExitHeader));
        auto *header = (ExitHeader *)buf.data();
        header->type = Type::kExit;
        return buf;
    }

    static BytesBuffer make_ack_msg() {
        BytesBuffer buf(sizeof(AckHeader));
        auto *header = (AckHeader *)buf.data();
        header->type = Type::kAck;
        return buf;
    }

    static RefVariant read(ConstBytesView src) {
        if (src.size() < sizeof(Type)) {
            throw_with("CtrlMsg: Wrong size");
        }

        auto *msg_type = (Type *)src.data();
        src.subspan(sizeof(Type));

        switch (*msg_type) {
            case Type::kInit: {
                if (src.size() < sizeof(InitHeader)) {
                    throw_with("CtrlMsg: Wrong init msg size");
                }
                InitMsgRef init_msg;
                init_msg.header = (InitHeader *)src.data();
                return init_msg;
            }
            case Type::kINitQp: {
                if (src.size() < sizeof(InitQpHeader)) {
                    throw_with("CtrlMsg: Wrong init_qp msg size");
                }
                InitQpMsgRef init_qp_msg;
                init_qp_msg.header = (InitQpHeader *)src.data();
                src = src.subspan(sizeof(InitQpHeader));

                if (src.size() < init_qp_msg.header->host_req_mmap_desc_size) {
                    throw_with("CtrlMsg: Wrong init_qp msg size");
                }
                init_qp_msg.host_req_mmap_desc = src.data();
                src = src.subspan(init_qp_msg.header->host_req_mmap_desc_size);

                if (src.size() < init_qp_msg.header->host_resp_mmap_desc_size) {
                    throw_with("CtrlMsg: Wrong init_qp msg size");
                }
                init_qp_msg.host_resp_mmap_desc = src.data();
                return init_qp_msg;
            }
            case Type::kStartComm: {
                if (src.size() < sizeof(StartCommHeader)) {
                    throw_with("CtrlMsg: Wrong start_comm msg size");
                }
                StartCommMsgRef start_comm_msg;
                start_comm_msg.header = (StartCommHeader *)src.data();
                return start_comm_msg;
            }
            case Type::kDeleteFile: {
                if (src.size() < sizeof(DeleteFileHeader)) {
                    throw_with("DeleteFileMsg: Wrong delete_file msg size");
                }
                DeleteFileMsgRef delete_file_msg;
                delete_file_msg.header = (DeleteFileHeader *)src.data();
                return delete_file_msg;
            }      
            case Type::kExit: {
                if (src.size() < sizeof(ExitHeader)) {
                    throw_with("CtrlMsg: Wrong exit msg size");
                }
                ExitMsgRef exit_msg;
                exit_msg.header = (ExitHeader *)src.data();
                return exit_msg;
            }
            case Type::kAck: {
                if (src.size() < sizeof(AckHeader)) {
                    throw_with("CtrlMsg: Wrong ack msg size");
                }
                AckMsgRef ack_msg;
                ack_msg.header = (AckHeader *)src.data();
                return ack_msg;
            }
            default:
                throw_with("CtrlMsg: Unexpected type");
        }
    }
};

} // namesapce pulsedb
