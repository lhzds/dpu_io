#pragma once

#include <cstddef>

namespace pulsedb {

struct H2DRbMetaMsg {
    enum class Type {
        kReqRbNewSize,
        kReqRbNeededSpace,
        kWaitStat,
    } type;
    
    union {
        size_t req_rb_new_size;
        size_t req_rb_needed_space;

        struct {
            size_t wait_size;
            size_t processed_size;
        } wait_stat;
    } payload;
};

struct D2HRbMetaMsg {
    enum class Type {
        kReqRbCleanedSize,
        kRespRbNewSize
    } type;

    union {
        size_t req_rb_cleaned_size;
        size_t resp_rb_new_size;
    } payload;
};

} // namesapce pulsedb