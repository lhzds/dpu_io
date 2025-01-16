#pragma once

#include <functional>
#include <queue>
#include <boost/lockfree/spsc_queue.hpp>
#include "constant.h"

namespace pulsedb {
// FIFO 事件队列（需要满足 cond，才能执行 action）
struct Event {
    std::function<bool()> cond;
    std::function<void()> action;
};
using EventQueue = std::queue<Event>;

using MsgQueue = boost::lockfree::spsc_queue<std::function<void()>,
        boost::lockfree::capacity<MSG_QUEUE_SIZE>>;

// 区间类型
struct Interval {
    size_t offset;
    size_t size;

    Interval shrink_forward(size_t n) {
        Interval origin = *this;
        offset += n;
        size -= n;
        return origin;
    }
};
}
