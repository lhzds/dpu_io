#pragma once

#include <functional>
#include <queue>

// FIFO 事件队列（需要满足 cond，才能执行 action）
struct Event {
    std::function<bool()> cond;
    std::function<void()> action;
};
using EventQueue = std::queue<Event>;

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
