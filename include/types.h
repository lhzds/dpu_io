#pragma once

#include <functional>
#include <queue>
#include <boost/lockfree/spsc_queue.hpp>
#include "constant.h"

namespace pulsedb {
// FIFO 事件队列（需要满足 cond，才能执行 action）
class EventQueue {
public:
    void do_or_wait(std::function<bool()> cond, std::function<void()> action) {
        // 如果队列中有等待进行的任务（FIFO）
        if (not m_queue.empty() || not cond()) {
            m_queue.emplace(std::move(cond), std::move(action));
        } else {
            action();
        }
    }

    void try_trigger() {
        while (not m_queue.empty() && m_queue.front().cond()) {
            auto &[_, action] = m_queue.front();
            action();
            m_queue.pop();
        }
    }
private:
    struct Event {
        std::function<bool()> cond;
        std::function<void()> action;

        Event(std::function<bool()> cond_, std::function<void()> action_)
            : cond(std::move(cond_)), action(std::move(action_)) {}
    };
    std::queue<Event> m_queue;
};

// 区间类型
struct Interval {
    size_t offset;
    size_t size;

    Interval(size_t offset_, size_t size_): offset(offset_), size(size_) {}

    Interval shrink_forward(size_t n) {
        Interval origin = *this;
        offset += n;
        size -= n;
        return origin;
    }
};

} // namespace pulsedb
