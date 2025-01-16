#pragma once

#include <chrono>
#include <functional>

namespace pulsedb {
using Timer = std::chrono::high_resolution_clock;
using ClockDuration = std::chrono::nanoseconds;

static inline ClockDuration clock(std::function<void()> f) {
    auto start = Timer::now();
    f();
    auto end = Timer::now();
    return end - start;
}
} // namespace pulsedb
