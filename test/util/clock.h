#pragma once

#include <chrono>
#include <functional>

using Timer = std::chrono::high_resolution_clock;

static inline std::chrono::nanoseconds clock(std::function<void()> f) {
    auto start = Timer::now();
    f();
    auto end = Timer::now();
    return end - start;
}
