#pragma once
#include <stdexcept>
#include <string>
#include <fmt/format.h>
#include <chrono>
#include <type_traits>

#include "constant.h"

inline static std::string size_str(size_t sz) {
    if (sz >= 1 * MB) {
        return fmt::format("{:.2f}M", static_cast<double>(sz) / MB);
    } else if (sz >= 1 * KB) {
        return fmt::format("{:.2f}K", static_cast<double>(sz) / KB);
    } else {
        return fmt::format("{}B", sz);
    }
}

template<typename Duration>
inline static std::string duration_str(Duration d) {
    using namespace std::chrono;
    if constexpr (std::is_same_v<seconds, Duration>) {
        return fmt::format("{}s", d.count());
    } else if constexpr(std::is_same_v<milliseconds, Duration>) {
        return fmt::format("{}ms", d.count());
    } else if constexpr(std::is_same_v<microseconds, Duration>) {
        return fmt::format("{}us", d.count());
    } else if constexpr(std::is_same_v<nanoseconds, Duration>) {
        return fmt::format("{}ns", d.count());
    } else {
        throw std::runtime_error("unsupported duration type");
    }
}
