#pragma once

#include <iostream>
#include <stdexcept>
#include <string_view>
#include <fmt/format.h>

template<class... Args>
static inline void throw_with(std::string_view format, Args... args) {
    throw std::runtime_error(fmt::format(fmt::runtime(format), args...));
}
