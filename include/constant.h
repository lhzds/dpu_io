#pragma once

#include <cstddef>

namespace pulsedb {

constexpr size_t KB = (1 << 10);
constexpr size_t MB = (1 << 20);
constexpr size_t GB = (1 << 30);

constexpr size_t kIoEngineCqDepth = 1024;
constexpr size_t kCommRbMetaSize = 128;
constexpr size_t kDefaultDirectAlignment = 4096;

} // namespace pulsedb
