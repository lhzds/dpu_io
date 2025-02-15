#include "io/sst_pool.h"
#include "bytes.h"
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>

namespace pulsedb {
SstPool::SstPool(std::string sst_dir, size_t base_pool_size,
                 size_t big_pool_cnt, size_t big_pool_size,
                 size_t file_size_base, size_t sst_pad_len)
    : m_sst_dir(std::move(sst_dir)), m_base_pool_size(base_pool_size),
      m_big_pool_cnt(big_pool_cnt), m_big_pool_size(big_pool_size),
      m_file_size_base(file_size_base), m_sst_pad_len(sst_pad_len) {
  create_ssts();

  m_slots.resize(slot_cnt());
  m_used_cnts.resize(pool_cnt());
  m_avail_slots.resize(pool_cnt());

  for (size_t pool_id{}; pool_id < pool_cnt(); ++pool_id) {
    auto [start, end] = slot_id_range_of(pool_id);

    for (size_t slot_id{start}; slot_id < end; ++slot_id) {
      m_avail_slots[pool_id].push_back(slot_id);
    }
  }
}

void SstPool::create_same_size_ssts(size_t file_cnt, size_t file_size,
                                    size_t start_id) {
  BytesBuffer buf;

  for (size_t i{}; i < file_cnt; ++i) {
    auto sst_name = slot_file_name(start_id + i);
    if (std::filesystem::exists(sst_name)) {
      continue;
    }

    int fd = open(sst_name.c_str(), O_WRONLY, 0644);
    if (fd < 0) {
      throw_with("Failed to open {}: {}", sst_name, strerror(errno));
    }

    if (i == 0) {
      buf.resize(file_size);
    }

    if (int ret = write(fd, buf.data(), buf.size()); ret < 0) {
      close(fd);
      std::filesystem::remove(sst_name);
      throw_with("Failed to write {} with size={}: {}", sst_name, buf.size(),
                 strerror(errno));
    }

    if (int ret = fsync(fd); ret < 0) {
      close(fd);
      std::filesystem::remove(sst_name);
      throw_with("Failed to sync {}: {}", sst_name, strerror(errno));
    }

    close(fd);
  }
}

void SstPool::create_ssts() {
  // 创建 base pool 的文件
  create_same_size_ssts(m_base_pool_size, m_file_size_base + m_sst_pad_len, 0);

  // 创建 big pool 的文件
  size_t start_id = m_base_pool_size;
  size_t file_size = 2 * m_file_size_base + m_sst_pad_len;
  for (size_t i{}; i < m_big_pool_cnt; ++i) {
    create_same_size_ssts(m_big_pool_size, file_size, start_id);

    start_id += m_big_pool_size;
    file_size += m_file_size_base;
  }
}

std::optional<size_t> SstPool::alloc_slot(size_t file_number,
                                          size_t base_size_multiplier) {
  assert(base_size_multiplier >= 1);
  size_t pool_id = base_size_multiplier - 1;
  assert(pool_id < PoolCnt());
  if (m_avail_slots[pool_id].empty()) {
    return {};
  }
  auto slot_id = m_avail_slots[pool_id].front();
  m_avail_slots[pool_id].pop_front();

  ++m_used_cnts[pool_id];
  assert(m_slots[slot_id] == 0);
  m_slots[slot_id] = file_number;
  m_file_slot[file_number] = slot_id;
  return slot_id;
}

void SstPool::free_slot(size_t file_number) {
  auto iter = m_file_slot.find(file_number);
  assert(iter != m_file_slot.end());

  auto [_, slot_id] = *iter;
  m_file_slot.erase(iter);

  m_slots[slot_id] = 0;
  size_t pool_id = pool_id_of(slot_id);
  --m_used_cnts[pool_id];
  m_avail_slots[pool_id].push_back(slot_id);
}

std::pair<size_t, size_t> SstPool::slot_id_range_of(size_t pool_id) const {
  assert(pool_id < PoolCnt());

  if (pool_id == 0) {
    return {0, m_base_pool_size};
  }
  size_t big_pool_id = pool_id - 1;
  auto start = m_base_pool_size + big_pool_id * m_big_pool_size;
  auto end = m_base_pool_size + (big_pool_id + 1) * m_big_pool_size;
  return {start, end};
}

size_t SstPool::pool_id_of(size_t slot_id) const {
  if (slot_id < m_base_pool_size) {
    return 0;
  }
  return 1 + (slot_id - m_base_pool_size) / m_big_pool_size;
}

} // namespace pulsedb
