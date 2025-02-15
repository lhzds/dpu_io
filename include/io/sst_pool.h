#pragma once

#include <cassert>
#include <list>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace pulsedb {

class SstPool {
public:
  SstPool(std::string sst_dir,
    size_t base_pool_size, size_t max_num_mems_in_flush, size_t big_pool_size,
    size_t file_size_base, size_t sst_pad_len);
  
  std::optional<size_t> alloc_slot(size_t file_number, size_t base_size_multiplier);
  void free_slot(size_t file_number);

  std::string slot_file_name(size_t slot_id) {
    return m_sst_dir + "/" + std::to_string(slot_id);
  }

protected:
  void free_slot_impl(std::unordered_map<size_t, size_t>::iterator iter);

  void create_ssts();
  void create_same_size_ssts(size_t file_cnt, size_t file_size, size_t start_id);

  std::pair<size_t, size_t> slot_id_range_of(size_t pool_id) const;
  size_t pool_id_of(size_t slot_id) const;

  size_t pool_cnt() const {
    return 1 + m_big_pool_cnt;
  }

  size_t slot_cnt() const {
    return m_base_pool_size + m_big_pool_cnt * m_big_pool_size;
  }

  const std::string m_sst_dir;

  const size_t m_base_pool_size;
  const size_t m_big_pool_cnt;
  const size_t m_big_pool_size;
  const size_t m_file_size_base;
  const size_t m_sst_pad_len;

  // slot id -> file number (filenumber 从 1开始，0 表示 slot 未分配)
  std::vector<size_t> m_slots;
  // file number -> slot id
  std::unordered_map<size_t, size_t> m_file_slot;

  // 各 pool 空闲的 slot id
  std::vector<std::list<size_t>> m_avail_slots;
  // 各 pool 的 slot 被使用数
  std::vector<size_t> m_used_cnts;
};

} // namespace pulsedb
