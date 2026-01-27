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
    int base_pool_size, int big_pool_cnt, int big_pool_size,
    size_t file_size_base, size_t sst_pad_len);
  
  std::optional<int> alloc_slot(size_t file_number, int base_size_multiplier);
  void free_slot(size_t file_number);
  bool has_slot(size_t file_number) const;

  std::string slot_file_name(int slot_id) {
    return m_sst_dir + "/" + std::to_string(slot_id);
  }

  void create_ssts();

protected:
  void create_same_size_ssts(int file_cnt, size_t file_size, int start_slot_id);

  std::pair<int, int> slot_id_range_of(int pool_id) const;
  int pool_id_of(int slot_id) const;

  int pool_cnt() const {
    return 1 + m_big_pool_cnt;
  }

  int slot_cnt() const {
    return m_base_pool_size + m_big_pool_cnt * m_big_pool_size;
  }

  const std::string m_sst_dir;

  const int m_base_pool_size;
  const int m_big_pool_cnt;
  const int m_big_pool_size;
  const size_t m_file_size_base;
  const size_t m_sst_pad_len;

  // slot id -> file number (filenumber 从 1开始，0 表示 slot 未分配)
  std::vector<size_t> m_slots;
  // file number -> slot id
  std::unordered_map<size_t, int> m_file_slot;

  // 各 pool 空闲的 slot id
  std::vector<std::list<int>> m_avail_slots;
  // 各 pool 被使用的 slot 数
  std::vector<int> m_used_slot_cnts;
};

} // namespace pulsedb
