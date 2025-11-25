#include <db/LeafPage.hpp>
#include <db/types.hpp>
#include <cstring>
#include <stdexcept>
using namespace db;


namespace {
// 读取指定槽位的 key（仅用于二分/比较）
inline int key_at(const TupleDesc& td, size_t key_index,
                  const uint8_t* base, size_t tbytes, size_t slot) {
  const uint8_t* p = base + slot * tbytes;
  Tuple tmp = td.deserialize(p);
  return std::get<int>(tmp.get_field(key_index));
}
} // namespace

LeafPage::LeafPage(Page &page, const TupleDesc &td_, size_t key_idx)
  : td(td_), key_index(key_idx)
{
  // 页布局：| LeafPageHeader | tuple bytes ... |
  header = reinterpret_cast<LeafPageHeader*>(page.data());
  data   = reinterpret_cast<uint8_t*>(page.data()) + sizeof(LeafPageHeader);

  const size_t bytes_for_tuples =
      page.size() - static_cast<uint32_t>(sizeof(LeafPageHeader));
  const size_t tbytes = td.length();
  capacity = static_cast<uint16_t>(bytes_for_tuples / tbytes);

  // 防御：若未格式化/异常，置零
  if (header->size > capacity) header->size = 0;
}

bool LeafPage::insertTuple(const Tuple &t) {
  const size_t   tbytes = td.length();
  const uint16_t n      = header->size;
  const int      k      = std::get<int>(t.get_field(key_index));

  // 二分找插入位（第一个 >= k）
  uint16_t lo = 0, hi = n;
  while (lo < hi) {
    const uint16_t mid = static_cast<uint16_t>((lo + hi) >> 1);
    if (key_at(td, key_index, data, tbytes, mid) < k)
      lo = mid + 1;
    else
      hi = mid;
  }
  const uint16_t pos = lo;

  // 1. 如果 key 已存在：允许更新（即使 n == capacity）
  if (pos < n && key_at(td, key_index, data, tbytes, pos) == k) {
    td.serialize(data + pos * tbytes, t);   // 覆盖 "apple" → "orange"
    return (n == capacity);                 // 页是否已满，按测试语义返回 true/false
  }

  // 2. 走到这里说明是“新 key”
  //    新 key 时如果已经满页，就不能插入，让上层去 split
  if (n >= capacity) {
    return true;    // 页满且无空间插新 tuple
  }

  // 3. 有空间插新 key：右移 [pos..n-1]，腾位置插入
  const size_t move_bytes = static_cast<size_t>(n - pos) * tbytes;
  if (move_bytes) {
    std::memmove(data + (pos + 1) * tbytes, data + pos * tbytes, move_bytes);
  }

  td.serialize(data + pos * tbytes, t);
  header->size = static_cast<uint16_t>(n + 1);

  return (header->size == capacity);
}


int LeafPage::split(LeafPage &new_page) {
  const uint16_t n = header->size;
  if (n == 0) throw std::logic_error("split on empty leaf page");

  const size_t tbytes = td.length();
  const uint16_t mid  = static_cast<uint16_t>(n / 2);
  const uint16_t right_cnt = static_cast<uint16_t>(n - mid);

  // 分裂键：新页第一条（原 mid 槽位）
  const int split_key = key_at(td, key_index, data, tbytes, mid);

  // 右半复制到新页；左半截断
  std::memcpy(new_page.data, data + mid * tbytes, static_cast<size_t>(right_cnt) * tbytes);
  new_page.header->size = right_cnt;
  new_page.header->next_leaf = header->next_leaf;

  header->size = mid;

  return split_key;
}

Tuple LeafPage::getTuple(size_t slot) const {
  if (slot >= header->size) throw std::out_of_range("leaf slot out of range");
  const size_t tbytes = td.length();
  return td.deserialize(data + slot * tbytes);
}
