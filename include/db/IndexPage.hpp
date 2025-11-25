#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <db/types.hpp>   // Page = std::array<uint8_t, DEFAULT_PAGE_SIZE>

namespace db {

// 索引页头：size = key 个数；index_children 表示 children 指向的是“索引页(1)”还是“叶页(0)”
struct IndexPageHeader {
  uint16_t size;
  uint8_t  index_children;   // 0: children 指向 LeafPage；1: children 指向 IndexPage
};

// 逻辑布局：| IndexPageHeader | keys[capacity] | children[capacity+1] |
// - keys: int32_t 升序
// - children: size_t 指向子页（size+1 个有效）
struct IndexPage {
  uint16_t         capacity{};   // 最大可容纳的 key 数
  IndexPageHeader* header{nullptr};
  int32_t*         keys{nullptr};
  size_t*          children{nullptr};

  explicit IndexPage(Page &page) {
    // 头部
    header = reinterpret_cast<IndexPageHeader*>(page.data());
    // 计算容量
    uint8_t* base = reinterpret_cast<uint8_t*>(page.data()) + sizeof(IndexPageHeader);
    const size_t rem = page.size() - sizeof(IndexPageHeader);
    const size_t ksz = sizeof(int32_t);
    const size_t csz = sizeof(size_t);

    // rem = capacity*ksz + (capacity+1)*csz  => capacity = (rem - csz) / (ksz + csz)
    const size_t cap = (rem > csz) ? ((rem - csz) / (ksz + csz)) : 0;
    capacity = static_cast<uint16_t>(cap);

    // 指针切分
    keys     = reinterpret_cast<int32_t*>(base);
    children = reinterpret_cast<size_t*>(base + capacity * ksz);

    // 防御：超界时复位
    if (header->size > capacity) header->size = 0;
  }

  // 在保持升序的前提下，把 (key, child) 插入到“合适位置的右侧”
  // 返回值：true 表示已满，需要上层 split
  bool insert(int key, size_t child) {
    const uint16_t n = header->size;
    if (n >= capacity) return true;  // 满了，交由上层 split

    // 找到第一个 >= key 的位置
    uint16_t pos = 0;
    while (pos < n && keys[pos] < key) ++pos;

    // 右移 keys[pos..n-1] → [pos+1..n]
    if (n > pos) {
      std::memmove(&keys[pos + 1], &keys[pos], (n - pos) * sizeof(int32_t));
    }
    // 右移 children[pos+1..n] → [pos+2..n+1]
    if (n + 1 > pos + 1) {
      std::memmove(&children[pos + 2], &children[pos + 1], (n - pos) * sizeof(size_t));
    }

    keys[pos]       = static_cast<int32_t>(key);
    children[pos+1] = child;

    header->size = static_cast<uint16_t>(n + 1);
    return (header->size == capacity);
  }

  // 分裂：把中间 key 上推到父结点（不保留在左右子页中）
  // 返回值：上推的中位 key
  int split(IndexPage &new_page) {
    const uint16_t n = header->size;
    // 约定：n>0 时才会被调用
    const uint16_t mid   = static_cast<uint16_t>(n / 2);
    const int up_key     = keys[mid];
    const uint16_t right = static_cast<uint16_t>(n - mid - 1); // 右侧 key 个数

    // 把右半部分移动到新页
    if (right > 0) {
      std::memcpy(new_page.keys, &keys[mid + 1], right * sizeof(int32_t));
    }
    // children 右半（比 keys 多 1 个）：children[mid+1 .. n]
    if (right + 1 > 0) {
      std::memcpy(new_page.children, &children[mid + 1], (right + 1) * sizeof(size_t));
    }

    // 更新两页 size
    new_page.header->size = right;
    new_page.header->index_children = header->index_children; // 同一层级
    header->size = mid; // 左页保留 [0..mid-1] 和 children[0..mid]

    return up_key; // 上推键
  }
};

} // namespace db
