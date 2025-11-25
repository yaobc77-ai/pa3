#pragma once

#include <cstdint>
#include <cstddef>
#include <db/types.hpp>   // Page = std::array<uint8_t, DEFAULT_PAGE_SIZE>

namespace db {

  // 索引页头信息：
  // - size: 当前 keys 个数
  // - index_children: 1 表示 children 指向的是“索引页”，0 表示指向“叶页”
  struct IndexPageHeader {
    uint16_t size;
    uint8_t  index_children;
  };

  struct IndexPage {
    uint16_t         capacity{};     // 最大可容纳的 key 数
    IndexPageHeader* header{nullptr};
    int32_t*         keys{nullptr};  // 升序 keys
    size_t*          children{nullptr}; // size+1 个指针

    // 仅声明，实际实现放在 src/db/IndexPage.cpp
    explicit IndexPage(Page &page);

    // 在保持升序下插入 (key, child)。返回 true 表示“已满，需要 split”
    bool insert(int key, size_t child);

    // 分裂当前页，把中位 key 上推到父结点；返回上推 key
    int split(IndexPage &new_page);
  };

} // namespace db
