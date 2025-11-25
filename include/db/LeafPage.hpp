#pragma once
#include <cstdint>
#include <cstddef>
#include <db/Tuple.hpp>
#include <db/types.hpp>   // 这里需要 Page

namespace db {

  struct LeafPageHeader {
    size_t   next_leaf;  // 没有则可设为 (size_t)-1
    uint16_t size;       // 当前元组数
  };

  struct LeafPage {
    const TupleDesc &td;
    const size_t     key_index;   // key 字段索引（类型为 int）
    uint16_t         capacity{};  // 该页最多可容纳的 tuple 数

    LeafPageHeader *header{nullptr};
    uint8_t        *data{nullptr};

    LeafPage(Page &page, const TupleDesc &td, size_t key_index);

    // 按 key 有序插入；若 key 已存在，则覆盖；返回是否“已满需要 split”
    bool insertTuple(const Tuple &t);

    // 分裂：右半移动到 new_page；返回 new_page 的首 key（分裂键）
    int split(LeafPage &new_page);

    Tuple getTuple(size_t slot) const;
  };

} // namespace db
