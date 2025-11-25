#pragma once

#include <db/DbFile.hpp>
#include <utility>   // std::pair
#include <vector>    // std::vector
#include <cstddef>   // size_t
#include <cstdint>   // int32_t

namespace db {

class Tuple;
class TupleDesc;
class Iterator;

class IndexPage; // 前置声明，避免在头文件包含实现
class LeafPage;  // 前置声明

class BTreeFile : public DbFile {
  // 根页页号恒为 0（文件创建即为索引页）
  static constexpr size_t root_id = 0;

  // 被排序的 key 所在字段下标（整型）
  size_t key_index;

  // ---------- 私有类型与工具 ----------
  using PathElem = std::pair<size_t /*index page id*/, size_t /*child slot*/>;

  struct SplitResult {
    int32_t up_key = 0;                                     // 上推的中位键
    size_t  new_page_id = static_cast<size_t>(-1);          // 新页id
    bool    did_split = false;                              // 是否发生分裂
  };

  // 注意：以下为实现细节的私有声明，仅供 .cpp 使用
  size_t index_page_max_keys() const;
  size_t leaf_page_max_tuples() const;

  static size_t choose_child_slot(const IndexPage &ip, int32_t key);
  std::vector<PathElem> descend_path(int32_t key) const;

  // Page 类型来自 types.hpp/DbFile 的 I/O
  Page  read_page(size_t page_id) const;
  void  write_page(const Page &p, size_t page_id) const;
  size_t allocate_empty_page();

  void ensure_root_initialized();

  SplitResult leaf_insert(size_t leaf_id, const Tuple &t);
  SplitResult index_insert_chain(size_t parent_id, size_t insert_after_child_slot,
                                 int32_t up_key, size_t right_child_id,
                                 bool child_level_is_leaf);

  void split_root_and_rebuild(const SplitResult &root_split, bool child_level_is_leaf);

  Iterator leftmost_begin() const;
  void     advance_with_leaf_link(Iterator &it) const;

public:
  /**
   * @brief Initialize a BTreeFile
   *
   * @param key_index the index of the key in the tuple
   */
  BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index);

  /**
   * @brief Insert a tuple into the file
   * @details Insert a tuple into the file. Traverse the BTree from the root to find the leaf node to insert the tuple.
   * If the leaf node is full, split the node and insert the new key and child to the parent node. This process is repeated
   * until no more split is needed. If the root node is split, create a create two new nodes with the contents of the root
   * and set the root to be the parent of the two new nodes.
   * @param t the tuple to insert
   */
  void insertTuple(const Tuple &t) override;

  void deleteTuple(const Iterator &it) override;

  /**
   * @brief Get a tuple from the database file.
   * @details Get a tuple from the database file by reading the tuple from the page.
   * @param it The iterator that identifies the tuple to be read.
   * @return The tuple read from the page.
   */
  Tuple getTuple(const Iterator &it) const override;

  /**
   * @brief Advance the iterator to the next tuple.
   * @details Advance the iterator to the next tuple by moving to the next slot of the page.
   * If the iterator is at the end of the page, move to the next page.
   * @param it The iterator to be advanced.
   */
  void next(Iterator &it) const override;

  /**
   * @brief Get the iterator to the first tuple of the leftmost leaf (head).
   * @details Traverse the tree to reach the head leaf and return the first tuple.
   * @return The iterator to the first tuple.
   */
  Iterator begin() const override;

  /**
   * @brief Get the iterator to the end of the file.
   * @details Return an iterator that points to the end of the file.
   * @return The iterator to the end of the file.
   */
  Iterator end() const override;
};

} // namespace db
