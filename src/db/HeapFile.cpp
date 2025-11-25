#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <db/BufferPool.hpp> // 必须包含
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

void HeapFile::insertTuple(const Tuple &t) {
    if (!getTupleDesc().compatible(t)) {
        throw std::logic_error("HeapFile::insertTuple: tuple not compatible with schema");
    }

    const TupleDesc &td = getTupleDesc();
    BufferPool &bp = getDatabase().getBufferPool();

    // 1. 尝试插入到最后一页
    if (numPages > 0) {
        size_t last_page_idx = numPages - 1;
        PageId pid{name, last_page_idx};

        Page &page = bp.getPage(pid);
        HeapPage hp(page, td);

        if (hp.insertTuple(t)) {
            bp.markDirty(pid);
            return;
        }
    }

    // 2. 新建页面
    size_t new_page_idx = numPages;
    PageId new_pid{name, new_page_idx};

    // 获取新页（BufferPool 会处理底层读取，对于新页是全0）
    Page &new_page = bp.getPage(new_pid);
    HeapPage hp_new(new_page, td);

    if (!hp_new.insertTuple(t)) {
        throw std::runtime_error("HeapFile::insertTuple: failed to insert into empty page");
    }

    bp.markDirty(new_pid);

    numPages++;
}

void HeapFile::deleteTuple(const Iterator &it) {
    if (it.page >= numPages) throw std::out_of_range("HeapFile::deleteTuple: page out of range");

    BufferPool &bp = getDatabase().getBufferPool();
    PageId pid{name, it.page};

    Page &page = bp.getPage(pid);
    HeapPage hp(page, getTupleDesc());
    hp.deleteTuple(it.slot);
    bp.markDirty(pid);
}

Tuple HeapFile::getTuple(const Iterator &it) const {
    if (it.page >= numPages) throw std::out_of_range("HeapFile::getTuple: page out of range");

    BufferPool &bp = getDatabase().getBufferPool();
    PageId pid{name, it.page};

    Page &page = bp.getPage(pid);
    const HeapPage hp(page, getTupleDesc());
    return hp.getTuple(it.slot);
}

void HeapFile::next(Iterator &it) const {
    if (it.page >= numPages) {
        it.page = numPages;
        it.slot = 0;
        return;
    }

    BufferPool &bp = getDatabase().getBufferPool();
    const TupleDesc &td = getTupleDesc();

    // 当前页查找
    {
        PageId pid{name, it.page};
        Page &page = bp.getPage(pid);
        HeapPage hp(page, td);

        size_t s = it.slot;
        hp.next(s);
        if (s != hp.end()) {
            it.slot = s;
            return;
        }
    }

    // 跨页查找
    for (size_t p = it.page + 1; p < numPages; ++p) {
        PageId pid{name, p};
        Page &page = bp.getPage(pid);
        HeapPage hp(page, td);

        size_t b = hp.begin();
        if (b != hp.end()) {
            it.page = p;
            it.slot = b;
            return;
        }
    }

    it.page = numPages;
    it.slot = 0;
}

Iterator HeapFile::begin() const {
    BufferPool &bp = getDatabase().getBufferPool();
    const TupleDesc &td = getTupleDesc();

    for (size_t p = 0; p < numPages; ++p) {
        PageId pid{name, p};
        Page &page = bp.getPage(pid);
        HeapPage hp(page, td);

        size_t b = hp.begin();
        if (b != hp.end()) {
            return Iterator(*this, p, b);
        }
    }
    return Iterator(*this, numPages, 0);
}

Iterator HeapFile::end() const {
    return Iterator(*this, numPages, 0);
}