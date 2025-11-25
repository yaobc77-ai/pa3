#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <cstring>

using namespace db;


HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
    const size_t P = DEFAULT_PAGE_SIZE;
    const size_t T = td.length();

    // 关键：考虑到每条记录需要 1 个头位(bit)
    capacity = (8 * P) / (8 * T + 1);

    // 头区字节数
    const size_t headerBytes = (capacity + 7) / 8;

    // 直接在页缓冲里定位头区和数据区（无额外分配）
    header = page.data();                  // 前 headerBytes 字节是头
    data   = page.data() + headerBytes;    // 后面紧跟数据
}


size_t HeapPage::begin() const {
    for (size_t i = 0; i < capacity; ++i) {
        const size_t  byte = i >> 3;
        const uint8_t mask = static_cast<uint8_t>(0x80u >> (i & 7)); // MSB-first
        if (header[byte] & mask) return i;
    }
    return capacity;
}

size_t HeapPage::end() const {
    return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
    for (size_t i = 0; i < capacity; ++i) {
        const size_t  byte = i >> 3;
        const uint8_t mask = static_cast<uint8_t>(0x80u >> (i & 7));
        if ((header[byte] & mask) == 0) {              // 空位
            td.serialize(data + i * td.length(), t);   // 写入
            header[byte] |= mask;                      // 置位
            return true;
        }
    }
    return false; // 满页
}

void HeapPage::deleteTuple(size_t slot) {
    if (slot >= capacity) throw std::out_of_range("slot OOB");
    const size_t  byte = slot >> 3;
    const uint8_t mask = static_cast<uint8_t>(0x80u >> (slot & 7));
    if ((header[byte] & mask) == 0) throw std::logic_error("slot empty");
    std::memset(data + slot * td.length(), 0, td.length());
    header[byte] &= ~mask;
}

Tuple HeapPage::getTuple(size_t slot) const {
    if (slot >= capacity) throw std::out_of_range("slot OOB");
    const size_t  byte = slot >> 3;
    const uint8_t mask = static_cast<uint8_t>(0x80u >> (slot & 7));
    if ((header[byte] & mask) == 0) throw std::logic_error("slot empty");
    return td.deserialize(data + slot * td.length());
}

bool HeapPage::empty(size_t slot) const {
    if (slot >= capacity) return true;
    const size_t  byte = slot >> 3;
    const uint8_t mask = static_cast<uint8_t>(0x80u >> (slot & 7));
    return (header[byte] & mask) == 0;
}

void HeapPage::next(size_t &slot) const {
    if (slot >= capacity) { slot = capacity; return; }
    for (size_t i = slot + 1; i < capacity; ++i) {
        const size_t  byte = i >> 3;
        const uint8_t mask = static_cast<uint8_t>(0x80u >> (i & 7));
        if (header[byte] & mask) { slot = i; return; }
    }
    slot = capacity;
}
