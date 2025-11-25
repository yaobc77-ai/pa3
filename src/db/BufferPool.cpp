#include <db/BufferPool.hpp>
#include <db/Database.hpp>
#include <numeric>
#include <vector>

using namespace db;

BufferPool::BufferPool()
    : pid_to_pos(),
      available(DEFAULT_NUM_PAGES)
{
    std::iota(available.rbegin(), available.rend(), 0);
}

BufferPool::~BufferPool() {
    std::vector<PageId> to_flush;
    to_flush.reserve(dirty.size());
    for (size_t pos : dirty) {
        const PageId &pid = pos_to_pid[pos];
        if (!pid.file.empty()) {
            to_flush.push_back(pid);
        }
    }
    for (const auto &pid : to_flush) {
        flushPage(pid);
    }
}

Page &BufferPool::getPage(const PageId &pid) {
    if (contains(pid)) {
        auto it = pid_to_pos.find(pid);
        size_t pos = it->second;
        auto lit = pos_to_lru.find(pos);
        if (lit != pos_to_lru.end()) {
            lru_list.splice(lru_list.begin(), lru_list, lit->second);
            lit->second = lru_list.begin();
        }
        return pages[pos];
    }

    if (available.empty()) {
        size_t pos = lru_list.back();
        PageId old_pid = pos_to_pid[pos];
        if (!old_pid.file.empty()) {
            if (isDirty(old_pid)) {
                flushPage(old_pid);
            }
            discardPage(old_pid);
        }
    }

    size_t pos = available.back();
    available.pop_back();

    Page &page = pages[pos];
    getDatabase().get(pid.file).readPage(page, pid.page);

    pid_to_pos[pid] = pos;
    pos_to_pid[pos] = pid;

    lru_list.push_front(pos);
    pos_to_lru[pos] = lru_list.begin();

    return page;
}

void BufferPool::markDirty(const PageId &pid) {
    auto it = pid_to_pos.find(pid);
    if (it == pid_to_pos.end()) {
        return;
    }
    size_t pos = it->second;
    dirty.insert(pos);
}

bool BufferPool::isDirty(const PageId &pid) const {
    auto it = pid_to_pos.find(pid);
    if (it == pid_to_pos.end()) {
        return false;
    }
    size_t pos = it->second;
    return dirty.contains(pos);
}

bool BufferPool::contains(const PageId &pid) const {
    return pid_to_pos.contains(pid);
}

void BufferPool::discardPage(const PageId &pid) {
    auto it = pid_to_pos.find(pid);
    if (it == pid_to_pos.end()) {
        return;
    }
    size_t pos = it->second;
    pid_to_pos.erase(it);

    pos_to_pid[pos] = PageId{};

    auto lit = pos_to_lru.find(pos);
    if (lit != pos_to_lru.end()) {
        lru_list.erase(lit->second);
        pos_to_lru.erase(lit);
    }

    dirty.erase(pos);
    available.push_back(pos);
}

void BufferPool::flushPage(const PageId &pid) {
    auto it = pid_to_pos.find(pid);
    if (it == pid_to_pos.end()) {
        return;
    }
    size_t pos = it->second;
    if (dirty.erase(pos) == 0) {
        return;
    }
    const Page &page = pages[pos];
    getDatabase().get(pid.file).writePage(page, pid.page);
}

void BufferPool::flushFile(const std::string &file) {
    std::vector<PageId> to_flush;
    to_flush.reserve(dirty.size());
    for (size_t pos : dirty) {
        const PageId &pid = pos_to_pid[pos];
        if (pid.file == file) {
            to_flush.push_back(pid);
        }
    }
    for (const auto &pid : to_flush) {
        flushPage(pid);
    }
}
