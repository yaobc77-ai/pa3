#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstring>
#include <cerrno>

// ================= 平台兼容性补丁 (Windows Fix) =================
#ifdef _WIN32
    #include <io.h>
    #include <windows.h>

    // 注意：不再定义 ssize_t，避免与系统库冲突
    // Windows 下 open 的标志位需要手动定义
    #ifndef O_BINARY
    #define O_BINARY 0
    #endif

    // 模拟 pread: Windows 的 _read 返回 int
    int pread(int fd, void *buf, size_t count, off_t offset) {
        if (_lseek(fd, offset, SEEK_SET) == -1) {
            return -1;
        }
        return _read(fd, buf, static_cast<unsigned int>(count));
    }

    // 模拟 pwrite: Windows 的 _write 返回 int
    int pwrite(int fd, const void *buf, size_t count, off_t offset) {
        if (_lseek(fd, offset, SEEK_SET) == -1) {
            return -1;
        }
        return _write(fd, buf, static_cast<unsigned int>(count));
    }

#else
    // 非 Windows 平台 (Linux/macOS) 直接引用 unistd.h
    #include <unistd.h>
#endif
// ==============================================================

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
    // Windows 下必须使用 O_BINARY 防止换行符转换
#ifdef _WIN32
    int flags = O_RDWR | O_CREAT | O_BINARY;
    int mode = _S_IREAD | _S_IWRITE;
    fd = ::_open(name.c_str(), flags, mode);
#else
    int flags = O_RDWR | O_CREAT;
    fd = ::open(name.c_str(), flags, 0644);
#endif

    if (fd < 0) {
        throw std::runtime_error("Failed to open file " + name + ": " + std::strerror(errno));
    }

    // 获取文件大小
    struct stat st{};
    if (::fstat(fd, &st) < 0) {
#ifdef _WIN32
        ::_close(fd);
#else
        ::close(fd);
#endif
        throw std::runtime_error("Failed to fstat file " + name + ": " + std::strerror(errno));
    }

    // 计算页数
    numPages = static_cast<size_t>(st.st_size) / DEFAULT_PAGE_SIZE;
}

DbFile::~DbFile() {
    if (fd >= 0) {
#ifdef _WIN32
        ::_close(fd);
#else
        ::close(fd);
#endif
    }
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
    std::lock_guard<std::mutex> lock(io_mtx);
    reads.push_back(id);

    // 计算偏移量
    off_t offset = static_cast<off_t>(id * DEFAULT_PAGE_SIZE);

    // 关键修改：使用 auto 接收返回值，避免显式使用 ssize_t
    auto bytes_read = ::pread(fd, page.data(), DEFAULT_PAGE_SIZE, offset);

    if (bytes_read < 0) {
        throw std::runtime_error("pread failed: " + std::string(std::strerror(errno)));
    }

    // 强转比较，安全处理不足一页的情况
    if (static_cast<size_t>(bytes_read) < DEFAULT_PAGE_SIZE) {
        std::memset(page.data() + bytes_read, 0, DEFAULT_PAGE_SIZE - bytes_read);
    }
}

void DbFile::writePage(const Page &page, const size_t id) const {
    std::lock_guard<std::mutex> lock(io_mtx);
    writes.push_back(id);

    off_t offset = static_cast<off_t>(id * DEFAULT_PAGE_SIZE);

    // 关键修改：使用 auto 接收返回值
    auto bytes_written = ::pwrite(fd, page.data(), DEFAULT_PAGE_SIZE, offset);

    if (bytes_written < 0) {
        throw std::runtime_error("pwrite failed: " + std::string(std::strerror(errno)));
    }

    if (static_cast<size_t>(bytes_written) != DEFAULT_PAGE_SIZE) {
        throw std::runtime_error("pwrite incomplete write");
    }
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }
const std::vector<size_t> &DbFile::getWrites() const { return writes; }

// 基类虚函数默认实现
void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented in DbFile"); }
void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented in DbFile"); }
Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented in DbFile"); }
void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented in DbFile"); }
Iterator DbFile::begin() const { throw std::runtime_error("Not implemented in DbFile"); }
Iterator DbFile::end() const { throw std::runtime_error("Not implemented in DbFile"); }

size_t DbFile::getNumPages() const { return numPages; }

// 解决 Clang/MSVC 警告的运算符重载
auto DbFile::operator<=>(size_t size) const { return numPages <=> size; }
bool DbFile::operator==(int i) const { return numPages == static_cast<size_t>(i); }
size_t DbFile::operator-(int i) const { return numPages - i; }