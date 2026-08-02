#pragma once

#include <cstddef>
#include <cstdlib>
#include <new>
#include <utility>

namespace easybd {

// Page-aligned heap buffer: the backing file is always opened O_DIRECT, so
// every buffer handed to pread/pwrite must be aligned (and not just the
// offset/size, which the client is responsible for).
class AlignedBuffer {
public:
    explicit AlignedBuffer(size_t size, size_t alignment = 4096) : _size(size) {
        if (size == 0) {
            return;
        }
        if (posix_memalign(&_ptr, alignment, size) != 0) {
            throw std::bad_alloc();
        }
    }

    ~AlignedBuffer() { free(_ptr); }

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    AlignedBuffer(AlignedBuffer&& other) noexcept
        : _ptr(std::exchange(other._ptr, nullptr)), _size(std::exchange(other._size, 0)) {}

    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
        if (this != &other) {
            free(_ptr);
            _ptr = std::exchange(other._ptr, nullptr);
            _size = std::exchange(other._size, 0);
        }
        return *this;
    }

    [[nodiscard]] std::byte* data() noexcept { return static_cast<std::byte*>(_ptr); }
    [[nodiscard]] const std::byte* data() const noexcept {
        return static_cast<const std::byte*>(_ptr);
    }
    [[nodiscard]] size_t size() const noexcept { return _size; }

private:
    void* _ptr = nullptr;
    size_t _size = 0;
};

} // namespace easybd
