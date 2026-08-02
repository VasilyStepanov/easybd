#include <easyio/queue.hpp>

#include <cerrno>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>

#ifdef EASYIO_WITH_LIBURING
#include "uring_queue.hpp"
#endif
#include "libc_queue.hpp"

namespace easyio {

std::string_view backend_name(Backend backend) noexcept {
    switch (backend) {
    case Backend::IoUring:
        return "io_uring";
    case Backend::Libc:
        return "libc";
    }
    return "?";
}

bool io_uring_available() noexcept {
#ifdef EASYIO_WITH_LIBURING
    return true;
#else
    return false;
#endif
}

void set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        throw std::system_error(errno, std::generic_category(), "fcntl(F_GETFL)");
    }
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::system_error(errno, std::generic_category(), "fcntl(F_SETFL)");
    }
}

std::unique_ptr<Queue> Queue::create(Backend backend, unsigned int depth) {
    switch (backend) {
    case Backend::IoUring:
#ifdef EASYIO_WITH_LIBURING
        return std::make_unique<uring::Queue>(depth);
#else
        throw std::invalid_argument(
            "easyio: io_uring backend requested but this build was configured "
            "with --without-liburing");
#endif
    case Backend::Libc:
        return std::make_unique<libc_backend::Queue>(depth);
    }
    throw std::invalid_argument("easyio: unknown backend");
}

Task<void> send_all(Queue& queue, int fd, const void* buf, size_t size) {
    const auto* p = static_cast<const std::byte*>(buf);
    size_t off = 0;
    while (off < size) {
        off += co_await queue.send(fd, p + off, size - off);
    }
}

} // namespace easyio
