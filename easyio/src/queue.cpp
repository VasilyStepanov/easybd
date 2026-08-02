#include <easyio/queue.hpp>

#include <cerrno>
#include <stdexcept>
#include <system_error>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

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

void set_tcp_nodelay(int fd) {
    int one = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        throw std::system_error(errno, std::generic_category(), "setsockopt(TCP_NODELAY)");
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

Task<void> send_all(
    Queue& queue, int fd, const void* buf1, size_t size1, const void* buf2, size_t size2) {
    iovec iov[2] = {
        {const_cast<void*>(buf1), size1},
        {const_cast<void*>(buf2), size2},
    };
    size_t total = size1 + size2;
    size_t sent = 0;
    int first = 0;
    while (sent < total) {
        size_t n = co_await queue.sendmsg(fd, iov + first, 2 - first);
        sent += n;
        while (n > 0) {
            if (n < iov[first].iov_len) {
                iov[first].iov_base = static_cast<std::byte*>(iov[first].iov_base) + n;
                iov[first].iov_len -= n;
                n = 0;
            } else {
                n -= iov[first].iov_len;
                ++first;
            }
        }
    }
}

} // namespace easyio
