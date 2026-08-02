#pragma once

#include <memory>

#include <easyio/recv_stream.hpp>

#include "libc_queue.hpp"

namespace easyio::libc_backend {

// multishot is accepted for interface symmetry with the io_uring backend
// and always ignored: this backend's poll()-driven ::recv() loop already
// is the "ordinary recv" behavior, there's no separate multishot mode to
// switch away from.
std::unique_ptr<RecvStream> make_recv_stream(
    Queue& queue, int fd, size_t entry_size, unsigned int entries, bool multishot);

} // namespace easyio::libc_backend
