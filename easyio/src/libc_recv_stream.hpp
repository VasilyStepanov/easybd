#pragma once

#include <memory>

#include <easyio/recv_stream.hpp>

#include "libc_queue.hpp"

namespace easyio::libc_backend {

std::unique_ptr<RecvStream> make_recv_stream(
    Queue& queue, int fd, size_t entry_size, unsigned int entries);

} // namespace easyio::libc_backend
