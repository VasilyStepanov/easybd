#pragma once

#include <memory>

#include <easyio/recv_stream.hpp>

#include "uring_queue.hpp"

namespace easyio::uring {

std::unique_ptr<RecvStream> make_recv_stream(
    Queue& queue, int fd, size_t entry_size, unsigned int entries, bool multishot);

} // namespace easyio::uring
