#pragma once

#include <cstdint>

#include <easyio/mutex.hpp>
#include <easyio/queue.hpp>
#include <easyio/task.hpp>

namespace easybd {

// Owns everything one accepted client connection needs: reads requests off
// the wire, spawns an independent handler per request (so several requests
// can be in flight at once against the backing file, which is the entire
// point of iodepth pipelining), and serializes response writes so two
// concurrently-finishing handlers can never interleave their bytes on the
// wire (see easyio::Mutex's doc comment).
//
// One reader coroutine parses the byte stream; each parsed request becomes
// a detached (spawn()ed) task so the reader can immediately go back to
// parsing the next request without waiting for this one's file I/O.
easyio::Task<void> handle_connection(
    easyio::Queue& queue, int client_fd, int file_fd, uint64_t file_size);

} // namespace easybd
