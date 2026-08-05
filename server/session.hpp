#pragma once

#include <cstddef>
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
//
// recv_ring_entries/recv_ring_entry_size size this connection's
// recv_stream() -- see ServerConfig's doc comment on why raising them
// matters for the zero-copy write path.
easyio::Task<void> handle_connection(
    easyio::Queue& queue, int client_fd, int file_fd, uint64_t file_size, bool multishot_recv,
    unsigned int recv_ring_entries, size_t recv_ring_entry_size);

// Process-wide tally of how a WRITE/WRITE_SYNC payload was actually
// delivered to the backing file -- see handle_connection's zero-copy
// attempt in session.cpp. `hits` writes went straight out of recv_stream's
// own (io_uring multishot) buffer memory via pwritev/pwritev_dsync, no
// intermediate copy; `pinned_unaligned` successfully pinned that memory but
// declined to use it (the ring's arbitrary internal chunk boundaries didn't
// happen to land on a device-alignment-friendly offset) and copied instead;
// `unavailable` couldn't even attempt pinning (non-multishot backend,
// or a request whose bytes were already entangled with a previous copy).
// Meant purely for observability during benchmarking -- see this phase's
// plan doc for why `hits` is expected to be low under the current wire
// protocol.
struct ZeroCopyStats {
    uint64_t hits = 0;
    uint64_t pinned_unaligned = 0;
    uint64_t unavailable = 0;
};
ZeroCopyStats zero_copy_stats() noexcept;

} // namespace easybd
