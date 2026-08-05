#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include <easyio/queue.hpp>

namespace easybd {

struct ServerConfig {
    std::string bind_host;
    uint16_t bind_port = 0;
    std::string file_path;
    easyio::Backend backend = easyio::Backend::Libc;
    unsigned int queue_depth = 128;
    unsigned int threads = 1;
    // See --feature-multishot in main.cpp. Only meaningful for the
    // io_uring backend; the libc backend ignores it (it has no multishot
    // recv to begin with).
    bool multishot_recv = false;
    // recv_stream() ring sizing -- see kRecvEntrySize/kRecvEntries's doc
    // comment (formerly in session.cpp, now just these defaults) for why
    // the ring is deliberately tiny by default. Raising --recv-ring-entries
    // matters specifically when testing the zero-copy write path (see
    // handle_write_zerocopy in session.cpp): a payload pinned until its
    // disk write completes ties up its ring slot(s) far longer than the
    // default sizing assumes, so sustaining pipelining under zero-copy at a
    // given iodepth needs a ring sized to cover that iodepth's worst-case
    // in-flight payload bytes, not just the ordinary-copy path's cache-
    // locality sweet spot.
    unsigned int recv_ring_entries = 4;
    size_t recv_ring_entry_size = 1u << 17; // 128 KiB
    // See --direct in main.cpp. Whether the backing file is opened
    // O_DIRECT (default) or not -- see open_backing_file()'s doc comment
    // for why this is sometimes useful to turn off.
    bool direct_io = true;
};

// Binds/listens, opens the backing file (O_DIRECT unless config.direct_io
// is false), spawns `threads` worker threads (each owning an independent
// Queue, all accept()ing off the same listening socket), and blocks the
// calling thread until SIGINT/SIGTERM.
void run_server(const ServerConfig& config);

} // namespace easybd
