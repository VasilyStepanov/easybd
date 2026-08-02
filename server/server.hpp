#pragma once

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
};

// Binds/listens, opens the backing file O_DIRECT, spawns `threads` worker
// threads (each owning an independent Queue, all accept()ing off the same
// listening socket), and blocks the calling thread until SIGINT/SIGTERM.
void run_server(const ServerConfig& config);

} // namespace easybd
