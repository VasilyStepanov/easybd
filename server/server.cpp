#include "server.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <system_error>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "session.hpp"

namespace easybd {

namespace {

// How often each worker's blocking step() wakes up on its own (with nothing
// to do) to check g_shutdown_requested. Bounds shutdown latency; this is
// the only thing that makes threads shut down promptly without needing any
// signal-delivery machinery (pthread_kill and friends) to force a blocking
// wait to return early.
constexpr int kShutdownPollTimeoutMs = 200;

std::atomic<bool> g_shutdown_requested{false};

// Deliberately minimal: just flips a flag. Every worker thread (and main)
// polls it via the timeout on Queue::step()/a sleep loop, rather than the
// signal needing to reach or interrupt any thread in particular.
extern "C" void handle_shutdown_signal(int /*sig*/) {
    g_shutdown_requested.store(true, std::memory_order_relaxed);
}

void throw_errno(int err, const char* what) {
    throw std::system_error(err, std::generic_category(), what);
}

int open_listen_socket(const std::string& host, uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw_errno(errno, "socket");
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        throw std::invalid_argument("easybd: invalid bind address '" + host + "'");
    }

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        int err = errno;
        ::close(fd);
        throw_errno(err, "bind");
    }
    if (::listen(fd, 256) < 0) {
        int err = errno;
        ::close(fd);
        throw_errno(err, "listen");
    }
    // Required for the libc backend: a blocking listen fd makes accept4()
    // block the whole worker thread waiting for the next connection
    // instead of returning EAGAIN, which starves queue->step() of any
    // chance to service already-accepted connections (their sends/recvs
    // never get pumped) until a new client happens to show up. Harmless
    // for the io_uring backend, which doesn't need nonblocking fds.
    easyio::set_nonblocking(fd);
    return fd;
}

int open_backing_file(const std::string& path, uint64_t& out_size) {
    int fd = ::open(path.c_str(), O_RDWR | O_DIRECT);
    if (fd < 0) {
        throw_errno(errno, ("open backing file '" + path + "'").c_str());
    }
    struct stat st{};
    if (::fstat(fd, &st) < 0) {
        int err = errno;
        ::close(fd);
        throw_errno(err, "fstat backing file");
    }
    out_size = static_cast<uint64_t>(st.st_size);
    return fd;
}

// One per accepted connection: drives handle_connection() to completion
// (swallowing whatever it propagates -- an ended/broken connection is not
// a server-level error) and closes the fd once every in-flight request on
// it has finished.
easyio::Task<void> accept_one(
    easyio::Queue& queue, int client_fd, int file_fd, uint64_t file_size) {
    try {
        co_await handle_connection(queue, client_fd, file_fd, file_size);
    } catch (...) { // NOLINT(bugprone-empty-catch): deliberate, see comment above
    }
    try {
        co_await queue.close(client_fd);
    } catch (...) { // NOLINT(bugprone-empty-catch): fd is already unusable either way
    }
}

// Accepts connections off the shared listening socket for the lifetime of
// this worker's queue. Several worker threads each run their own instance
// of this loop against the *same* listen_fd; the kernel distributes
// incoming connections across whichever threads have a pending accept.
easyio::Task<void> accept_loop(
    easyio::Queue& queue, int listen_fd, int file_fd, uint64_t file_size) {
    for (;;) {
        int client_fd = 0;
        try {
            client_fd = co_await queue.accept(listen_fd);
        } catch (...) {
            // Listen socket closed (shutdown) or an unrecoverable accept
            // error -- either way, this worker stops accepting.
            co_return;
        }
        easyio::spawn(accept_one(queue, client_fd, file_fd, file_size));
    }
}

void worker_main(
    easyio::Backend backend, unsigned int depth, int listen_fd, int file_fd, uint64_t file_size) {
    auto queue = easyio::Queue::create(backend, depth);
    easyio::spawn(accept_loop(*queue, listen_fd, file_fd, file_size));
    // The pending accept (and any in-flight request) stays registered
    // across these calls -- a timed-out step() just means "nothing happened
    // in the last kShutdownPollTimeoutMs," not that anything gets
    // resubmitted or lost.
    while (!g_shutdown_requested.load(std::memory_order_relaxed)) {
        queue->step(kShutdownPollTimeoutMs);
    }
}

} // namespace

void run_server(const ServerConfig& config) {
    uint64_t file_size = 0;
    int file_fd = open_backing_file(config.file_path, file_size);
    int listen_fd = open_listen_socket(config.bind_host, config.bind_port);

    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);

    std::vector<std::thread> workers;
    workers.reserve(config.threads);
    for (unsigned int i = 0; i < config.threads; ++i) {
        workers.emplace_back(worker_main, config.backend, config.queue_depth, listen_fd, file_fd, file_size);
    }

    while (!g_shutdown_requested.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownPollTimeoutMs));
    }
    std::fprintf(stderr, "easybd-server: shutting down\n");

    for (auto& t : workers) {
        t.join();
    }

    ::close(listen_fd);
    ::close(file_fd);
}

} // namespace easybd
