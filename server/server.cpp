#include "server.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <exception>
#include <mutex>
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

// Set by worker_main() if a worker thread fails before/during its normal
// step() loop (e.g. Queue::create() -- io_uring_queue_init() -- failing on
// some environments/threads but not others). An exception escaping a
// std::thread's entry function is entirely uncaught -- std::terminate(),
// no ifs or buts -- so worker_main() catches it itself, stashes it here,
// and flips g_shutdown_requested so every other worker unwinds cleanly;
// run_server() then rethrows it after joining, turning what would
// otherwise be a SIGABRT/coredump into the same clean
// "easybd-server: <what>" + exit(1) path main() already uses for any
// other startup failure. First failure wins if more than one thread hits
// this near-simultaneously; the rest are equivalent/redundant anyway.
std::mutex g_worker_exception_mutex;
std::exception_ptr g_worker_exception;

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

// direct_io=false exists for benchmarking/diagnostic purposes -- e.g.
// isolating whether a given result is actually about O_DIRECT bypassing
// the page cache, or about something else entirely (RWF_DSYNC's io-wq
// hand-off, a filesystem's own caching behavior, etc.) by comparing the
// same backend/workload with and without it. Real deployments should
// leave this at the default (true): without O_DIRECT, reads/writes go
// through the page cache like an ordinary buffered file, which quietly
// changes what's actually being measured/served.
int open_backing_file(const std::string& path, uint64_t& out_size, bool direct_io) {
    int flags = O_RDWR | (direct_io ? O_DIRECT : 0);
    int fd = ::open(path.c_str(), flags);
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
    easyio::Queue& queue, int client_fd, int file_fd, uint64_t file_size, bool multishot_recv) {
    try {
        co_await handle_connection(queue, client_fd, file_fd, file_size, multishot_recv);
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
    easyio::Queue& queue, int listen_fd, int file_fd, uint64_t file_size, bool multishot_recv) {
    for (;;) {
        int client_fd = 0;
        try {
            client_fd = co_await queue.accept(listen_fd);
        } catch (...) {
            // accept(2)'s BUGS section: a peer that resets a connection
            // after the kernel completes the handshake but before this
            // thread gets around to calling accept4() surfaces as an
            // error here (ECONNABORTED, and in practice sometimes
            // ECONNRESET/EPROTO/ENETDOWN/... depending on the network
            // stack) -- portable code is expected to just try again, not
            // treat it as the listen socket itself dying. The same applies
            // to any OTHER exception this doesn't specifically catalogue:
            // guessing "is this fatal" from an exception type/errno and
            // getting it wrong doesn't just drop the one connection --
            // since nothing else ever re-arms accept() on this thread's
            // listen_fd, it silently and permanently stops this worker
            // from accepting *any* future connection, while the kernel
            // keeps completing TCP handshakes into the backlog regardless
            // -- callers see their connect() succeed and then hang
            // forever with no application on the other end ever reading
            // anything (found via a stuck benchmark connection with
            // unread bytes in Recv-Q, and gdb confirming every worker
            // thread's poll() set had gone completely empty -- not even
            // listen_fd was still registered on any of them). So instead
            // of trying to classify the exception, ask the one question
            // that actually matters: has a real shutdown been requested?
            // If not, this is retried unconditionally, no matter what it
            // was. g_shutdown_requested flipping true is what run_server()
            // uses right before it closes listen_fd out from under every
            // worker -- checking it directly is a strictly more reliable
            // signal than trying to infer the same fact from whatever
            // errno that close happens to surface as here (EBADF/EINVAL in
            // practice, but that's an implementation detail of what
            // accept4() on a just-closed fd happens to return, not a
            // contract).
            if (g_shutdown_requested.load(std::memory_order_relaxed)) {
                co_return;
            }
            continue;
        }
        easyio::spawn(accept_one(queue, client_fd, file_fd, file_size, multishot_recv));
    }
}

void worker_main(
    easyio::Backend backend, unsigned int depth, int listen_fd, int file_fd, uint64_t file_size,
    bool multishot_recv) {
    try {
        auto queue = easyio::Queue::create(backend, depth);
        easyio::spawn(accept_loop(*queue, listen_fd, file_fd, file_size, multishot_recv));
        // The pending accept (and any in-flight request) stays registered
        // across these calls -- a timed-out step() just means "nothing
        // happened in the last kShutdownPollTimeoutMs," not that anything
        // gets resubmitted or lost.
        while (!g_shutdown_requested.load(std::memory_order_relaxed)) {
            queue->step(kShutdownPollTimeoutMs);
        }
    } catch (...) {
        {
            std::lock_guard<std::mutex> lock(g_worker_exception_mutex);
            if (!g_worker_exception) {
                g_worker_exception = std::current_exception();
            }
        }
        g_shutdown_requested.store(true, std::memory_order_relaxed);
    }
}

} // namespace

void run_server(const ServerConfig& config) {
    uint64_t file_size = 0;
    int file_fd = open_backing_file(config.file_path, file_size, config.direct_io);
    int listen_fd = open_listen_socket(config.bind_host, config.bind_port);

    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);

    std::vector<std::thread> workers;
    workers.reserve(config.threads);
    for (unsigned int i = 0; i < config.threads; ++i) {
        workers.emplace_back(
            worker_main, config.backend, config.queue_depth, listen_fd, file_fd, file_size,
            config.multishot_recv);
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

    if (g_worker_exception) {
        std::rethrow_exception(g_worker_exception);
    }
}

} // namespace easybd
