#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include <sys/socket.h>
#include <sys/types.h>

#include <easyio/recv_stream.hpp>
#include <easyio/task.hpp>

namespace easyio {

enum class Backend {
    IoUring,
    Libc,
};

std::string_view backend_name(Backend backend) noexcept;

// True iff the binary was built with io_uring support at all (i.e. NOT
// configured with --without-liburing). When false, Backend::IoUring is not
// a valid argument to Queue::create() and callers (e.g. server CLI parsing)
// must not offer it as a choice.
bool io_uring_available() noexcept;

// Sets O_NONBLOCK on fd. Required before using a socket fd with the libc
// backend's connect()/send()/recv_stream() (accept() hands out already-
// nonblocking fds itself); harmless no-op requirement-wise for the
// io_uring backend, which does not need nonblocking fds, so callers that
// want to be backend-agnostic should just always call this once per fd.
void set_nonblocking(int fd);

// Abstract async I/O queue. Only the operations actually needed by the
// easybd server/client are exposed -- this is not a general-purpose port of
// librawio's full API surface.
//
// A Queue is driven by exactly one thread, which must call run() to pump
// completions and resume waiting coroutines; there is no internal thread or
// background reactor. Tasks returned by the methods below only make
// progress while run() is executing on some thread -- typically the same
// thread that owns the Queue and spawned the tasks in the first place.
//
// Coroutines that co_await a Queue operation and throw std::system_error on
// failure (constructed from the errno describing the failure); short reads
// / EOF are represented as a result smaller than requested, matching POSIX
// read() semantics, not as an error.
class Queue {
public:
    static std::unique_ptr<Queue> create(Backend backend, unsigned int depth);

    explicit Queue(unsigned int depth) noexcept : _depth(depth) {}
    virtual ~Queue() = default;

    Queue(const Queue&) = delete;
    Queue& operator=(const Queue&) = delete;

    [[nodiscard]] unsigned int depth() const noexcept { return _depth; }
    [[nodiscard]] virtual Backend backend() const noexcept = 0;

    virtual Task<int> open(const char* path, int flags, mode_t mode) = 0;
    virtual Task<void> close(int fd) = 0;

    virtual Task<size_t> pread(int fd, void* buf, size_t size, uint64_t offset) = 0;
    virtual Task<size_t> pwrite(int fd, const void* buf, size_t size, uint64_t offset) = 0;

    virtual Task<int> accept(int fd) = 0;
    virtual Task<void> connect(int fd, const sockaddr* addr, socklen_t addrlen) = 0;

    virtual Task<size_t> send(int fd, const void* buf, size_t size) = 0;

    // entry_size/entries must be powers of two; total buffer memory used is
    // entry_size * entries. Chunks handed out by the stream reference
    // backend-owned memory that stays valid only until the next next() call
    // completes.
    virtual std::unique_ptr<RecvStream> recv_stream(
        int fd, size_t entry_size, unsigned int entries) = 0;

    // Cancels every pending operation on fd (including a live recv_stream)
    // and arranges for their awaiting coroutines to be resumed with an
    // error. Used when tearing down a connection so its coroutine can
    // unwind before the fd is closed. Does not close fd itself.
    virtual void cancel_fd(int fd) = 0;

    // Pumps completions, resuming coroutines as their operations finish.
    // Blocks the calling thread when there is nothing ready, for at most
    // timeout_ms (a negative value, the default, blocks indefinitely).
    // Returns when stop() has been called, when timeout_ms elapses with
    // nothing to do, or when a signal interrupts the wait -- these three
    // cases are otherwise indistinguishable to the caller by design: a
    // std::atomic<bool> shutdown flag checked between calls, combined with
    // a bounded timeout_ms, is enough to shut a worker thread down
    // promptly without any signal-delivery machinery (pthread_kill and
    // friends) to force a blocking wait to return. Throws on an
    // unrecoverable error.
    virtual void run(int timeout_ms = -1) = 0;

    // Requests that a currently-running (or future) call to run() return
    // once the current batch of ready completions has been dispatched.
    // Safe to call from within a coroutine running on this Queue.
    virtual void stop() = 0;

private:
    unsigned int _depth;
};

// Sends exactly `size` bytes, looping over Queue::send()'s short-write-
// permitting result. Built purely on the public Queue interface, so it's
// backend-agnostic.
Task<void> send_all(Queue& queue, int fd, const void* buf, size_t size);

} // namespace easyio
