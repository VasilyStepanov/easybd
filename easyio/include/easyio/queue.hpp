#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>

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

// Disables Nagle's algorithm on a TCP socket. Without this, a request/
// response protocol that sends a header and payload as separate writes
// stalls for the platform's delayed-ACK timeout (~40ms on Linux) on every
// round trip: Nagle holds back the trailing small segment waiting for an
// ACK of the previous one, and the peer has nothing to piggyback that ACK
// on until it has read the full request, so nothing moves until the
// timer fires. Callers on both ends of a low-latency connection must set
// this.
void set_tcp_nodelay(int fd);

// Abstract async I/O queue. Only the operations actually needed by the
// easybd server/client are exposed -- this is not a general-purpose port of
// librawio's full API surface.
//
// A Queue is driven by exactly one thread, which must call step() to pump
// completions and resume waiting coroutines; there is no internal thread or
// background reactor. Tasks returned by the methods below only make
// progress while step() is executing on some thread -- typically the same
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

    // Like pwrite(), but the write isn't complete until the data has been
    // flushed to the storage device (RWF_DSYNC, i.e. per-write O_DSYNC) --
    // one syscall/SQE that both writes and durably commits, instead of a
    // pwrite() followed by a separate fdatasync() (two round trips, and on
    // io_uring two SQEs/CQEs). A pwrite() completing is not by itself a
    // durability guarantee: O_DIRECT only bypasses the page cache, it says
    // nothing about a volatile write cache the underlying device may still
    // have, which RWF_DSYNC's implicit flush/FUA is what actually forces.
    virtual Task<size_t> pwrite_dsync(int fd, const void* buf, size_t size, uint64_t offset) = 0;

    // Vectored counterparts of pwrite()/pwrite_dsync(): writes iov[0..iovcnt)
    // as a single kernel operation (::pwritev / IORING_OP_WRITEV), for a
    // payload that's scattered across several source buffers instead of one
    // contiguous one -- e.g. writing straight out of several io_uring
    // provided-buffer-ring chunks without first gathering them into one
    // buffer. With O_DIRECT, every iov's base must be device-alignment-
    // aligned, and every iov but the last must also have an aligned length
    // (same constraint a single pwrite()'s buf/size already have to satisfy,
    // just per-segment now).
    virtual Task<size_t> pwritev(int fd, const iovec* iov, int iovcnt, uint64_t offset) = 0;
    virtual Task<size_t> pwritev_dsync(int fd, const iovec* iov, int iovcnt, uint64_t offset) = 0;

    virtual Task<int> accept(int fd) = 0;
    virtual Task<void> connect(int fd, const sockaddr* addr, socklen_t addrlen) = 0;

    virtual Task<size_t> send(int fd, const void* buf, size_t size) = 0;

    // Gathered send: writes iov[0..iovcnt) as a single kernel operation
    // (::sendmsg / IORING_OP_SENDMSG) instead of one send() per buffer, so a
    // header-then-payload message costs one syscall/SQE round trip, not two.
    virtual Task<size_t> sendmsg(int fd, const iovec* iov, int iovcnt) = 0;

    // entry_size/entries must be powers of two; total buffer memory used is
    // entry_size * entries. Chunks handed out by the stream reference
    // backend-owned memory that stays valid only until the next next() call
    // completes.
    //
    // multishot selects between the io_uring backend's two recv strategies:
    // true (the default) is one multishot recv op + a provided buffer ring
    // (see RecvStream's own doc comment); false is one plain recv op in
    // flight at a time, resubmitted per chunk -- the "ordinary recv"
    // baseline multishot exists to avoid, kept as an option so the two can
    // be benchmarked against each other (see --feature-multishot on the
    // server/client CLIs). The libc backend has no multishot concept to
    // begin with -- it already always behaves like the false case here --
    // so it accepts and ignores this parameter.
    virtual std::unique_ptr<RecvStream> recv_stream(
        int fd, size_t entry_size, unsigned int entries, bool multishot = true) = 0;

    // Cancels every pending operation on fd (including a live recv_stream)
    // and arranges for their awaiting coroutines to be resumed with an
    // error. Used when tearing down a connection so its coroutine can
    // unwind before the fd is closed. Does not close fd itself.
    virtual void cancel_fd(int fd) = 0;

    // Waits for at least one completion (blocking the calling thread for at
    // most timeout_ms; a negative value, the default, blocks indefinitely),
    // resumes whichever coroutines just became ready, and returns -- this
    // is exactly ONE wait-then-dispatch cycle, never more, regardless of
    // whether more work is immediately available. Also returns (having
    // dispatched nothing) if timeout_ms elapses with nothing ready, or if a
    // signal interrupts the wait.
    //
    // This is deliberately not "pump until told to stop": a caller that
    // wants to keep going just calls step() again -- e.g. a server worker's
    // `while (!shutdown) queue->step(200);`, or Client::wait()'s single
    // `queue->step(timeout_ms)` per call, matching fio's getevents()
    // contract of "process what's ready, report how much, return". Looping
    // *inside* step() until nothing is left could never return at all in
    // that last case: once nothing further is pending, step(-1) would
    // block forever waiting for a next event that isn't coming, even
    // though the caller only asked to wait for "at least one".
    //
    // Throws on an unrecoverable error.
    virtual void step(int timeout_ms = -1) = 0;

private:
    unsigned int _depth;
};

// Sends exactly `size` bytes, looping over Queue::send()'s short-write-
// permitting result. Built purely on the public Queue interface, so it's
// backend-agnostic.
Task<void> send_all(Queue& queue, int fd, const void* buf, size_t size);

// Sends exactly buf1[0..size1) followed by buf2[0..size2) as a single
// gathered write (see Queue::sendmsg()), looping over short writes the same
// way send_all() does. Used for header+payload messages so the two logical
// pieces cost one syscall/SQE round trip instead of two.
Task<void> send_all(
    Queue& queue, int fd, const void* buf1, size_t size1, const void* buf2, size_t size2);

} // namespace easyio
