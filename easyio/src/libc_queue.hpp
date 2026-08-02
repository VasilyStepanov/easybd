#pragma once

#include <deque>
#include <unordered_map>

#include <easyio/queue.hpp>

namespace easyio::libc_backend {

// A pending operation waiting for POLLIN/POLLOUT readiness on some fd.
class FdOp {
public:
    // Called when the fd is ready in this op's direction. Returns true if
    // the op is finished and should be deregistered (one-shot ops always
    // return true once they get a real result; a multishot recv stream
    // returns false forever, since it never "finishes" via this signal).
    virtual bool on_ready() noexcept = 0;

    // Forcibly finishes the op with an error, e.g. when its fd is torn down
    // while the op is still pending.
    virtual void cancel() noexcept = 0;

protected:
    ~FdOp() = default;
};

// poll()-based backend: real non-blocking waiting for sockets (accept,
// connect, send, recv), plain synchronous syscalls for regular-file I/O
// (open/close/pread/pwrite -- a regular file is always "ready" per POSIX
// poll semantics, so there is nothing to wait for; the read/write syscall
// itself is what takes the time, same as it would on any thread).
class Queue final : public easyio::Queue {
public:
    explicit Queue(unsigned int depth);
    ~Queue() override;

    [[nodiscard]] Backend backend() const noexcept override { return Backend::Libc; }

    Task<int> open(const char* path, int flags, mode_t mode) override;
    Task<void> close(int fd) override;
    Task<size_t> pread(int fd, void* buf, size_t size, uint64_t offset) override;
    Task<size_t> pwrite(int fd, const void* buf, size_t size, uint64_t offset) override;
    Task<size_t> pwrite_dsync(int fd, const void* buf, size_t size, uint64_t offset) override;
    Task<int> accept(int fd) override;
    Task<void> connect(int fd, const sockaddr* addr, socklen_t addrlen) override;
    Task<size_t> send(int fd, const void* buf, size_t size) override;
    Task<size_t> sendmsg(int fd, const iovec* iov, int iovcnt) override;
    std::unique_ptr<RecvStream> recv_stream(
        int fd, size_t entry_size, unsigned int entries, bool multishot) override;
    void cancel_fd(int fd) override;
    void step(int timeout_ms = -1) override;

    // Internal API for the AcceptAwaiter/ConnectAwaiter/SendAwaiter/
    // RecvStreamImpl helper classes defined in the .cpp files (they live in
    // anonymous namespaces there, so a friend declaration here can't name
    // them -- this type itself is already an implementation-private class,
    // not part of easyio's public API, so exposing these publicly costs
    // nothing).
    void _set_read_op(int fd, FdOp* op);
    void _clear_read_op(int fd, FdOp* op) noexcept;
    void _push_write_op(int fd, FdOp* op);

private:
    struct FdState {
        FdOp* read_op = nullptr;
        std::deque<FdOp*> write_queue;
        [[nodiscard]] bool empty() const noexcept { return !read_op && write_queue.empty(); }
    };

    std::unordered_map<int, FdState> _fds;
};

} // namespace easyio::libc_backend
