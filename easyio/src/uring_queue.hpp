#pragma once

#include <liburing.h>

#include <easyio/queue.hpp>

namespace easyio::uring {

// Common completion sink so the dispatch loop can resume a plain one-shot
// awaiter (SqeAwaiter) or feed a multishot recv stream, without the two
// concepts otherwise having anything to do with each other.
class CqeSink {
public:
    virtual void on_cqe(int res, unsigned int flags) noexcept = 0;

protected:
    ~CqeSink() = default;
};

class Queue final : public easyio::Queue {
public:
    explicit Queue(unsigned int depth);
    ~Queue() override;

    [[nodiscard]] Backend backend() const noexcept override { return Backend::IoUring; }

    Task<int> open(const char* path, int flags, mode_t mode) override;
    Task<void> close(int fd) override;
    Task<size_t> pread(int fd, void* buf, size_t size, uint64_t offset) override;
    Task<size_t> pwrite(int fd, const void* buf, size_t size, uint64_t offset) override;
    Task<size_t> pwrite_dsync(int fd, const void* buf, size_t size, uint64_t offset) override;
    Task<size_t> pwritev(int fd, const iovec* iov, int iovcnt, uint64_t offset) override;
    Task<size_t> pwritev_dsync(int fd, const iovec* iov, int iovcnt, uint64_t offset) override;
    Task<int> accept(int fd) override;
    Task<void> connect(int fd, const sockaddr* addr, socklen_t addrlen) override;
    Task<size_t> send(int fd, const void* buf, size_t size) override;
    Task<size_t> sendmsg(int fd, const iovec* iov, int iovcnt) override;
    std::unique_ptr<RecvStream> recv_stream(
        int fd, size_t entry_size, unsigned int entries, bool multishot) override;
    void cancel_fd(int fd) override;
    void step(int timeout_ms = -1) override;

    // Internal API for the SqeAwaiter/RecvStreamImpl helper classes defined
    // in the .cpp files (they live in anonymous namespaces there, so a
    // friend declaration here can't name them -- this type itself is
    // already an implementation-private class, not part of easyio's public
    // API, so exposing these publicly costs nothing).
    io_uring_sqe* _get_sqe();
    io_uring& _ring_handle() noexcept { return _ring; }

private:
    void _dispatch();

    // Lazily registers fd as this ring's single fixed file (index 0) on
    // first use, so pread/pwrite/pwrite_dsync can pass IOSQE_FIXED_FILE and
    // skip the kernel's per-submission fdget/fdput on the backing file --
    // cheap (fixed file lookup is a plain array index) but only actually
    // saves that one refcount op per op, not the io-wq worker-thread hand-
    // off that O_DIRECT writes still need on this hardware regardless.
    // Returns whether fd is now (or already was) the registered fixed
    // file. Our only real caller (session.cpp) always passes the same
    // backing-file fd for a Queue's whole lifetime, so registration happens
    // at most once per Queue; a second, different fd here just falls back
    // to plain (non-fixed) I/O rather than trying to re-register.
    bool _use_fixed_file(int fd) noexcept;

    io_uring _ring{};
    int _registered_fd = -1;
};

} // namespace easyio::uring
