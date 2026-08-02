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
    Task<int> accept(int fd) override;
    Task<void> connect(int fd, const sockaddr* addr, socklen_t addrlen) override;
    Task<size_t> send(int fd, const void* buf, size_t size) override;
    std::unique_ptr<RecvStream> recv_stream(
        int fd, size_t entry_size, unsigned int entries) override;
    void cancel_fd(int fd) override;
    void run(int timeout_ms = -1) override;
    void stop() override;

    // Internal API for the SqeAwaiter/RecvStreamImpl helper classes defined
    // in the .cpp files (they live in anonymous namespaces there, so a
    // friend declaration here can't name them -- this type itself is
    // already an implementation-private class, not part of easyio's public
    // API, so exposing these publicly costs nothing).
    io_uring_sqe* _get_sqe();
    io_uring& _ring_handle() noexcept { return _ring; }

private:
    void _dispatch();

    io_uring _ring{};
    bool _stop_requested = false;
};

} // namespace easyio::uring
