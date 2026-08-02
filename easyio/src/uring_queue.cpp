#include "uring_queue.hpp"

#include <cerrno>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <linux/fs.h>

#include "uring_recv_stream.hpp"

namespace easyio::uring {

namespace {

void throw_errno(int neg_errno, const char* what) {
    throw std::system_error(-neg_errno, std::generic_category(), what);
}

// One-shot awaitable: submits a single SQE (built by `prep`) on construction
// and resumes the awaiting coroutine when its CQE arrives. Lives entirely on
// the awaiting coroutine's frame (as the co_await temporary), so its address
// is stable across the suspend without any heap allocation.
class SqeAwaiter final : public CqeSink {
public:
    template <typename PrepFn>
    SqeAwaiter(Queue& queue, PrepFn&& prep) {
        io_uring_sqe* sqe = queue._get_sqe();
        prep(sqe);
        io_uring_sqe_set_data(sqe, static_cast<CqeSink*>(this));
    }

    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { _handle = h; }
    [[nodiscard]] int await_resume() const noexcept { return _result; }

private:
    void on_cqe(int res, unsigned int /*flags*/) noexcept override {
        _result = res;
        _handle.resume();
    }

    std::coroutine_handle<> _handle;
    int _result = 0;
};

// Gathered send: unlike SqeAwaiter's prep-lambda approach, the msghdr must
// stay alive for as long as the kernel might still read it -- i.e. until
// the CQE arrives, not just until prep returns -- so it has to be a member
// of the awaiter itself (which, like SqeAwaiter, lives on the awaiting
// coroutine's frame) rather than a temporary built inside a lambda.
class SendMsgAwaiter final : public CqeSink {
public:
    SendMsgAwaiter(Queue& queue, int fd, const iovec* iov, int iovcnt) {
        _msg.msg_iov = const_cast<iovec*>(iov);
        _msg.msg_iovlen = static_cast<size_t>(iovcnt);
        io_uring_sqe* sqe = queue._get_sqe();
        io_uring_prep_sendmsg(sqe, fd, &_msg, MSG_NOSIGNAL);
        io_uring_sqe_set_data(sqe, static_cast<CqeSink*>(this));
    }

    [[nodiscard]] bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { _handle = h; }
    [[nodiscard]] int await_resume() const noexcept { return _result; }

private:
    void on_cqe(int res, unsigned int /*flags*/) noexcept override {
        _result = res;
        _handle.resume();
    }

    msghdr _msg{};
    std::coroutine_handle<> _handle;
    int _result = 0;
};

} // namespace

Queue::Queue(unsigned int depth) : easyio::Queue(depth) {
    int ret = io_uring_queue_init(
        depth, &_ring, IORING_SETUP_SUBMIT_ALL | IORING_SETUP_COOP_TASKRUN);
    if (ret < 0) {
        throw_errno(ret, "io_uring_queue_init");
    }
}

Queue::~Queue() {
    io_uring_submit(&_ring);
    io_uring_cqe* cqe;
    while (io_uring_peek_cqe(&_ring, &cqe) == 0) {
        io_uring_cqe_seen(&_ring, cqe);
    }
    io_uring_queue_exit(&_ring);
}

io_uring_sqe* Queue::_get_sqe() {
    io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
    if (!sqe) {
        // The SQ ring is full of not-yet-submitted entries (this is a
        // routine occurrence with a small queue depth, not a real error) --
        // flush them to the kernel to free up slots and retry once.
        int ret = io_uring_submit(&_ring);
        if (ret < 0) {
            throw_errno(ret, "io_uring_submit");
        }
        sqe = io_uring_get_sqe(&_ring);
        if (!sqe) {
            throw_errno(-ENOBUFS, "io_uring_get_sqe");
        }
    }
    return sqe;
}

void Queue::_dispatch() {
    unsigned int head = 0;
    unsigned int nr = 0;
    io_uring_cqe* cqe;
    io_uring_for_each_cqe(&_ring, head, cqe) {
        ++nr;
        auto* sink = static_cast<CqeSink*>(io_uring_cqe_get_data(cqe));
        if (sink) {
            sink->on_cqe(cqe->res, cqe->flags);
        }
    }
    io_uring_cq_advance(&_ring, nr);
}

void Queue::step(int timeout_ms) {
    // Exactly one wait-then-dispatch cycle -- see queue.hpp for why this
    // must not loop internally until nothing-left-to-do.
    int ret;
    if (timeout_ms < 0) {
        ret = io_uring_submit_and_wait(&_ring, 1);
    } else {
        __kernel_timespec ts{};
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = static_cast<long long>(timeout_ms % 1000) * 1000000LL;
        io_uring_cqe* cqe = nullptr;
        ret = io_uring_submit_and_wait_timeout(&_ring, &cqe, 1, &ts, nullptr);
        if (ret == -ETIME) {
            // Nothing completed within timeout_ms -- reap anything that did
            // land right at the boundary, then return either way.
            _dispatch();
            return;
        }
    }
    if (ret < 0) {
        if (ret == -EINTR) {
            return;
        }
        throw_errno(ret, "io_uring_submit_and_wait");
    }
    _dispatch();
}

void Queue::cancel_fd(int fd) {
    io_uring_sqe* sqe = _get_sqe();
    io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
    io_uring_sqe_set_data(sqe, nullptr);
}

Task<int> Queue::open(const char* path, int flags, mode_t mode) {
    int res = co_await SqeAwaiter(*this, [&](io_uring_sqe* sqe) {
        io_uring_prep_openat(sqe, AT_FDCWD, path, flags, mode);
    });
    if (res < 0) {
        throw_errno(res, "open");
    }
    co_return res;
}

Task<void> Queue::close(int fd) {
    int res = co_await SqeAwaiter(
        *this, [&](io_uring_sqe* sqe) { io_uring_prep_close(sqe, fd); });
    if (res < 0) {
        throw_errno(res, "close");
    }
    co_return;
}

Task<size_t> Queue::pread(int fd, void* buf, size_t size, uint64_t offset) {
    int res = co_await SqeAwaiter(*this, [&](io_uring_sqe* sqe) {
        io_uring_prep_read(sqe, fd, buf, size, offset);
    });
    if (res < 0) {
        throw_errno(res, "pread");
    }
    co_return static_cast<size_t>(res);
}

Task<size_t> Queue::pwrite(int fd, const void* buf, size_t size, uint64_t offset) {
    int res = co_await SqeAwaiter(*this, [&](io_uring_sqe* sqe) {
        io_uring_prep_write(sqe, fd, buf, size, offset);
    });
    if (res < 0) {
        throw_errno(res, "pwrite");
    }
    co_return static_cast<size_t>(res);
}

Task<size_t> Queue::pwrite_dsync(int fd, const void* buf, size_t size, uint64_t offset) {
    int res = co_await SqeAwaiter(*this, [&](io_uring_sqe* sqe) {
        io_uring_prep_write(sqe, fd, buf, size, offset);
        sqe->rw_flags |= RWF_DSYNC;
    });
    if (res < 0) {
        throw_errno(res, "pwrite_dsync");
    }
    co_return static_cast<size_t>(res);
}

Task<int> Queue::accept(int fd) {
    int res = co_await SqeAwaiter(*this, [&](io_uring_sqe* sqe) {
        io_uring_prep_accept(sqe, fd, nullptr, nullptr, 0);
    });
    if (res < 0) {
        throw_errno(res, "accept");
    }
    co_return res;
}

Task<void> Queue::connect(int fd, const sockaddr* addr, socklen_t addrlen) {
    int res = co_await SqeAwaiter(*this, [&](io_uring_sqe* sqe) {
        io_uring_prep_connect(sqe, fd, addr, addrlen);
    });
    if (res < 0) {
        throw_errno(res, "connect");
    }
    co_return;
}

Task<size_t> Queue::send(int fd, const void* buf, size_t size) {
    int res = co_await SqeAwaiter(*this, [&](io_uring_sqe* sqe) {
        io_uring_prep_send(sqe, fd, buf, size, MSG_NOSIGNAL);
    });
    if (res < 0) {
        throw_errno(res, "send");
    }
    co_return static_cast<size_t>(res);
}

Task<size_t> Queue::sendmsg(int fd, const iovec* iov, int iovcnt) {
    int res = co_await SendMsgAwaiter(*this, fd, iov, iovcnt);
    if (res < 0) {
        throw_errno(res, "sendmsg");
    }
    co_return static_cast<size_t>(res);
}

std::unique_ptr<RecvStream> Queue::recv_stream(
    int fd, size_t entry_size, unsigned int entries, bool multishot) {
    return make_recv_stream(*this, fd, entry_size, entries, multishot);
}

} // namespace easyio::uring
