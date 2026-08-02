#include "uring_queue.hpp"

#include <cerrno>
#include <system_error>
#include <utility>

#include <fcntl.h>

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
        throw_errno(-ENOBUFS, "io_uring_get_sqe");
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

void Queue::run() {
    while (!_stop_requested) {
        int ret = io_uring_submit_and_wait(&_ring, 1);
        if (ret < 0 && ret != -EINTR) {
            throw_errno(ret, "io_uring_submit_and_wait");
        }
        _dispatch();
    }
}

void Queue::stop() { _stop_requested = true; }

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
        io_uring_prep_send(sqe, fd, buf, size, 0);
    });
    if (res < 0) {
        throw_errno(res, "send");
    }
    co_return static_cast<size_t>(res);
}

std::unique_ptr<RecvStream> Queue::recv_stream(
    int fd, size_t entry_size, unsigned int entries) {
    return make_recv_stream(*this, fd, entry_size, entries);
}

} // namespace easyio::uring
