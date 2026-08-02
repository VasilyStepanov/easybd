#include "libc_queue.hpp"

#include <cerrno>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "libc_recv_stream.hpp"

namespace easyio::libc_backend {

namespace {

void throw_errno(int err, const char* what) {
    throw std::system_error(err, std::generic_category(), what);
}

class AcceptAwaiter final : public FdOp {
public:
    AcceptAwaiter(Queue& queue, int fd) : _queue(queue), _fd(fd) {}

    [[nodiscard]] bool await_ready() noexcept { return _try(); }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        _handle = h;
        _queue._set_read_op(_fd, this);
    }
    [[nodiscard]] int await_resume() const noexcept { return _result; }

private:
    bool _try() noexcept {
        int r = ::accept4(_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return false;
            }
            _result = -errno;
            return true;
        }
        _result = r;
        return true;
    }

    bool on_ready() noexcept override {
        if (!_try()) {
            return false;
        }
        _handle.resume();
        return true;
    }

    void cancel() noexcept override {
        _result = -ECANCELED;
        _handle.resume();
    }

    Queue& _queue;
    int _fd;
    std::coroutine_handle<> _handle;
    int _result = 0;
};

class ConnectAwaiter final : public FdOp {
public:
    ConnectAwaiter(Queue& queue, int fd, const sockaddr* addr, socklen_t addrlen)
        : _queue(queue), _fd(fd) {
        if (::connect(fd, addr, addrlen) == 0) {
            _result = 0;
            _done = true;
        } else if (errno == EINPROGRESS) {
            _done = false;
        } else {
            _result = -errno;
            _done = true;
        }
    }

    [[nodiscard]] bool await_ready() const noexcept { return _done; }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        _handle = h;
        _queue._push_write_op(_fd, this);
    }
    [[nodiscard]] int await_resume() const noexcept { return _result; }

private:
    bool on_ready() noexcept override {
        int err = 0;
        socklen_t len = sizeof(err);
        if (::getsockopt(_fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
            err = errno;
        }
        _result = err ? -err : 0;
        _handle.resume();
        return true;
    }

    void cancel() noexcept override {
        _result = -ECANCELED;
        _handle.resume();
    }

    Queue& _queue;
    int _fd;
    bool _done;
    std::coroutine_handle<> _handle;
    int _result = 0;
};

class SendAwaiter final : public FdOp {
public:
    SendAwaiter(Queue& queue, int fd, const void* buf, size_t size)
        : _queue(queue), _fd(fd), _buf(buf), _size(size) {}

    [[nodiscard]] bool await_ready() noexcept { return _try(); }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        _handle = h;
        _queue._push_write_op(_fd, this);
    }
    [[nodiscard]] ssize_t await_resume() const noexcept { return _result; }

private:
    bool _try() noexcept {
        ssize_t r = ::send(_fd, _buf, _size, MSG_NOSIGNAL);
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return false;
            }
            _result = -errno;
            return true;
        }
        _result = r;
        return true;
    }

    bool on_ready() noexcept override {
        if (!_try()) {
            return false;
        }
        _handle.resume();
        return true;
    }

    void cancel() noexcept override {
        _result = -ECANCELED;
        _handle.resume();
    }

    Queue& _queue;
    int _fd;
    const void* _buf;
    size_t _size;
    std::coroutine_handle<> _handle;
    ssize_t _result = 0;
};

class SendMsgAwaiter final : public FdOp {
public:
    SendMsgAwaiter(Queue& queue, int fd, const iovec* iov, int iovcnt)
        : _queue(queue), _fd(fd), _iov(iov), _iovcnt(iovcnt) {}

    [[nodiscard]] bool await_ready() noexcept { return _try(); }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        _handle = h;
        _queue._push_write_op(_fd, this);
    }
    [[nodiscard]] ssize_t await_resume() const noexcept { return _result; }

private:
    bool _try() noexcept {
        msghdr msg{};
        msg.msg_iov = const_cast<iovec*>(_iov);
        msg.msg_iovlen = static_cast<size_t>(_iovcnt);
        ssize_t r = ::sendmsg(_fd, &msg, MSG_NOSIGNAL);
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return false;
            }
            _result = -errno;
            return true;
        }
        _result = r;
        return true;
    }

    bool on_ready() noexcept override {
        if (!_try()) {
            return false;
        }
        _handle.resume();
        return true;
    }

    void cancel() noexcept override {
        _result = -ECANCELED;
        _handle.resume();
    }

    Queue& _queue;
    int _fd;
    const iovec* _iov;
    int _iovcnt;
    std::coroutine_handle<> _handle;
    ssize_t _result = 0;
};

} // namespace

Queue::Queue(unsigned int depth) : easyio::Queue(depth) {}

Queue::~Queue() {
    for (auto& [fd, state] : _fds) {
        (void)fd;
        if (state.read_op) {
            state.read_op->cancel();
        }
        for (auto* op : state.write_queue) {
            op->cancel();
        }
    }
}

void Queue::_set_read_op(int fd, FdOp* op) { _fds[fd].read_op = op; }

void Queue::_clear_read_op(int fd, FdOp* op) noexcept {
    auto it = _fds.find(fd);
    if (it != _fds.end() && it->second.read_op == op) {
        it->second.read_op = nullptr;
        if (it->second.empty()) {
            _fds.erase(it);
        }
    }
}

void Queue::_push_write_op(int fd, FdOp* op) { _fds[fd].write_queue.push_back(op); }

void Queue::cancel_fd(int fd) {
    auto it = _fds.find(fd);
    if (it == _fds.end()) {
        return;
    }
    // Copy out: cancel() resumes coroutines which may reenter and mutate
    // _fds (including erasing this very entry), so don't iterate it live.
    FdOp* read_op = it->second.read_op;
    std::deque<FdOp*> write_queue = std::move(it->second.write_queue);
    _fds.erase(it);

    if (read_op) {
        read_op->cancel();
    }
    for (auto* op : write_queue) {
        op->cancel();
    }
}

void Queue::step(int timeout_ms) {
    // Exactly one wait-then-dispatch cycle -- see queue.hpp for why this
    // must not loop internally until nothing-left-to-do.
    std::vector<pollfd> pfds;
    pfds.reserve(_fds.size());
    for (auto& [fd, state] : _fds) {
        short events = 0;
        if (state.read_op) {
            events |= POLLIN;
        }
        if (!state.write_queue.empty()) {
            events |= POLLOUT;
        }
        if (events) {
            pfds.push_back(pollfd{fd, events, 0});
        }
    }

    int n = ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()), timeout_ms);
    if (n < 0) {
        if (errno == EINTR) {
            return;
        }
        throw_errno(errno, "poll");
    }
    if (n == 0) {
        // timeout_ms elapsed with nothing ready.
        return;
    }

    for (auto& pfd : pfds) {
        if (pfd.revents == 0) {
            continue;
        }
        if (pfd.revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)) {
            auto it = _fds.find(pfd.fd);
            if (it != _fds.end() && it->second.read_op) {
                FdOp* op = it->second.read_op;
                // on_ready() resumes a coroutine, which may reentrantly
                // submit new operations on *other* fds -- e.g. an accept
                // completing spawns a handler that registers a recv_stream
                // on the new client fd. That's an unordered_map insertion,
                // which can rehash and invalidate `it` (references/pointers
                // to elements survive a rehash, but iterators don't) -- so
                // `it` must not be reused afterwards without re-finding it.
                if (op->on_ready()) {
                    it = _fds.find(pfd.fd);
                    if (it != _fds.end() && it->second.read_op == op) {
                        it->second.read_op = nullptr;
                    }
                }
            }
        }
        if (pfd.revents & (POLLOUT | POLLHUP | POLLERR | POLLNVAL)) {
            for (;;) {
                auto it = _fds.find(pfd.fd);
                if (it == _fds.end() || it->second.write_queue.empty()) {
                    break;
                }
                FdOp* op = it->second.write_queue.front();
                if (!op->on_ready()) {
                    break;
                }
                // Same reentrancy hazard as above: re-find rather than
                // reuse `it` across the on_ready() call.
                it = _fds.find(pfd.fd);
                if (it != _fds.end() && !it->second.write_queue.empty() &&
                    it->second.write_queue.front() == op) {
                    it->second.write_queue.pop_front();
                }
            }
        }
        auto it = _fds.find(pfd.fd);
        if (it != _fds.end() && it->second.empty()) {
            _fds.erase(it);
        }
    }
}

Task<int> Queue::open(const char* path, int flags, mode_t mode) {
    int fd = ::open(path, flags, mode);
    if (fd < 0) {
        throw_errno(errno, "open");
    }
    co_return fd;
}

Task<void> Queue::close(int fd) {
    if (::close(fd) < 0) {
        throw_errno(errno, "close");
    }
    co_return;
}

Task<size_t> Queue::pread(int fd, void* buf, size_t size, uint64_t offset) {
    ssize_t r = ::pread(fd, buf, size, static_cast<off_t>(offset));
    if (r < 0) {
        throw_errno(errno, "pread");
    }
    co_return static_cast<size_t>(r);
}

Task<size_t> Queue::pwrite(int fd, const void* buf, size_t size, uint64_t offset) {
    ssize_t r = ::pwrite(fd, buf, size, static_cast<off_t>(offset));
    if (r < 0) {
        throw_errno(errno, "pwrite");
    }
    co_return static_cast<size_t>(r);
}

Task<void> Queue::fdatasync(int fd) {
    if (::fdatasync(fd) < 0) {
        throw_errno(errno, "fdatasync");
    }
    co_return;
}

Task<int> Queue::accept(int fd) {
    int res = co_await AcceptAwaiter(*this, fd);
    if (res < 0) {
        throw_errno(-res, "accept");
    }
    co_return res;
}

Task<void> Queue::connect(int fd, const sockaddr* addr, socklen_t addrlen) {
    int res = co_await ConnectAwaiter(*this, fd, addr, addrlen);
    if (res < 0) {
        throw_errno(-res, "connect");
    }
    co_return;
}

Task<size_t> Queue::send(int fd, const void* buf, size_t size) {
    ssize_t res = co_await SendAwaiter(*this, fd, buf, size);
    if (res < 0) {
        throw_errno(static_cast<int>(-res), "send");
    }
    co_return static_cast<size_t>(res);
}

Task<size_t> Queue::sendmsg(int fd, const iovec* iov, int iovcnt) {
    ssize_t res = co_await SendMsgAwaiter(*this, fd, iov, iovcnt);
    if (res < 0) {
        throw_errno(static_cast<int>(-res), "sendmsg");
    }
    co_return static_cast<size_t>(res);
}

std::unique_ptr<RecvStream> Queue::recv_stream(
    int fd, size_t entry_size, unsigned int entries) {
    return make_recv_stream(*this, fd, entry_size, entries);
}

} // namespace easyio::libc_backend
