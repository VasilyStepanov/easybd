#include "libc_recv_stream.hpp"

#include <cerrno>
#include <cstdlib>
#include <deque>
#include <new>
#include <utility>

namespace easyio::libc_backend {

namespace {

// Multishot-recv equivalent for the libc backend: opportunistically drains
// ::recv() into a fixed ring of `entries` buffers whenever the fd is
// readable, so a chatty connection doesn't need one poll() round trip per
// message the way a naive read-one-then-poll-again loop would.
class RecvStreamImpl final : public RecvStream, public FdOp {
public:
    RecvStreamImpl(Queue& queue, int fd, size_t entry_size, unsigned int entries)
        : _queue(queue), _fd(fd), _entry_size(entry_size), _entries(entries) {
        if (posix_memalign(&_buf_base, 4096, entry_size * entries) != 0) {
            throw std::bad_alloc();
        }
        _queue._set_read_op(_fd, this);
        on_ready(); // opportunistically pick up anything already buffered
    }

    ~RecvStreamImpl() override {
        _queue._clear_read_op(_fd, this);
        free(_buf_base);
    }

private:
    bool _ready() noexcept override {
        // Must run here, not just at the top of _take(): _ready() is called
        // on every next() regardless of whether it ends up suspending, but
        // _take() only runs once a chunk is actually delivered. If a
        // request's whole response was satisfied by one already-buffered
        // chunk (no further next() call needed), _take() never runs again
        // for it, so the slot it checked out stays "checked out" forever
        // unless something else frees it -- and with entries == 1, that's
        // the entire ring, permanently wedging on_ready() into thinking
        // there's never room for another recv() even though the data it
        // last delivered has long since been copied out and the fd may
        // have gone on to deliver more (silently sitting unread in the
        // kernel's receive buffer -- confirmed via `ss -tn` Recv-Q while
        // chasing this down). See uring_recv_stream.cpp's _ready(), which
        // already gets this right.
        _return_checked_out();
        return !_pending.empty() || _eof || _error != 0;
    }

    void _arm(std::coroutine_handle<> h) noexcept override { _waiter = h; }

    Chunk _take() noexcept override {
        if (!_pending.empty()) {
            Pending p = _pending.front();
            _pending.pop_front();
            _checked_out_idx = static_cast<int>(p.idx);
            const auto* base = reinterpret_cast<const std::byte*>(
                static_cast<char*>(_buf_base) + p.idx * _entry_size);
            return Chunk{std::span<const std::byte>(base, p.len), 0};
        }
        if (_error != 0) {
            return Chunk{{}, -_error};
        }
        return Chunk{{}, 0};
    }

    bool on_ready() noexcept override {
        while (_error == 0 && !_eof &&
               _pending.size() + (_checked_out_idx >= 0 ? 1u : 0u) < _entries) {
            unsigned int idx = _write_idx;
            ssize_t r = ::recv(
                _fd, static_cast<char*>(_buf_base) + idx * _entry_size, _entry_size, 0);
            if (r < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                _error = errno;
                break;
            }
            if (r == 0) {
                _eof = true;
                break;
            }
            _write_idx = (idx + 1) % _entries;
            _pending.push_back({idx, static_cast<unsigned int>(r)});
        }

        if (_waiter && (!_pending.empty() || _eof || _error != 0)) {
            std::exchange(_waiter, nullptr).resume();
        }
        return false; // never auto-deregister; only cancel()/destructor do
    }

    void cancel() noexcept override {
        if (_error == 0 && !_eof) {
            _error = ECANCELED;
        }
        if (_waiter) {
            std::exchange(_waiter, nullptr).resume();
        }
    }

    void _return_checked_out() {
        // Nothing to do: the slot becomes free for reuse simply because
        // _write_idx already moved past it and won't reuse it until the
        // ring wraps back around after `_entries` further receives, by
        // which point _take() will have long since been called again.
        _checked_out_idx = -1;
    }

    Queue& _queue;
    int _fd;
    size_t _entry_size;
    unsigned int _entries;

    void* _buf_base = nullptr;
    unsigned int _write_idx = 0;

    struct Pending {
        unsigned int idx;
        unsigned int len;
    };
    std::deque<Pending> _pending;
    int _checked_out_idx = -1;

    bool _eof = false;
    int _error = 0;
    std::coroutine_handle<> _waiter;
};

} // namespace

std::unique_ptr<RecvStream> make_recv_stream(
    Queue& queue, int fd, size_t entry_size, unsigned int entries) {
    return std::make_unique<RecvStreamImpl>(queue, fd, entry_size, entries);
}

} // namespace easyio::libc_backend
