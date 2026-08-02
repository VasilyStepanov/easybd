#include "uring_recv_stream.hpp"

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <deque>
#include <new>
#include <system_error>
#include <utility>

namespace easyio::uring {

namespace {

unsigned short next_buffer_group_id() noexcept {
    static std::atomic<unsigned short> counter{1};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

// Multishot recv on one fd, backed by an io_uring provided buffer ring: the
// kernel fills buffers from the ring as data arrives and keeps generating
// CQEs (IORING_CQE_F_MORE) without us resubmitting, until it errors, hits
// EOF, or runs out of provided buffers.
class RecvStreamImpl final : public RecvStream, public CqeSink {
public:
    RecvStreamImpl(Queue& queue, int fd, size_t entry_size, unsigned int entries)
        : _queue(queue), _fd(fd), _entry_size(entry_size), _entries(entries),
          _bgid(next_buffer_group_id()) {
        int ret = 0;
        _br = io_uring_setup_buf_ring(&_queue._ring_handle(), _entries, _bgid, 0, &ret);
        if (!_br) {
            throw std::system_error(-ret, std::generic_category(), "io_uring_setup_buf_ring");
        }

        _buf_len = _entry_size * _entries;
        if (posix_memalign(&_buf_base, 4096, _buf_len) != 0) {
            io_uring_free_buf_ring(&_queue._ring_handle(), _br, _entries, _bgid);
            throw std::bad_alloc();
        }

        int mask = io_uring_buf_ring_mask(_entries);
        for (unsigned int i = 0; i < _entries; ++i) {
            io_uring_buf_ring_add(
                _br, static_cast<char*>(_buf_base) + i * _entry_size,
                static_cast<unsigned int>(_entry_size), static_cast<unsigned short>(i), mask, i);
        }
        io_uring_buf_ring_advance(_br, static_cast<int>(_entries));

        _submit_recv();
    }

    ~RecvStreamImpl() override {
        io_uring_sync_cancel_reg reg{};
        reg.addr = reinterpret_cast<unsigned long long>(static_cast<CqeSink*>(this));
        reg.fd = _fd;
        // Ignore the result: ENOENT just means the multishot op had already
        // finished (EOF/error) before we got here.
        io_uring_register_sync_cancel(&_queue._ring_handle(), &reg);

        free(_buf_base);
        io_uring_free_buf_ring(&_queue._ring_handle(), _br, _entries, _bgid);
    }

private:
    void _submit_recv() {
        io_uring_sqe* sqe = _queue._get_sqe();
        io_uring_prep_recv_multishot(sqe, _fd, nullptr, 0, 0);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = _bgid;
        io_uring_sqe_set_data(sqe, static_cast<CqeSink*>(this));
    }

    void on_cqe(int res, unsigned int flags) noexcept override {
        bool more = flags & IORING_CQE_F_MORE;
        bool enobufs = false;

        if (res < 0) {
            if (-res == ENOBUFS) {
                // Ring ran out of provided buffers, e.g. because data
                // arrived faster than the consumer is freeing chunks. Not a
                // real error: the multishot op self-terminates on ENOBUFS,
                // and we resubmit as soon as a buffer frees up.
                enobufs = true;
                _needs_resubmit = true;
            } else {
                _error = -res;
            }
        } else if (flags & IORING_CQE_F_BUFFER) {
            unsigned int buf_idx = flags >> IORING_CQE_BUFFER_SHIFT;
            _pending.push_back({buf_idx, static_cast<unsigned int>(res)});
        } else {
            // res == 0, no buffer selected: orderly shutdown (EOF).
            _eof = true;
        }

        if (!more && _error == 0 && !_eof && !enobufs) {
            // Multishot ended without an explicit error/EOF marker -- surface
            // it as EOF so a waiting reader doesn't hang forever instead of
            // silently stalling.
            _eof = true;
        }

        if (enobufs) {
            // A buffer may already be free (the freeing side runs at the
            // start of every next() call, not gated on this notification),
            // in which case resubmit right away instead of waiting for a
            // future free that may never come.
            _maybe_resubmit();
        }

        // Only wake the reader once there's actually something for _take()
        // to deliver -- an ENOBUFS cqe alone produces neither pending data
        // nor a terminal state, it's just an internal signal to resubmit
        // once a buffer frees up.
        if (_waiter && (!_pending.empty() || _eof || _error != 0)) {
            std::exchange(_waiter, nullptr).resume();
        }
    }

    // Runs at the start of every next() call (see recv_stream.hpp): reclaims
    // the buffer handed out by the previous _take(), which by contract the
    // caller is done with as of this new call, then reports whether a chunk
    // is available without suspending.
    bool _ready() noexcept override {
        _return_checked_out();
        return !_pending.empty() || _eof || _error != 0;
    }

    void _arm(std::coroutine_handle<> h) noexcept override { _waiter = h; }

    Chunk _take() noexcept override {
        if (!_pending.empty()) {
            Pending p = _pending.front();
            _pending.pop_front();
            _checked_out_idx = static_cast<int>(p.buf_idx);
            const auto* base = reinterpret_cast<const std::byte*>(
                static_cast<char*>(_buf_base) + p.buf_idx * _entry_size);
            return Chunk{std::span<const std::byte>(base, p.len), 0};
        }
        if (_error != 0) {
            return Chunk{{}, -_error};
        }
        return Chunk{{}, 0};
    }

    void _return_checked_out() {
        if (_checked_out_idx >= 0) {
            io_uring_buf_ring_add(
                _br, static_cast<char*>(_buf_base) + _checked_out_idx * _entry_size,
                static_cast<unsigned int>(_entry_size), static_cast<unsigned short>(_checked_out_idx),
                io_uring_buf_ring_mask(_entries), 0);
            io_uring_buf_ring_advance(_br, 1);
            _checked_out_idx = -1;
        }
        _maybe_resubmit();
    }

    void _maybe_resubmit() {
        if (_needs_resubmit && _checked_out_idx < 0) {
            _needs_resubmit = false;
            _submit_recv();
        }
    }

    Queue& _queue;
    int _fd;
    size_t _entry_size;
    unsigned int _entries;
    unsigned short _bgid;

    void* _buf_base = nullptr;
    size_t _buf_len = 0;
    io_uring_buf_ring* _br = nullptr;

    struct Pending {
        unsigned int buf_idx;
        unsigned int len;
    };
    std::deque<Pending> _pending;
    int _checked_out_idx = -1;

    bool _eof = false;
    int _error = 0;
    bool _needs_resubmit = false;
    std::coroutine_handle<> _waiter;
};

} // namespace

std::unique_ptr<RecvStream> make_recv_stream(
    Queue& queue, int fd, size_t entry_size, unsigned int entries) {
    return std::make_unique<RecvStreamImpl>(queue, fd, entry_size, entries);
}

} // namespace easyio::uring
