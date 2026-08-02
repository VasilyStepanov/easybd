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

class RecvStreamImpl;

// Owns the provided buffer ring and is the actual io_uring completion
// target (sqe/cqe user_data) for the multishot recv op -- kept separate
// from RecvStreamImpl so it can outlive it.
//
// This matters because Queue::_dispatch() resumes coroutines synchronously
// while iterating a batch of CQEs that may contain more than one entry for
// operations on the connection being torn down. If a coroutine resumed
// partway through that batch destroys its RecvStream (e.g. because the
// connection is done and closes it), a later CQE in the *same* batch may
// still be addressed to it. Cancelling synchronously from within
// ~RecvStreamImpl (the previous approach, via io_uring_register_sync_cancel)
// both reenters the ring mid-iteration and frees memory a not-yet-processed
// CQE in this batch still points at -- both are use-after-free/corruption.
// Instead, ~RecvStreamImpl detaches this sink and lets it keep itself alive
// (via a plain async cancel SQE) until it observes the op's own terminal
// CQE (IORING_CQE_F_MORE unset), only then freeing the buffer ring.
class RecvSink final : public CqeSink {
public:
    RecvSink(Queue& queue, int fd, size_t entry_size, unsigned int entries)
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
                static_cast<unsigned int>(_entry_size), static_cast<unsigned short>(i), mask,
                static_cast<int>(i));
        }
        io_uring_buf_ring_advance(_br, static_cast<int>(_entries));

        submit_recv();
    }

    ~RecvSink() {
        free(_buf_base);
        io_uring_free_buf_ring(&_queue._ring_handle(), _br, _entries, _bgid);
    }

    void submit_recv() {
        io_uring_sqe* sqe = _queue._get_sqe();
        io_uring_prep_recv_multishot(sqe, _fd, nullptr, 0, 0);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = _bgid;
        io_uring_sqe_set_data(sqe, static_cast<CqeSink*>(this));
        _live = true;
    }

    // Best-effort: if there's no room to even queue the cancel SQE, we just
    // leak this sink rather than risk throwing out of a destructor.
    void request_cancel() noexcept {
        try {
            io_uring_sqe* sqe = _queue._get_sqe();
            io_uring_prep_cancel(sqe, static_cast<CqeSink*>(this), 0);
            io_uring_sqe_set_data(sqe, nullptr);
        } catch (...) { // NOLINT(bugprone-empty-catch): deliberately best-effort, see comment above
        }
    }

    [[nodiscard]] bool live() const noexcept { return _live; }
    void detach() noexcept { owner = nullptr; }

    void* buf_base() noexcept { return _buf_base; }
    [[nodiscard]] size_t entry_size() const noexcept { return _entry_size; }
    [[nodiscard]] unsigned int entries() const noexcept { return _entries; }
    io_uring_buf_ring* buf_ring() noexcept { return _br; }

    RecvStreamImpl* owner = nullptr;

private:
    void on_cqe(int res, unsigned int flags) noexcept override;

    Queue& _queue;
    int _fd;
    size_t _entry_size;
    unsigned int _entries;
    unsigned short _bgid;

    void* _buf_base = nullptr;
    size_t _buf_len = 0;
    io_uring_buf_ring* _br = nullptr;
    bool _live = false;
};

// Multishot recv on one fd: the kernel fills buffers from RecvSink's
// provided buffer ring as data arrives and keeps generating CQEs
// (IORING_CQE_F_MORE) without us resubmitting, until it errors, hits EOF,
// or runs out of provided buffers (ENOBUFS, handled by resubmitting once a
// buffer frees up rather than treating it as terminal).
class RecvStreamImpl final : public RecvStream {
public:
    RecvStreamImpl(Queue& queue, int fd, size_t entry_size, unsigned int entries)
        : _sink(new RecvSink(queue, fd, entry_size, entries)) {
        _sink->owner = this;
    }

    ~RecvStreamImpl() override {
        _sink->detach();
        if (_sink->live()) {
            _sink->request_cancel();
            // Ownership of the sink (and its eventual cleanup) is now
            // fully self-contained: it deletes itself once it observes the
            // cancellation's terminal completion.
        } else {
            delete _sink;
        }
    }

    void handle_cqe(int res, unsigned int flags) noexcept {
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
            if (!more) {
                // The kernel can fold "ran out of provided buffers" into the
                // same CQE that delivers the last chunk it *could* still
                // serve, instead of always following up with a separate
                // ENOBUFS completion -- same situation as the ENOBUFS case
                // above (ring's dry, not "connection's done"), just
                // reported differently. Observed in practice with large
                // (multi-MB) transfers, where the ring drains faster than
                // the consumer can be resumed to free slots.
                enobufs = true;
                _needs_resubmit = true;
            }
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

private:
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
                static_cast<char*>(_sink->buf_base()) + p.buf_idx * _sink->entry_size());
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
                _sink->buf_ring(),
                static_cast<char*>(_sink->buf_base()) + _checked_out_idx * _sink->entry_size(),
                static_cast<unsigned int>(_sink->entry_size()),
                static_cast<unsigned short>(_checked_out_idx),
                io_uring_buf_ring_mask(_sink->entries()), 0);
            io_uring_buf_ring_advance(_sink->buf_ring(), 1);
            _checked_out_idx = -1;
        }
        _maybe_resubmit();
    }

    void _maybe_resubmit() {
        if (_needs_resubmit && _checked_out_idx < 0) {
            _needs_resubmit = false;
            _sink->submit_recv();
        }
    }

    RecvSink* _sink;

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

void RecvSink::on_cqe(int res, unsigned int flags) noexcept {
    if (!(flags & IORING_CQE_F_MORE)) {
        _live = false;
    }
    if (owner) {
        owner->handle_cqe(res, flags);
    } else if (!_live) {
        // Orphaned (owner already destroyed) and the kernel confirms no
        // more completions are coming for this op: safe to free now.
        delete this;
    }
}

class SingleShotRecvStreamImpl;

// --feature-multishot off: the "ordinary recv" baseline the RecvStream doc
// comment contrasts multishot against -- exactly one plain IORING_OP_RECV
// in flight at a time, into a private (non-provided-buffer) buffer sized
// entry_size, resubmitted only once the caller has reclaimed the previous
// chunk (at the start of the next next() call, same contract every backend
// follows). No buffer ring, no IOSQE_BUFFER_SELECT, no multishot flag --
// this is the one-SQE-per-chunk cost multishot recv exists to avoid.
// `entries` is accepted by the factory below for interface symmetry with
// the multishot path but is meaningless here (there is never more than one
// buffer/op in flight), so it's simply ignored.
class SingleShotRecvSink final : public CqeSink {
public:
    SingleShotRecvSink(Queue& queue, int fd, size_t entry_size)
        : _queue(queue), _fd(fd), _entry_size(entry_size) {
        if (posix_memalign(&_buf, 4096, entry_size) != 0) {
            throw std::bad_alloc();
        }
        submit_recv();
    }

    ~SingleShotRecvSink() { free(_buf); }

    void submit_recv() {
        io_uring_sqe* sqe = _queue._get_sqe();
        io_uring_prep_recv(sqe, _fd, _buf, _entry_size, 0);
        io_uring_sqe_set_data(sqe, static_cast<CqeSink*>(this));
        _live = true;
    }

    // Same best-effort reasoning as RecvSink::request_cancel().
    void request_cancel() noexcept {
        try {
            io_uring_sqe* sqe = _queue._get_sqe();
            io_uring_prep_cancel(sqe, static_cast<CqeSink*>(this), 0);
            io_uring_sqe_set_data(sqe, nullptr);
        } catch (...) { // NOLINT(bugprone-empty-catch): deliberately best-effort
        }
    }

    [[nodiscard]] bool live() const noexcept { return _live; }
    void detach() noexcept { owner = nullptr; }
    [[nodiscard]] void* buf() const noexcept { return _buf; }

    SingleShotRecvStreamImpl* owner = nullptr;

private:
    void on_cqe(int res, unsigned int flags) noexcept override;

    Queue& _queue;
    int _fd;
    size_t _entry_size;
    void* _buf = nullptr;
    bool _live = false;
};

class SingleShotRecvStreamImpl final : public RecvStream {
public:
    SingleShotRecvStreamImpl(Queue& queue, int fd, size_t entry_size)
        : _sink(new SingleShotRecvSink(queue, fd, entry_size)) {
        _sink->owner = this;
    }

    // Same detach-and-self-delete lifetime handling as RecvStreamImpl, just
    // simpler: there is only ever one outstanding op to cancel and wait out,
    // not a live multishot op that might still be mid-batch in this
    // dispatch cycle.
    ~SingleShotRecvStreamImpl() override {
        _sink->detach();
        if (_sink->live()) {
            _sink->request_cancel();
        } else {
            delete _sink;
        }
    }

    void handle_cqe(int res) noexcept {
        if (res < 0) {
            _error = -res;
        } else if (res == 0) {
            _eof = true;
        } else {
            _len = static_cast<unsigned int>(res);
            _has_chunk = true;
        }

        if (_waiter && (_has_chunk || _eof || _error != 0)) {
            std::exchange(_waiter, nullptr).resume();
        }
    }

private:
    bool _ready() noexcept override {
        _reclaim();
        return _has_chunk || _eof || _error != 0;
    }

    void _arm(std::coroutine_handle<> h) noexcept override { _waiter = h; }

    Chunk _take() noexcept override {
        if (_has_chunk) {
            _has_chunk = false;
            _checked_out = true;
            return Chunk{
                std::span<const std::byte>(
                    reinterpret_cast<const std::byte*>(_sink->buf()), _len),
                0};
        }
        if (_error != 0) {
            return Chunk{{}, -_error};
        }
        return Chunk{{}, 0};
    }

    void _reclaim() {
        if (_checked_out) {
            _checked_out = false;
            if (_error == 0 && !_eof) {
                _sink->submit_recv();
            }
        }
    }

    SingleShotRecvSink* _sink;
    unsigned int _len = 0;
    bool _has_chunk = false;
    bool _checked_out = false;
    bool _eof = false;
    int _error = 0;
    std::coroutine_handle<> _waiter;
};

void SingleShotRecvSink::on_cqe(int res, unsigned int /*flags*/) noexcept {
    _live = false; // a plain recv always completes exactly once
    if (owner) {
        owner->handle_cqe(res);
    } else {
        // Orphaned (owner already destroyed): this is the op's own terminal
        // completion (single-shot never has a "more" flag), so it's always
        // safe to free right here, unlike RecvSink which must wait for the
        // specific CQE that clears IORING_CQE_F_MORE.
        delete this;
    }
}

} // namespace

std::unique_ptr<RecvStream> make_recv_stream(
    Queue& queue, int fd, size_t entry_size, unsigned int entries, bool multishot) {
    if (multishot) {
        return std::make_unique<RecvStreamImpl>(queue, fd, entry_size, entries);
    }
    return std::make_unique<SingleShotRecvStreamImpl>(queue, fd, entry_size);
}

} // namespace easyio::uring
