#pragma once

#include <coroutine>
#include <cstddef>
#include <span>
#include <utility>

namespace easyio {

// A live, backend-managed multishot recv operation on one fd. Delivers
// arbitrarily-sized chunks of received bytes as they arrive, without a
// syscall/SQE per chunk (io_uring: one recv_multishot SQE + provided buffer
// ring; libc: one registered read-interest, opportunistic ::recv() loop
// batched under the hood).
//
// RecvStream itself only hands out raw chunks; reassembling a length-
// prefixed protocol out of chunks that may split or coalesce message
// boundaries is layered on top (see FramedReader) rather than duplicated
// per backend.
class RecvStream {
public:
    struct Chunk {
        std::span<const std::byte> data; // empty at EOF
        int error = 0;                   // 0 = ok (data may still be empty at EOF), else -errno
    };

    // Move-only handle for a set of chunks pinned past their normal
    // lifetime (see pin_last()'s doc comment). Default-constructed (or
    // moved-from) is "invalid" -- explicit operator bool() reports whether
    // release() is required. Deliberately opaque: the token is whatever
    // the owning backend's _pin_last() returned, meaningful only to that
    // backend's _release_pin().
    class PinHandle {
    public:
        PinHandle() noexcept = default;
        PinHandle(PinHandle&& other) noexcept
            : _stream(std::exchange(other._stream, nullptr)),
              _token(std::exchange(other._token, nullptr)) {}
        PinHandle& operator=(PinHandle&& other) noexcept {
            if (this != &other) {
                _stream = std::exchange(other._stream, nullptr);
                _token = std::exchange(other._token, nullptr);
            }
            return *this;
        }
        PinHandle(const PinHandle&) = delete;
        PinHandle& operator=(const PinHandle&) = delete;
        ~PinHandle() = default; // see release()'s doc comment: caller must release explicitly

        [[nodiscard]] explicit operator bool() const noexcept { return _stream != nullptr; }

    private:
        friend class RecvStream;
        PinHandle(RecvStream* stream, void* token) noexcept : _stream(stream), _token(token) {}

        RecvStream* _stream = nullptr;
        void* _token = nullptr;
    };

    virtual ~RecvStream() = default;

    RecvStream(const RecvStream&) = delete;
    RecvStream& operator=(const RecvStream&) = delete;

    // Awaiter returned by next(); usable directly as `co_await stream.next()`.
    struct Awaiter {
        RecvStream* stream;

        [[nodiscard]] bool await_ready() noexcept { return stream->_ready(); }
        void await_suspend(std::coroutine_handle<> h) noexcept { stream->_arm(h); }
        [[nodiscard]] Chunk await_resume() noexcept { return stream->_take(); }
    };

    // Returns the next chunk of data (or an empty chunk on EOF, or a chunk
    // with error set on failure). Once EOF/error has been delivered once,
    // the stream is over and must not be awaited again.
    Awaiter next() noexcept { return Awaiter{this}; }

    // Awaiter returned by next_batch(); usable as `co_await
    // stream.next_batch(out, max_bytes)`.
    struct BatchAwaiter {
        RecvStream* stream;
        std::span<Chunk> out;
        size_t max_bytes;

        [[nodiscard]] bool await_ready() noexcept { return stream->_ready(); }
        void await_suspend(std::coroutine_handle<> h) noexcept { stream->_arm(h); }
        [[nodiscard]] size_t await_resume() noexcept { return stream->_take_batch(out, max_bytes); }
    };

    // Batched variant of next(): waits for at least one chunk (identical
    // suspend semantics to next()), then greedily drains any additional
    // chunks that have *already* arrived -- no further waiting involved --
    // up to max_bytes total and out.size() capacity, writing them into `out`
    // and returning how many were written. Letting a backend that already
    // has several completions queued up hand them all out in one go (rather
    // than one chunk per next() round trip) is what lets a caller like
    // FramedReader gather-copy several ring-provided chunks in one pass
    // instead of one coroutine suspend/resume per chunk.
    //
    // Every chunk returned this way -- like next()'s -- stays valid only
    // until the following next()/next_batch() call: that call's _ready()
    // reclaims every buffer handed out (via either method) since the one
    // before it, exactly like next()'s existing single-chunk contract, just
    // generalized to however many are simultaneously outstanding.
    BatchAwaiter next_batch(std::span<Chunk> out, size_t max_bytes) noexcept {
        return BatchAwaiter{this, out, max_bytes};
    }

    // Pins every chunk handed out by the immediately preceding _take()/
    // _take_batch() call -- i.e. exactly the set the *next* next()/
    // next_batch() call's _ready() would otherwise reclaim -- so that set
    // survives past that point instead. Meant for a caller that wants to
    // keep reading (or issuing further I/O against) backend-owned memory
    // beyond the usual one-call lifetime, e.g. writing straight out of an
    // io_uring provided buffer instead of copying it first.
    //
    // Returns an invalid handle (operator bool() == false) if this
    // backend/stream can't pin right now (the libc backend and io_uring's
    // singleshot mode never can; io_uring multishot usually can, but may
    // decline for backend-specific reasons) -- the caller must fall back
    // to copying the chunk's bytes out itself in that case, exactly as if
    // pinning didn't exist. Not noexcept: a backend's bookkeeping for a
    // newly-pinned set may need to allocate (e.g. to remember which ring
    // slots it covers), and a genuine allocation failure here should
    // surface as an ordinary exception rather than a silent decline that
    // would otherwise look identical to "backend just can't pin."
    [[nodiscard]] PinHandle pin_last() {
        void* token = _pin_last();
        return token ? PinHandle(this, token) : PinHandle();
    }

    // Returns every chunk referenced by handle to the backend, undoing
    // pin_last(). Must be called exactly once per valid PinHandle, and may
    // be called from anywhere (not necessarily before the stream's next
    // next() call) -- typically once whatever I/O was reading directly out
    // of that pinned memory has completed. A handle that's still valid
    // when destroyed without this having run leaks its chunks (they never
    // return to circulation) -- same "caller must follow the contract"
    // expectation as WaitGroup's add()/done().
    void release(PinHandle handle) noexcept {
        if (handle._stream) {
            handle._stream->_release_pin(handle._token);
            handle._stream = nullptr;
            handle._token = nullptr;
        }
    }

protected:
    RecvStream() = default;

    // Backend hooks driving the Awaiter protocols above. _ready() runs at
    // the start of every next()/next_batch() call regardless of whether it
    // ends up suspending, so it's also where a backend reclaims the
    // buffer(s) handed out by the *previous* call (see the chunk-lifetime
    // note above) and acts on any bookkeeping that unblocked as a result
    // (e.g. resubmitting a multishot op that paused for lack of buffers).
    virtual bool _ready() noexcept = 0;
    virtual void _arm(std::coroutine_handle<> h) noexcept = 0;
    virtual Chunk _take() noexcept = 0;

    // Writes up to out.size() chunks (stopping early once max_bytes total
    // has been reached) into `out`, returning the count written. Must
    // behave like _take() called repeatedly, except it never waits for more
    // data than was already available when the call started.
    virtual size_t _take_batch(std::span<Chunk> out, size_t max_bytes) noexcept = 0;

    // Backend hooks for pin_last()/release() -- see their doc comments.
    // Default (no pinning support): always decline. _pin_last() is
    // deliberately not noexcept -- see pin_last()'s doc comment.
    virtual void* _pin_last() { return nullptr; }
    virtual void _release_pin(void* token) noexcept { (void)token; }
};

} // namespace easyio
