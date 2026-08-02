#pragma once

#include <coroutine>
#include <cstddef>
#include <span>

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

    virtual ~RecvStream() = default;

    RecvStream(const RecvStream&) = delete;
    RecvStream& operator=(const RecvStream&) = delete;

    // Awaiter returned by next(); usable directly as `co_await stream.next()`.
    struct Awaiter {
        RecvStream* stream;

        bool await_ready() noexcept { return stream->_ready(); }
        void await_suspend(std::coroutine_handle<> h) noexcept { stream->_arm(h); }
        Chunk await_resume() noexcept { return stream->_take(); }
    };

    // Returns the next chunk of data (or an empty chunk on EOF, or a chunk
    // with error set on failure). Once EOF/error has been delivered once,
    // the stream is over and must not be awaited again.
    Awaiter next() noexcept { return Awaiter{this}; }

protected:
    RecvStream() = default;

    // Backend hooks driving the Awaiter protocol above. _ready() runs at the
    // start of every next() call regardless of whether it ends up
    // suspending, so it's also where a backend reclaims the buffer handed
    // out by the *previous* call (see recv_stream.hpp's chunk-lifetime note)
    // and acts on any bookkeeping that unblocked as a result (e.g.
    // resubmitting a multishot op that paused for lack of buffers).
    virtual bool _ready() noexcept = 0;
    virtual void _arm(std::coroutine_handle<> h) noexcept = 0;
    virtual Chunk _take() noexcept = 0;
};

} // namespace easyio
