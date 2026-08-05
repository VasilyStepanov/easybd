#include "session.hpp"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <vector>

#include <sys/uio.h>

#include <easybd/protocol.h>
#include <easyio/framed_reader.hpp>
#include <easyio/recv_stream.hpp>
#include <easyio/wait_group.hpp>

#include "aligned_buffer.hpp"

namespace easybd {

namespace {

std::atomic<uint64_t> g_zc_hits{0};
std::atomic<uint64_t> g_zc_pinned_unaligned{0};
std::atomic<uint64_t> g_zc_unavailable{0};

// O_DIRECT alignment requirement checked against pinned ring memory before
// trusting it to pwritev straight into the backing file. 4096 matches
// AlignedBuffer's existing default and covers every backend/device this
// server targets (tmpfs and brd don't care; a real NVMe device's logical
// block size is usually 512 but its physical/optimal alignment is commonly
// 4096) -- conservative rather than querying the device's actual logical
// block size, since declining zero-copy just means falling back to the
// always-correct copy path, not a correctness problem.
constexpr uintptr_t kZeroCopyAlignment = 4096;

// True iff every chunk's address is alignment-safe to hand straight to
// pwritev, and (for every chunk but the last) its length is too -- an
// interior chunk boundary that isn't itself alignment-sized would leave
// the *next* chunk's effective file offset misaligned even if its own
// pointer happens to be.
bool zero_copy_aligned(const std::vector<easyio::RecvStream::Chunk>& chunks) noexcept {
    for (size_t i = 0; i < chunks.size(); ++i) {
        auto addr = reinterpret_cast<uintptr_t>(chunks[i].data.data());
        if (addr % kZeroCopyAlignment != 0) {
            return false;
        }
        if (i + 1 < chunks.size() && chunks[i].data.size() % kZeroCopyAlignment != 0) {
            return false;
        }
    }
    return true;
}

// Releases every pin in `pins` when this guard goes out of scope --
// covers handle_write_zerocopy's normal return path as well as any
// exception unwinding through it (e.g. something other than the
// std::system_error its own try/catch already handles).
struct PinReleaseGuard {
    easyio::RecvStream& stream;
    std::vector<easyio::RecvStream::PinHandle>& pins;

    ~PinReleaseGuard() {
        for (auto& p : pins) {
            stream.release(std::move(p));
        }
    }
};

// recv_stream() ring sizing now lives in ServerConfig::recv_ring_entries/
// recv_ring_entry_size (see its doc comment) rather than fixed constants
// here, so it can be raised to cover zero-copy's pinned-until-write-
// completes slots -- see handle_write_zerocopy below. entry_size close to
// what a single TCP recv actually delivers, not EASYBD_MAX_PAYLOAD_SIZE
// (one whole max-size message), is still the right default: a provided-
// buffer-ring slot is consumed whole per completion regardless of how many
// bytes it actually holds, so slots sized for the rare worst case (one
// giant write request body) exhaust after just a handful of ordinary-sized
// chunks, forcing far more resubmits than slots sized for the common case
// would.
//
// The default entry count (4) is deliberately tiny: what actually drives
// multishot's per-op CPU cost on large multi-chunk transfers isn't slot
// size or ring capacity in isolation, it's how many *distinct* buffer-ring
// slots end up cycling through cache before one gets reused -- a big ring
// (this used to be 256 entries / 32 MiB, sized to comfortably outrun
// queue.depth()'s worst case) spreads a transfer's chunks across many
// cold, rarely-revisited memory regions, while a small ring forces the
// same handful of physical buffers to be reused (and stay cache-hot) many
// times per transfer. Measured on seq4m_write with --feature-multishot:
// shrinking entries from 256 down to 4 (and nothing else) cut server-side
// CPU cost per op by ~25%, closing what had been a large, consistent gap
// against the ordinary (non-multishot) recv path -- confirmed via repeated
// same-session A/B trials, not a one-off. The added resubmit frequency
// this implies (a 4MiB request needs 32 chunks at this entry_size, so this
// ring recycles many times over per single request) turned out to cost far
// less than the cache locality it buys back; entries down at the
// theoretical minimum (2) was measured statistically indistinguishable
// from 4, so 4 is kept for a little pipelining headroom rather than
// chasing the last fraction of a percent. Independent of queue.depth()
// (the whole worker's underlying SQE/poll capacity, shared across every
// connection it handles) -- using queue.depth() directly here was tried
// and measured worse.

// Used for a request that fails validation before any actual I/O is
// attempted (currently just an oversized read) -- same response shape as
// handle_read/handle_write's own error path, just without the I/O.
easyio::Task<void> send_error(
    easyio::Queue& queue, int client_fd, easyio::Mutex& send_mtx, uint64_t cid, int err) {
    EasyBDResponseHeader resp{cid, -static_cast<int64_t>(err)};
    auto guard = co_await easyio::lock_guard(send_mtx);
    co_await easyio::send_all(queue, client_fd, &resp, sizeof(resp));
}

easyio::Task<void> handle_write(
    easyio::Queue& queue, int client_fd, int file_fd, easyio::Mutex& send_mtx, uint64_t cid,
    uint64_t offset, AlignedBuffer payload, bool durable) {
    int64_t res = 0;
    try {
        // durable (EASYBD_OP_WRITE_SYNC) picks pwrite_dsync() over plain
        // pwrite(): a plain pwrite() completing with O_DIRECT only means
        // the page cache was bypassed, not that the backing device's own
        // volatile write cache has been flushed -- without that, res>0
        // would be lying about durability. RWF_DSYNC gets both in one
        // syscall/SQE instead of a separate fdatasync() round trip. See
        // protocol.h's doc comment for why this is a per-request choice
        // (mirrors fio's own --sync) rather than always on.
        size_t written;
        if (durable) {
            written = co_await queue.pwrite_dsync(file_fd, payload.data(), payload.size(), offset);
        } else {
            written = co_await queue.pwrite(file_fd, payload.data(), payload.size(), offset);
        }
        res = static_cast<int64_t>(written);
    } catch (const std::system_error& e) {
        res = -static_cast<int64_t>(e.code().value());
    }

    EasyBDResponseHeader resp{cid, res};
    auto guard = co_await easyio::lock_guard(send_mtx);
    co_await easyio::send_all(queue, client_fd, &resp, sizeof(resp));
}

// Same as handle_write(), except the payload was never copied out of
// recv_stream's own (io_uring multishot) buffer memory in the first place:
// `chunks` point straight into ring slots pinned via `pins` (see
// FramedReader::try_read_exact_zerocopy() and handle_connection's alignment
// check before this is ever spawned). pwritev/pwritev_dsync write directly
// out of that memory; PinReleaseGuard hands every slot back to the ring
// once this coroutine is done with it, however it ends.
easyio::Task<void> handle_write_zerocopy(
    easyio::Queue& queue, easyio::RecvStream& stream, int client_fd, int file_fd,
    easyio::Mutex& send_mtx, uint64_t cid, uint64_t offset,
    std::vector<easyio::RecvStream::Chunk> chunks, std::vector<easyio::RecvStream::PinHandle> pins,
    bool durable) {
    PinReleaseGuard release_guard{stream, pins};

    int64_t res = 0;
    try {
        std::vector<iovec> iov;
        iov.reserve(chunks.size());
        for (const auto& chunk : chunks) {
            iov.push_back(iovec{const_cast<std::byte*>(chunk.data.data()), chunk.data.size()});
        }
        size_t written;
        if (durable) {
            written = co_await queue.pwritev_dsync(
                file_fd, iov.data(), static_cast<int>(iov.size()), offset);
        } else {
            written = co_await queue.pwritev(file_fd, iov.data(), static_cast<int>(iov.size()), offset);
        }
        res = static_cast<int64_t>(written);
    } catch (const std::system_error& e) {
        res = -static_cast<int64_t>(e.code().value());
    }

    EasyBDResponseHeader resp{cid, res};
    auto guard = co_await easyio::lock_guard(send_mtx);
    co_await easyio::send_all(queue, client_fd, &resp, sizeof(resp));
}

easyio::Task<void> handle_read(
    easyio::Queue& queue, int client_fd, int file_fd, easyio::Mutex& send_mtx, uint64_t cid,
    uint64_t offset, uint64_t size) {
    AlignedBuffer buf(size);
    int64_t res = 0;
    try {
        size_t got = co_await queue.pread(file_fd, buf.data(), buf.size(), offset);
        res = static_cast<int64_t>(got);
    } catch (const std::system_error& e) {
        res = -static_cast<int64_t>(e.code().value());
    }

    EasyBDResponseHeader resp{cid, res};
    auto guard = co_await easyio::lock_guard(send_mtx);
    if (res > 0) {
        co_await easyio::send_all(
            queue, client_fd, &resp, sizeof(resp), buf.data(), static_cast<size_t>(res));
    } else {
        co_await easyio::send_all(queue, client_fd, &resp, sizeof(resp));
    }
}

// Wraps a spawned request handler so a failure in it (which shouldn't
// happen -- handle_read/handle_write only let std::system_error past their
// own try/catch, and that's turned into an error response, not a thrown
// exception) can't escape into spawn()'s DetachedTask, which terminates the
// process on an unhandled exception. Worst case here: this one response
// never gets sent and the client eventually sees the connection close.
easyio::Task<void> tracked(easyio::Task<void> t, easyio::WaitGroup& wg) {
    try {
        co_await std::move(t);
    } catch (...) { // NOLINT(bugprone-empty-catch): deliberate, see comment above
    }
    wg.done();
}

} // namespace

easyio::Task<void> handle_connection(
    easyio::Queue& queue, int client_fd, int file_fd, uint64_t /*file_size*/, bool multishot_recv,
    unsigned int recv_ring_entries, size_t recv_ring_entry_size) {
    easyio::set_nonblocking(client_fd);
    easyio::set_tcp_nodelay(client_fd);
    // See ServerConfig::recv_ring_entries/recv_ring_entry_size's doc
    // comment. multishot: see --feature-multishot in server/main.cpp.
    auto stream =
        queue.recv_stream(client_fd, recv_ring_entry_size, recv_ring_entries, multishot_recv);
    easyio::FramedReader reader(*stream);
    easyio::Mutex send_mtx;
    easyio::WaitGroup wg;

    try {
        for (;;) {
            auto header_span = co_await reader.read_exact(sizeof(EasyBDRequestHeader));
            EasyBDRequestHeader req; // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::memcpy(&req, header_span.data(), sizeof(req));

            if (req.op == EASYBD_OP_WRITE || req.op == EASYBD_OP_WRITE_SYNC) {
                if (req.size > EASYBD_MAX_PAYLOAD_SIZE) {
                    // Can't just respond with an error and move on: the
                    // client is about to send req.size bytes of payload
                    // right after this header regardless of what we do, and
                    // skipping/not-reading them would desync framing for
                    // whatever request comes next on this connection. Ending
                    // the connection is the only safe response.
                    throw std::runtime_error(
                        "easybd: write request exceeds EASYBD_MAX_PAYLOAD_SIZE");
                }
                bool durable = req.op == EASYBD_OP_WRITE_SYNC;
                std::vector<easyio::RecvStream::Chunk> zc_chunks;
                std::vector<easyio::RecvStream::PinHandle> zc_pins;
                // Zero-length payload has nothing to pin/align meaningfully
                // (and AlignedBuffer(0) is already a well-defined no-op
                // buffer) -- skip straight to the ordinary path for it.
                bool zc = req.size > 0 &&
                    co_await reader.try_read_exact_zerocopy(req.size, zc_chunks, zc_pins);

                if (zc && zero_copy_aligned(zc_chunks)) {
                    g_zc_hits.fetch_add(1, std::memory_order_relaxed);
                    wg.add();
                    easyio::spawn(tracked(
                        handle_write_zerocopy(
                            queue, *stream, client_fd, file_fd, send_mtx, req.cid, req.offset,
                            std::move(zc_chunks), std::move(zc_pins), durable),
                        wg));
                } else if (zc) {
                    // Pinned successfully but the ring's arbitrary internal
                    // chunk boundaries didn't happen to align -- we already
                    // have the bytes in hand (no need to re-read them from
                    // the stream), so just copy them out ourselves and
                    // release the pins immediately, then fall back to the
                    // ordinary path exactly as if zero-copy never existed.
                    g_zc_pinned_unaligned.fetch_add(1, std::memory_order_relaxed);
                    AlignedBuffer buf(req.size);
                    size_t off = 0;
                    for (const auto& chunk : zc_chunks) {
                        std::memcpy(buf.data() + off, chunk.data.data(), chunk.data.size());
                        off += chunk.data.size();
                    }
                    for (auto& pin : zc_pins) {
                        stream->release(std::move(pin));
                    }
                    wg.add();
                    easyio::spawn(tracked(
                        handle_write(
                            queue, client_fd, file_fd, send_mtx, req.cid, req.offset,
                            std::move(buf), durable),
                        wg));
                } else {
                    g_zc_unavailable.fetch_add(1, std::memory_order_relaxed);
                    auto payload_span = co_await reader.read_exact(req.size);
                    AlignedBuffer buf(req.size);
                    std::memcpy(buf.data(), payload_span.data(), req.size);
                    wg.add();
                    easyio::spawn(tracked(
                        handle_write(
                            queue, client_fd, file_fd, send_mtx, req.cid, req.offset,
                            std::move(buf), durable),
                        wg));
                }
            } else if (req.op == EASYBD_OP_READ) {
                wg.add();
                if (req.size > EASYBD_MAX_PAYLOAD_SIZE) {
                    // A read request has no payload of its own following it
                    // on the wire, so framing isn't at risk here -- this can
                    // just be reported as an ordinary per-request error.
                    easyio::spawn(
                        tracked(send_error(queue, client_fd, send_mtx, req.cid, EMSGSIZE), wg));
                } else {
                    easyio::spawn(tracked(
                        handle_read(
                            queue, client_fd, file_fd, send_mtx, req.cid, req.offset, req.size),
                        wg));
                }
            } else {
                throw std::runtime_error("easybd: invalid request op");
            }
        }
    } catch (const std::exception&) { // NOLINT(bugprone-empty-catch): deliberate
        // Either a graceful EOF (client done, or closed the connection) or a
        // real protocol/IO error -- either way, nothing more to read. Fall
        // through to draining in-flight handlers before this coroutine (and
        // therefore the connection) is considered done.
    }

    co_await wg.wait();
}

ZeroCopyStats zero_copy_stats() noexcept {
    return ZeroCopyStats{
        g_zc_hits.load(std::memory_order_relaxed),
        g_zc_pinned_unaligned.load(std::memory_order_relaxed),
        g_zc_unavailable.load(std::memory_order_relaxed),
    };
}

} // namespace easybd
