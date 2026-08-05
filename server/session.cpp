#include "session.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>

#include <easybd/protocol.h>
#include <easyio/framed_reader.hpp>
#include <easyio/wait_group.hpp>

#include "aligned_buffer.hpp"

namespace easybd {

namespace {

// recv_stream() ring sizing: entry_size close to what a single TCP recv
// actually delivers, not EASYBD_MAX_PAYLOAD_SIZE (one whole max-size
// message) -- a provided-buffer-ring slot is consumed whole per
// completion regardless of how many bytes it actually holds, so slots
// sized for the rare worst case (one giant write request body) exhaust
// after just a handful of ordinary-sized chunks, forcing far more
// resubmits than slots sized for the common case would.
//
// entries is deliberately tiny (a handful of slots, not hundreds): what
// actually drives multishot's per-op CPU cost on large multi-chunk
// transfers isn't slot size or ring capacity in isolation, it's how many
// *distinct* buffer-ring slots end up cycling through cache before one
// gets reused -- a big ring (this used to be 256 entries / 32 MiB, sized
// to comfortably outrun queue.depth()'s worst case) spreads a transfer's
// chunks across many cold, rarely-revisited memory regions, while a
// small ring forces the same handful of physical buffers to be reused
// (and stay cache-hot) many times per transfer. Measured on seq4m_write
// with --feature-multishot: shrinking entries from 256 down to 4 (and
// nothing else) cut server-side CPU cost per op by ~25%, closing what
// had been a large, consistent gap against the ordinary (non-multishot)
// recv path -- confirmed via repeated same-session A/B trials, not a
// one-off. The added resubmit frequency this implies (a 4MiB request
// needs 32 chunks at this entry_size, so this ring recycles many times
// over per single request) turned out to cost far less than the cache
// locality it buys back; entries down at the theoretical minimum (2) was
// measured statistically indistinguishable from 4, so 4 is kept for a
// little pipelining headroom rather than chasing the last fraction of a
// percent. Independent of queue.depth() (the whole worker's underlying
// SQE/poll capacity, shared across every connection it handles) -- using
// queue.depth() directly here was tried and measured worse.
constexpr size_t kRecvEntrySize = 1u << 17; // 128 KiB
constexpr unsigned int kRecvEntries = 4; // 512 KiB ring total

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
    easyio::Queue& queue, int client_fd, int file_fd, uint64_t /*file_size*/,
    bool multishot_recv) {
    easyio::set_nonblocking(client_fd);
    easyio::set_tcp_nodelay(client_fd);
    // See kRecvEntrySize/kRecvEntries above. multishot: see
    // --feature-multishot in server/main.cpp.
    auto stream = queue.recv_stream(client_fd, kRecvEntrySize, kRecvEntries, multishot_recv);
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
                AlignedBuffer buf(req.size);
                co_await reader.read_exact_into(buf.data(), req.size);
                wg.add();
                easyio::spawn(tracked(
                    handle_write(
                        queue, client_fd, file_fd, send_mtx, req.cid, req.offset, std::move(buf),
                        /*durable=*/req.op == EASYBD_OP_WRITE_SYNC),
                    wg));
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

} // namespace easybd
