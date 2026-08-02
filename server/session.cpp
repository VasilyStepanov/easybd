#include "session.hpp"

#include <cstring>
#include <stdexcept>
#include <system_error>

#include <easybd/protocol.h>
#include <easyio/framed_reader.hpp>
#include <easyio/wait_group.hpp>

#include "aligned_buffer.hpp"

namespace easybd {

namespace {

easyio::Task<void> handle_write(
    easyio::Queue& queue, int client_fd, int file_fd, easyio::Mutex& send_mtx, uint64_t cid,
    uint64_t offset, AlignedBuffer payload) {
    int64_t res = 0;
    try {
        size_t written = co_await queue.pwrite(file_fd, payload.data(), payload.size(), offset);
        res = static_cast<int64_t>(written);
    } catch (const std::system_error& e) {
        res = -static_cast<int64_t>(e.code().value());
    }

    EasybdResponseHeader resp{cid, res};
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

    EasybdResponseHeader resp{cid, res};
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
    easyio::Queue& queue, int client_fd, int file_fd, uint64_t /*file_size*/) {
    easyio::set_nonblocking(client_fd);
    easyio::set_tcp_nodelay(client_fd);
    auto stream = queue.recv_stream(client_fd, 1U << 16, 8);
    easyio::FramedReader reader(*stream);
    easyio::Mutex send_mtx;
    easyio::WaitGroup wg;

    try {
        for (;;) {
            auto header_span = co_await reader.read_exact(sizeof(EasybdRequestHeader));
            EasybdRequestHeader req; // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::memcpy(&req, header_span.data(), sizeof(req));

            if (req.op == EASYBD_OP_WRITE) {
                auto payload_span = co_await reader.read_exact(req.size);
                AlignedBuffer buf(req.size);
                std::memcpy(buf.data(), payload_span.data(), req.size);
                wg.add();
                easyio::spawn(tracked(
                    handle_write(
                        queue, client_fd, file_fd, send_mtx, req.cid, req.offset,
                        std::move(buf)),
                    wg));
            } else if (req.op == EASYBD_OP_READ) {
                wg.add();
                easyio::spawn(tracked(
                    handle_read(queue, client_fd, file_fd, send_mtx, req.cid, req.offset, req.size),
                    wg));
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
