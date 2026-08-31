#include "client.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <easybd/protocol.h>
#include <easyio/framed_reader.hpp>

namespace easybd {

namespace {

void throw_errno(int err, const char* what) {
    throw std::system_error(err, std::generic_category(), what);
}

// Accepts either a dotted-decimal literal (checked first, so the common
// case never touches the resolver) or a hostname -- e.g. a docker-compose
// service name like "server", which is how the client is meant to reach a
// server container on the compose network. IPv4 only, matching the rest of
// this client (and easybd-server's own --bind parsing).
in_addr resolve_ipv4(const std::string& host) {
    in_addr addr{};
    if (::inet_pton(AF_INET, host.c_str(), &addr) == 1) {
        return addr;
    }
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    int gai_err = ::getaddrinfo(host.c_str(), nullptr, &hints, &result);
    if (gai_err != 0) {
        throw std::invalid_argument(
            "easybd: could not resolve host '" + host + "': " + ::gai_strerror(gai_err));
    }
    addr = reinterpret_cast<const sockaddr_in*>(result->ai_addr)->sin_addr;
    ::freeaddrinfo(result);
    return addr;
}

// recv_stream() ring sizing: entry_size close to what a single TCP recv
// actually delivers, not EASYBD_MAX_PAYLOAD_SIZE (one whole max-size
// message) -- a provided-buffer-ring slot is consumed whole per completion
// regardless of how many bytes it actually holds, so slots sized for the
// rare worst case (one giant message) exhaust after just a handful of
// ordinary-sized chunks, forcing far more resubmits than slots sized for
// the common case would. Matches rawstor's librawstorio (see
// ost_session.cpp/ost/session.cpp), which uses these same two constants.
constexpr size_t kRecvEntrySize = 1u << 17; // 128 KiB
constexpr unsigned int kRecvEntries = 64 * 4; // 256 -- 32 MiB ring total

} // namespace

Client::Client(
    const std::string& host, uint16_t port, easyio::Backend backend, unsigned int queue_depth,
    bool multishot_recv, bool sync_writes)
    : _queue(easyio::Queue::create(backend, queue_depth)), _multishot_recv(multishot_recv),
      _sync_writes(sync_writes) {
    _fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (_fd < 0) {
        throw_errno(errno, "socket");
    }
    easyio::set_nonblocking(_fd);
    easyio::set_tcp_nodelay(_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    try {
        addr.sin_addr = resolve_ipv4(host);
    } catch (...) {
        ::close(_fd);
        throw;
    }

    bool connected = false;
    std::exception_ptr connect_err;
    easyio::spawn(connect_task(addr, connected, connect_err));
    // Block until connect() finishes -- easybd_client_create() is
    // documented as a blocking call, so pump the queue synchronously here
    // rather than exposing partial-connection state to the caller.
    // step(-1) waits for exactly one completion and returns; since connect
    // is the only thing registered on a freshly-created queue, that one
    // completion is always the connect itself.
    while (!connected) { // NOLINT(bugprone-infinite-loop): connected is set by
        // connect_task, resumed from inside step() -- not visible to the
        // analyzer through the coroutine/reference indirection.
        _queue->step(-1);
    }
    if (connect_err) {
        ::close(_fd);
        std::rethrow_exception(connect_err);
    }

    easyio::spawn(reader_loop());
}

Client::~Client() {
    if (_fd >= 0) {
        // reader_loop() (and any send_request() mid-send) is a detached
        // coroutine that runs forever -- it's still alive (suspended,
        // registered on _fd) right now. cancel_fd() resumes it
        // synchronously with a cancellation error, which its own catch
        // block handles by failing every still-pending request and then
        // returning normally, which is what lets its coroutine frame
        // actually finish and free itself (via DetachedTask's
        // final_suspend). This must happen here, before any member starts
        // being destroyed: _pending is declared (and so destroyed) before
        // _queue, so if cancellation instead happened implicitly as a
        // side effect of ~Queue() later, reader_loop would resume into an
        // already-destroyed _pending -- caught via a real SIGSEGV during
        // manual testing.
        _queue->cancel_fd(_fd);
        ::close(_fd);
    }
}

easyio::Task<void> Client::connect_task(
    const sockaddr_in& addr, bool& done, std::exception_ptr& err) {
    try {
        co_await _queue->connect(_fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    } catch (...) {
        err = std::current_exception();
    }
    done = true;
}

easyio::Task<void> Client::reader_loop() {
    // See kRecvEntrySize/kRecvEntries above. A response larger than one
    // entry is simply reassembled from several chunks -- FramedReader
    // already has to handle chunks splitting or coalescing message
    // boundaries regardless of entry size, so there's no correctness
    // reason to size entries off the max message size.
    auto stream = _queue->recv_stream(_fd, kRecvEntrySize, kRecvEntries, _multishot_recv);
    easyio::FramedReader reader(*stream);

    try {
        for (;;) {
            auto hdr_span = co_await reader.read_exact(sizeof(EasyBDResponseHeader));
            EasyBDResponseHeader resp; // NOLINT(cppcoreguidelines-pro-type-member-init)
            std::memcpy(&resp, hdr_span.data(), sizeof(resp));

            auto it = _pending.find(resp.cid);
            if (it == _pending.end()) {
                // No recovering framing once we can't tell whether a
                // read's payload follows this header or not.
                throw std::runtime_error("easybd: response with unknown cid");
            }
            // Copy out, but leave the map entry in place until the body (if
            // any) is fully read: read_exact() below can throw (a large
            // body's multishot recv can still fail partway through), and if
            // it does, this cid must still be in _pending for the catch
            // block's fail_all_pending() to find and fail it -- erasing it
            // up front, before the body was actually confirmed read, would
            // silently drop this request's callback forever instead, since
            // by the time the exception propagates there'd be no trace of
            // it left to fail. `it` itself isn't used past this point since
            // the co_await below may run other coroutines that insert into
            // _pending and rehash it, invalidating the iterator.
            Pending pending = it->second;

            if (pending.is_read && resp.res > 0) {
                auto body_span = co_await reader.read_exact(static_cast<size_t>(resp.res));
                std::memcpy(pending.buf, body_span.data(), static_cast<size_t>(resp.res));
            }
            _pending.erase(resp.cid);

            ++_total_dispatched;
            pending.cb(resp.res, pending.user_data);
        }
    } catch (...) { // NOLINT(bugprone-empty-catch): handled below, not empty
        // Connection ended (EOF) or broke (protocol/IO error): nothing more
        // will ever arrive, so every still-pending request must be failed
        // now, or its caller would wait for a callback that never comes.
        fail_all_pending(ECONNRESET);
    }
}

easyio::Task<void> Client::send_request(
    uint64_t cid, uint64_t offset, uint64_t size, uint8_t op, const void* payload) {
    EasyBDRequestHeader req{cid, offset, size, op};
    auto guard = co_await easyio::lock_guard(_send_mtx);
    try {
        if (op == EASYBD_OP_WRITE || op == EASYBD_OP_WRITE_SYNC) {
            co_await easyio::send_all(*_queue, _fd, &req, sizeof(req), payload, size);
        } else {
            co_await easyio::send_all(*_queue, _fd, &req, sizeof(req));
        }
    } catch (...) { // NOLINT(bugprone-empty-catch): handled below, not empty
        // Sending this request failed. It may already have been failed by
        // reader_loop's own catch if that noticed the broken connection
        // first -- only fail it here if it's still pending.
        auto it = _pending.find(cid);
        if (it != _pending.end()) {
            Pending pending = it->second;
            _pending.erase(it);
            ++_total_dispatched;
            pending.cb(-ECONNRESET, pending.user_data);
        }
    }
}

void Client::fail_all_pending(int err) {
    std::vector<Pending> pendings;
    pendings.reserve(_pending.size());
    for (auto& [cid, pending] : _pending) {
        pendings.push_back(pending);
    }
    _pending.clear();
    for (auto& pending : pendings) {
        ++_total_dispatched;
        pending.cb(-err, pending.user_data);
    }
}

int Client::pread(void* buf, size_t size, uint64_t offset, EasyBDCallback cb, void* user_data) {
    if (size > EASYBD_MAX_PAYLOAD_SIZE) {
        return -EMSGSIZE;
    }
    uint64_t cid = _next_cid++;
    _pending[cid] = Pending{true, buf, cb, user_data};
    easyio::spawn(send_request(cid, offset, size, EASYBD_OP_READ, nullptr));
    return 0;
}

int Client::pwrite(
    const void* buf, size_t size, uint64_t offset, EasyBDCallback cb, void* user_data) {
    if (size > EASYBD_MAX_PAYLOAD_SIZE) {
        return -EMSGSIZE;
    }
    uint64_t cid = _next_cid++;
    _pending[cid] = Pending{false, nullptr, cb, user_data};
    uint8_t op = _sync_writes ? EASYBD_OP_WRITE_SYNC : EASYBD_OP_WRITE;
    easyio::spawn(send_request(cid, offset, size, op, buf));
    return 0;
}

int Client::wait(int timeout_ms) {
    size_t before = _total_dispatched;
    _queue->step(timeout_ms);
    return static_cast<int>(_total_dispatched - before);
}

} // namespace easybd
