#include <easyio/framed_reader.hpp>
#include <easyio/mutex.hpp>
#include <easyio/queue.hpp>
#include <easyio/task.hpp>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

namespace {

// Drives one or more concurrent tasks to completion on one Queue,
// propagating the first exception either raised (test assertions inside
// the tasks throw).
easyio::Task<void> track(easyio::Task<void> t, int& remaining, std::exception_ptr& err) {
    try {
        co_await std::move(t);
    } catch (...) {
        err = std::current_exception();
    }
    --remaining;
}

// step() does exactly one wait-then-dispatch cycle and returns (see
// queue.hpp) -- it's the caller's job to keep calling it until whatever
// they're waiting for is done. A bounded per-call timeout means a
// genuinely-stuck test fails with a clear timeout instead of hanging CI
// forever.
void run_until_done(easyio::Queue& queue, const int& remaining) {
    while (remaining > 0) {
        queue.step(2000);
    }
}

void run_pair(easyio::Queue& queue, easyio::Task<void> a, easyio::Task<void> b) {
    int remaining = 2;
    std::exception_ptr err_a, err_b;
    easyio::spawn(track(std::move(a), remaining, err_a));
    easyio::spawn(track(std::move(b), remaining, err_b));
    run_until_done(queue, remaining);
    if (err_a) {
        std::rethrow_exception(err_a);
    }
    if (err_b) {
        std::rethrow_exception(err_b);
    }
}

easyio::Task<void> file_roundtrip(easyio::Queue& queue, std::string path) {
    static const char msg[] = "hello easyio";

    int fd = co_await queue.open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0600);
    size_t written = co_await queue.pwrite(fd, msg, sizeof(msg), 0);
    if (written != sizeof(msg)) {
        throw std::runtime_error("short write");
    }

    char buf[sizeof(msg)] = {};
    size_t got = co_await queue.pread(fd, buf, sizeof(buf), 0);
    if (got != sizeof(msg) || std::memcmp(buf, msg, sizeof(msg)) != 0) {
        throw std::runtime_error("read back mismatch");
    }

    co_await queue.close(fd);
}

easyio::Task<void> server_echo_one(
    easyio::Queue& queue, int listen_fd, std::string* out, bool multishot = true) {
    int client_fd = co_await queue.accept(listen_fd);
    auto stream = queue.recv_stream(client_fd, 4096, 4, multishot);

    std::string data;
    for (;;) {
        auto chunk = co_await stream->next();
        if (chunk.error != 0) {
            throw std::system_error(-chunk.error, std::generic_category(), "recv");
        }
        if (chunk.data.empty()) {
            break;
        }
        data.append(reinterpret_cast<const char*>(chunk.data.data()), chunk.data.size());
    }

    *out = std::move(data);
    co_await queue.close(client_fd);
}

easyio::Task<void> client_send_one(easyio::Queue& queue, int fd, sockaddr_in addr, std::string msg) {
    co_await queue.connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    size_t off = 0;
    while (off < msg.size()) {
        off += co_await queue.send(fd, msg.data() + off, msg.size() - off);
    }
    co_await queue.close(fd);
}

int make_listen_socket(uint16_t* out_port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "socket");
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::system_error(errno, std::generic_category(), "bind");
    }
    if (::listen(fd, 16) < 0) {
        throw std::system_error(errno, std::generic_category(), "listen");
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
        throw std::system_error(errno, std::generic_category(), "getsockname");
    }
    *out_port = ntohs(addr.sin_port);
    easyio::set_nonblocking(fd);
    return fd;
}

class EasyioTest : public ::testing::TestWithParam<easyio::Backend> {};

TEST_P(EasyioTest, FileReadWriteRoundtrip) {
    auto queue = easyio::Queue::create(GetParam(), 32);
    char path[] = "/tmp/easyio_test_XXXXXX";
    int fd = ::mkstemp(path);
    ASSERT_GE(fd, 0);
    ::close(fd);

    int remaining = 1;
    std::exception_ptr err;
    easyio::spawn(track(file_roundtrip(*queue, path), remaining, err));
    run_until_done(*queue, remaining);
    ::unlink(path);
    if (err) {
        std::rethrow_exception(err);
    }
}

TEST_P(EasyioTest, SocketSendRecvStream) {
    auto queue = easyio::Queue::create(GetParam(), 32);

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    easyio::set_nonblocking(client_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    std::string received;
    std::string message(70000, 'x'); // larger than one recv_stream entry, forces >1 chunk

    run_pair(
        *queue, server_echo_one(*queue, listen_fd, &received),
        client_send_one(*queue, client_fd, addr, message));

    ASSERT_EQ(received.size(), message.size());
    EXPECT_EQ(received, message);
    ::close(listen_fd);
}

easyio::Task<void> framed_server(
    easyio::Queue& queue, int listen_fd, std::vector<std::string>* out, bool multishot = true) {
    int client_fd = co_await queue.accept(listen_fd);
    auto stream = queue.recv_stream(client_fd, 4096, 4, multishot);
    easyio::FramedReader reader(*stream);

    std::vector<std::string> messages;
    for (int i = 0; i < 5; ++i) {
        auto len_span = co_await reader.read_exact(4);
        uint32_t len;
        std::memcpy(&len, len_span.data(), 4);
        auto body_span = co_await reader.read_exact(len);
        messages.emplace_back(reinterpret_cast<const char*>(body_span.data()), body_span.size());
    }
    *out = std::move(messages);
    co_await queue.close(client_fd);
}

easyio::Task<void> framed_client(
    easyio::Queue& queue, int fd, sockaddr_in addr, std::vector<std::string> messages) {
    co_await queue.connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));

    for (const auto& m : messages) {
        auto len = static_cast<uint32_t>(m.size());
        std::string frame(reinterpret_cast<char*>(&len), sizeof(len));
        frame += m;

        // Send in tiny, irregular pieces so chunk boundaries never line up
        // with message boundaries -- exercises both "partial" (need more
        // data) and "coalesced" (more than one message's worth already
        // buffered) cases in FramedReader.
        size_t off = 0;
        while (off < frame.size()) {
            size_t piece = std::min<size_t>(3, frame.size() - off);
            size_t sent = 0;
            while (sent < piece) {
                sent += co_await queue.send(fd, frame.data() + off + sent, piece - sent);
            }
            off += piece;
        }
    }
    co_await queue.close(fd);
}

TEST_P(EasyioTest, FramedReaderExactBoundaries) {
    auto queue = easyio::Queue::create(GetParam(), 32);

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    easyio::set_nonblocking(client_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    std::vector<std::string> expected{"a", "hello world", std::string(5000, 'z'), "", "last"};
    std::vector<std::string> received;

    run_pair(
        *queue, framed_server(*queue, listen_fd, &received),
        framed_client(*queue, client_fd, addr, expected));

    EXPECT_EQ(received, expected);
    ::close(listen_fd);
}

// Minimal WaitGroup so a test coroutine can wait for N independently
// spawned (fire-and-forget) tasks to finish -- not part of easyio itself,
// this is purely test plumbing to get real concurrency between the two
// send_pattern() producers below (spawn() starts a task eagerly, unlike
// co_await-ing a Task<T> in sequence).
struct WaitGroup {
    int remaining = 0;
    std::coroutine_handle<> waiter;

    struct Awaiter {
        WaitGroup* wg;
        [[nodiscard]] bool await_ready() const noexcept { return wg->remaining == 0; }
        void await_suspend(std::coroutine_handle<> h) noexcept { wg->waiter = h; }
        void await_resume() const noexcept {}
    };

    Awaiter wait() noexcept { return Awaiter{this}; }

    void done() noexcept {
        if (--remaining == 0 && waiter) {
            std::exchange(waiter, nullptr).resume();
        }
    }
};

easyio::Task<void> tracked(easyio::Task<void> t, WaitGroup& wg) {
    co_await std::move(t);
    wg.done();
}

easyio::Task<void> send_pattern(
    easyio::Queue& queue, int fd, easyio::Mutex& mtx, char marker, size_t count) {
    auto guard = co_await easyio::lock_guard(mtx);
    std::string msg(count, marker);
    // Send in small pieces, each a real suspend/resume round trip, so the
    // other producer has every opportunity to interleave if the mutex
    // didn't actually serialize them.
    size_t off = 0;
    while (off < msg.size()) {
        size_t piece = std::min<size_t>(3, msg.size() - off);
        co_await easyio::send_all(queue, fd, msg.data() + off, piece);
        off += piece;
    }
}

easyio::Task<void> mutex_server(easyio::Queue& queue, int listen_fd) {
    int client_fd = co_await queue.accept(listen_fd);
    easyio::Mutex mtx;
    WaitGroup wg{2, nullptr};
    easyio::spawn(tracked(send_pattern(queue, client_fd, mtx, 'A', 3000), wg));
    easyio::spawn(tracked(send_pattern(queue, client_fd, mtx, 'B', 3000), wg));
    co_await wg.wait();
    co_await queue.close(client_fd);
}

easyio::Task<void> recv_all_into(easyio::Queue& queue, int fd, std::string* out) {
    auto stream = queue.recv_stream(fd, 4096, 4);
    std::string data;
    for (;;) {
        auto chunk = co_await stream->next();
        if (chunk.error != 0) {
            throw std::system_error(-chunk.error, std::generic_category(), "recv");
        }
        if (chunk.data.empty()) {
            break;
        }
        data.append(reinterpret_cast<const char*>(chunk.data.data()), chunk.data.size());
    }
    *out = std::move(data);
}

easyio::Task<void> mutex_client(
    easyio::Queue& queue, int fd, sockaddr_in addr, std::string* out) {
    co_await queue.connect(fd, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr));
    co_await recv_all_into(queue, fd, out);
    co_await queue.close(fd);
}

TEST_P(EasyioTest, MutexSerializesConcurrentSends) {
    auto queue = easyio::Queue::create(GetParam(), 32);

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    easyio::set_nonblocking(client_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    std::string received;
    run_pair(
        *queue, mutex_server(*queue, listen_fd), mutex_client(*queue, client_fd, addr, &received));

    // Both producers' messages must show up whole and non-interleaved,
    // regardless of which one happened to acquire the mutex first.
    std::string a(3000, 'A');
    std::string b(3000, 'B');
    EXPECT_TRUE(received == a + b || received == b + a)
        << "received.size()=" << received.size();
    ::close(listen_fd);
}

easyio::Task<void> pin_decline_server(easyio::Queue& queue, int listen_fd, bool* pin_was_valid) {
    int fd = co_await queue.accept(listen_fd);
    // multishot=false forces the "ordinary recv" path on every backend
    // (see recv_stream()'s doc comment): the libc backend never supports
    // pinning, and io_uring's own singleshot mode doesn't either -- only
    // io_uring multishot does (see EasyioZeroCopyPin below).
    auto stream = queue.recv_stream(fd, 4096, 4, /*multishot=*/false);
    auto chunk = co_await stream->next();
    if (chunk.error != 0) {
        throw std::system_error(-chunk.error, std::generic_category(), "recv");
    }
    auto pin = stream->pin_last();
    *pin_was_valid = static_cast<bool>(pin);
    if (*pin_was_valid) {
        stream->release(std::move(pin));
    }
    co_await queue.close(fd);
}

TEST_P(EasyioTest, PinLastDeclinedWithoutMultishot) {
    auto queue = easyio::Queue::create(GetParam(), 32);

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    easyio::set_nonblocking(client_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    bool pin_was_valid = true;
    run_pair(
        *queue, pin_decline_server(*queue, listen_fd, &pin_was_valid),
        client_send_one(*queue, client_fd, addr, std::string("hi")));

    EXPECT_FALSE(pin_was_valid);
    ::close(listen_fd);
}

// --feature-multishot off is only a distinct code path on the io_uring
// backend (see queue.hpp's recv_stream() doc comment) -- these two mirror
// SocketSendRecvStream/FramedReaderExactBoundaries above but force the
// SingleShotRecvStreamImpl path explicitly, since the parameterized
// EasyioTest suite above only ever exercises the (default) multishot=true
// path.
TEST(EasyioSingleShotRecvStream, SocketSendRecvStream) {
    if (!easyio::io_uring_available()) {
        GTEST_SKIP() << "built --without-liburing";
    }
    auto queue = easyio::Queue::create(easyio::Backend::IoUring, 32);

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    easyio::set_nonblocking(client_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    std::string received;
    std::string message(70000, 'x'); // forces several resubmit cycles

    run_pair(
        *queue, server_echo_one(*queue, listen_fd, &received, /*multishot=*/false),
        client_send_one(*queue, client_fd, addr, message));

    ASSERT_EQ(received.size(), message.size());
    EXPECT_EQ(received, message);
    ::close(listen_fd);
}

TEST(EasyioSingleShotRecvStream, FramedReaderExactBoundaries) {
    if (!easyio::io_uring_available()) {
        GTEST_SKIP() << "built --without-liburing";
    }
    auto queue = easyio::Queue::create(easyio::Backend::IoUring, 32);

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    easyio::set_nonblocking(client_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    std::vector<std::string> expected{"a", "hello world", std::string(5000, 'z'), "", "last"};
    std::vector<std::string> received;

    run_pair(
        *queue, framed_server(*queue, listen_fd, &received, /*multishot=*/false),
        framed_client(*queue, client_fd, addr, expected));

    EXPECT_EQ(received, expected);
    ::close(listen_fd);
}

// Reads the first chunk, pins it (see RecvStream::pin_last()'s doc
// comment), then keeps draining the rest of a large message through the
// same small (4-entry) ring before finally releasing it. If pin_last()
// didn't actually exclude that chunk's ring slot from being recycled, the
// slot's buf_idx would inevitably get reused (and its content overwritten)
// well before the whole message arrives -- entries=4 with a ~70000-byte
// message needs on the order of 18 chunks, cycling through the ring
// several times over.
easyio::Task<void> pin_recycle_server(
    easyio::Queue& queue, int listen_fd, std::string* out, std::string* pinned_snapshot,
    std::string* pinned_reread, bool* pin_was_valid) {
    int fd = co_await queue.accept(listen_fd);
    auto stream = queue.recv_stream(fd, 4096, 4, /*multishot=*/true);

    auto first = co_await stream->next();
    if (first.error != 0) {
        throw std::system_error(-first.error, std::generic_category(), "recv");
    }
    if (first.data.empty()) {
        throw std::runtime_error("connection closed before any data");
    }
    *pinned_snapshot =
        std::string(reinterpret_cast<const char*>(first.data.data()), first.data.size());
    auto pin = stream->pin_last();
    *pin_was_valid = static_cast<bool>(pin);

    std::string data(*pinned_snapshot);
    for (;;) {
        auto chunk = co_await stream->next();
        if (chunk.error != 0) {
            throw std::system_error(-chunk.error, std::generic_category(), "recv");
        }
        if (chunk.data.empty()) {
            break;
        }
        data.append(reinterpret_cast<const char*>(chunk.data.data()), chunk.data.size());
    }

    if (*pin_was_valid) {
        // Read the pinned span back now, right before releasing it --
        // still pointing at the exact same ring memory as when first
        // captured above, so any mismatch here means something else was
        // allowed to reuse that slot while it was supposedly pinned.
        *pinned_reread =
            std::string(reinterpret_cast<const char*>(first.data.data()), first.data.size());
        stream->release(std::move(pin));
    }

    *out = std::move(data);
    co_await queue.close(fd);
}

TEST(EasyioZeroCopyPin, PinnedChunkSurvivesRingRecycling) {
    if (!easyio::io_uring_available()) {
        GTEST_SKIP() << "built --without-liburing";
    }
    auto queue = easyio::Queue::create(easyio::Backend::IoUring, 32);

    uint16_t port = 0;
    int listen_fd = make_listen_socket(&port);

    int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(client_fd, 0);
    easyio::set_nonblocking(client_fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    std::string first_marker = "PIN-ME-0123456789";
    std::string message = first_marker + std::string(70000 - first_marker.size(), 'z');

    std::string received;
    std::string pinned_snapshot;
    std::string pinned_reread;
    bool pin_was_valid = false;

    run_pair(
        *queue,
        pin_recycle_server(
            *queue, listen_fd, &received, &pinned_snapshot, &pinned_reread, &pin_was_valid),
        client_send_one(*queue, client_fd, addr, message));

    ASSERT_EQ(received.size(), message.size());
    EXPECT_EQ(received, message);
    ASSERT_TRUE(pin_was_valid) << "io_uring multishot recv_stream should support pin_last()";
    EXPECT_EQ(pinned_reread, pinned_snapshot)
        << "pinned chunk's memory changed before release() -- its ring slot was recycled early";
    ::close(listen_fd);
}

std::vector<easyio::Backend> available_backends() {
    std::vector<easyio::Backend> backends{easyio::Backend::Libc};
    if (easyio::io_uring_available()) {
        backends.push_back(easyio::Backend::IoUring);
    }
    return backends;
}

INSTANTIATE_TEST_SUITE_P(
    Backends, EasyioTest, ::testing::ValuesIn(available_backends()),
    [](const ::testing::TestParamInfo<easyio::Backend>& info) {
        return std::string(easyio::backend_name(info.param));
    });

} // namespace
