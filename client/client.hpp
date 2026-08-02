#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include <easybd/client.h>
#include <easyio/mutex.hpp>
#include <easyio/queue.hpp>
#include <easyio/task.hpp>

struct sockaddr_in;

namespace easybd {

// One TCP connection + one easyio::Queue. A single reader coroutine parses
// the response byte stream (multishot recv via FramedReader) for the
// connection's whole lifetime; each pread()/pwrite() call spawns an
// independent send task so several requests can be in flight at once
// (pipelined, correlated by cid) -- mirrors the server's session design.
class Client {
public:
    // multishot_recv: see --feature-multishot on the fio engine / whatever
    // else drives this client; only meaningful with backend ==
    // easyio::Backend::IoUring (see Queue::recv_stream()'s doc comment).
    Client(
        const std::string& host, uint16_t port, easyio::Backend backend, unsigned int queue_depth,
        bool multishot_recv);
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    int pread(void* buf, size_t size, uint64_t offset, EasyBDCallback cb, void* user_data);
    int pwrite(const void* buf, size_t size, uint64_t offset, EasyBDCallback cb, void* user_data);

    // Returns the number of callbacks invoked. May throw std::system_error
    // if the underlying queue hits an unrecoverable error (extremely
    // rare); the C shim translates that into a negative-errno return.
    int wait(int timeout_ms);

private:
    struct Pending {
        bool is_read;
        void* buf; // read target; unused for writes
        EasyBDCallback cb;
        void* user_data;
    };

    easyio::Task<void> connect_task(const sockaddr_in& addr, bool& done, std::exception_ptr& err);
    easyio::Task<void> reader_loop();
    easyio::Task<void> send_request(
        uint64_t cid, uint64_t offset, uint64_t size, uint8_t op, const void* payload);
    void fail_all_pending(int err);

    std::unique_ptr<easyio::Queue> _queue;
    bool _multishot_recv;
    int _fd = -1;
    uint64_t _next_cid = 1;
    std::unordered_map<uint64_t, Pending> _pending;
    easyio::Mutex _send_mtx;
    size_t _total_dispatched = 0;
};

} // namespace easybd
