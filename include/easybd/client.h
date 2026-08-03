#ifndef EASYBD_CLIENT_H
#define EASYBD_CLIENT_H

/*
 * C client library for the easybd wire protocol (see easybd/protocol.h).
 *
 * Internally implemented in C++ on top of easyio (multishot recv for
 * response parsing, coroutines driving the connection), but exposes a
 * plain callback-based C API: callers (e.g. an fio ioengine) drive
 * progress by calling easybd_client_wait() in a loop, exactly like
 * librawio/librawstor's rawio_wait() pattern.
 *
 * One EasyBDClient owns exactly one TCP connection and one async queue.
 * Multiple requests may be in flight concurrently on the same client
 * (pipelined, matching the protocol's cid field) -- that's what lets a
 * single connection exercise iodepth>1 against the server.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct EasyBDClient EasyBDClient;

typedef enum {
    EASYBD_BACKEND_IO_URING = 0,
    EASYBD_BACKEND_LIBC = 1,
} EasyBDBackend;

/* Nonzero iff this build has io_uring support (i.e. was NOT built
 * --without-liburing); EASYBD_BACKEND_IO_URING is only a valid argument to
 * easybd_client_create() when this returns nonzero. */
int easybd_io_uring_available(void);

/* Connects to host:port and returns 0 on success (with *out set), or a
 * negative errno on failure (*out left untouched). Blocks the calling
 * thread until the connection completes.
 *
 * feature_multishot: nonzero to use io_uring multishot recv for parsing
 * responses off this connection, zero for an ordinary recv per chunk (see
 * easyio's Queue::recv_stream() doc comment for what the difference is).
 * Only meaningful with backend == EASYBD_BACKEND_IO_URING; ignored (there
 * is no multishot recv to begin with) with EASYBD_BACKEND_LIBC.
 *
 * sync_writes: nonzero to have easybd_client_pwrite() request durability
 * (the server flushes to the storage device before responding) at the
 * cost of write latency/throughput, zero for a plain write with no such
 * guarantee. Meant to mirror whatever "synchronous write IO" setting the
 * caller itself exposes (e.g. fio's --sync) rather than being a separate
 * ad hoc easybd setting -- see protocol.h's doc comment on
 * EASYBD_OP_WRITE vs EASYBD_OP_WRITE_SYNC. */
int easybd_client_create(
    const char* host, uint16_t port, EasyBDBackend backend, unsigned int queue_depth,
    int feature_multishot, int sync_writes, EasyBDClient** out);

/* Closes the connection. Any requests still in flight at this point never
 * get their callback invoked. */
void easybd_client_destroy(EasyBDClient* client);

/* res >= 0: bytes transferred (read: bytes read; write: bytes written).
 * res < 0: -errno. Called at most once per request, from within
 * easybd_client_wait(). */
typedef void (*EasyBDCallback)(int64_t res, void* user_data);

/* Queues a request; it is actually written to the connection the next time
 * easybd_client_wait() pumps the queue, not synchronously by this call.
 * Returns 0 if the request was queued (the callback WILL eventually fire,
 * from some future easybd_client_wait() call -- including with a
 * connection-error result if the connection breaks before a response
 * arrives), or a negative errno if it could not be queued at all (the
 * callback is never invoked in that case).
 *
 * `buf` must stay valid until the callback fires. offset/size should be
 * aligned to the server's backing file's logical block size if the server
 * opened it O_DIRECT (the reference server always does), or the request
 * will come back with a negative result. size must not exceed
 * EASYBD_MAX_PAYLOAD_SIZE, or this call itself returns -EMSGSIZE. */
int easybd_client_pread(
    EasyBDClient* client, void* buf, size_t size, uint64_t offset, EasyBDCallback cb,
    void* user_data);
int easybd_client_pwrite(
    EasyBDClient* client, const void* buf, size_t size, uint64_t offset, EasyBDCallback cb,
    void* user_data);

/* Pumps the connection for at most timeout_ms (a negative value blocks
 * indefinitely): sends queued requests, receives responses, invokes
 * callbacks for whichever requests completed. Returns the number of
 * callbacks invoked during this call (0 if timeout_ms elapsed with nothing
 * to do), or a negative errno on an unrecoverable connection error (in
 * which case every still-pending request's callback has already been
 * invoked with a negative result, and the client is no longer usable
 * except to destroy). */
int easybd_client_wait(EasyBDClient* client, int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* EASYBD_CLIENT_H */
