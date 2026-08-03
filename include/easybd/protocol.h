#ifndef EASYBD_PROTOCOL_H
#define EASYBD_PROTOCOL_H

/*
 * Wire protocol for easybd's trivial block-access-over-TCP service.
 *
 * Deliberately minimal: no magic number, no version field, native byte
 * order. This is a benchmarking tool meant to run on a trusted LAN between
 * a purpose-built client and server that are always upgraded together, not
 * a long-lived public wire format -- the usual reasons to pay for framing
 * robustness (garbage-stream detection, cross-version compatibility) don't
 * apply here.
 *
 * Request:
 *   struct EasyBDRequestHeader (fixed size)
 *   + `size` bytes of payload immediately following, but ONLY for
 *     EASYBD_OP_WRITE/EASYBD_OP_WRITE_SYNC. EASYBD_OP_READ carries no
 *     request payload.
 *
 * EASYBD_OP_WRITE vs EASYBD_OP_WRITE_SYNC: identical payload, differ only
 * in the durability guarantee the server gives before responding.
 * EASYBD_OP_WRITE is a plain pwrite() -- with the backing file opened
 * O_DIRECT (the server always does), that bypasses the page cache but
 * says nothing about a volatile write cache the underlying device may
 * still hold the data in, so a "success" response doesn't mean the write
 * survives a power loss. EASYBD_OP_WRITE_SYNC additionally forces that
 * flush (RWF_DSYNC/O_DSYNC-equivalent) before responding. Which one a
 * client sends is meant to mirror fio's own --sync option (see the fio
 * engine, which reads td->o.sync_io) rather than being a separate ad hoc
 * easybd setting.
 *
 * Response:
 *   struct EasyBDResponseHeader (fixed size), echoing the request's cid
 *   + `res` bytes of payload immediately following, but ONLY when this is
 *     the response to an EASYBD_OP_READ request AND res >= 0.
 *
 * cid is a client-assigned, per-connection request id with no meaning to
 * the server beyond echoing it back -- it exists purely so a client may
 * pipeline multiple in-flight requests on one connection and match
 * responses (which may arrive out of order) back to the request that
 * caused them.
 *
 * offset/size are in bytes; when the backing file was opened O_DIRECT
 * (the server always does), both must be aligned to the backing device's
 * logical block size (typically 512 or 4096 bytes) or the request fails
 * with -EINVAL, exactly as a raw O_DIRECT pread/pwrite would.
 *
 * `size` (request) and `res` (a positive read response) must not exceed
 * EASYBD_MAX_PAYLOAD_SIZE, which is purely a sanity/DoS limit on how much
 * one request can ask for -- it has no bearing on how the client/server
 * size their recv_stream() ring buffers (see the kRecvEntrySize/
 * kRecvEntries constants in client.cpp/session.cpp), since a response
 * larger than one ring entry is simply reassembled from several chunks
 * like any other. A request over the limit gets an EMSGSIZE error
 * response (write) or is rejected before it can even be issued (read,
 * checked client-side since the size comes from the caller, not the
 * wire).
 */

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
    EASYBD_OP_READ = 0,
    EASYBD_OP_WRITE = 1,
    EASYBD_OP_WRITE_SYNC = 2,
};

/* See the payload-size note above. */
#define EASYBD_MAX_PAYLOAD_SIZE ((uint64_t)4 * 1024 * 1024)

#if defined(__GNUC__) || defined(__clang__)
#define EASYBD_PACKED __attribute__((packed))
#else
#error "easybd/protocol.h requires a compiler supporting __attribute__((packed))"
#endif

struct EasyBDRequestHeader {
    uint64_t cid;
    uint64_t offset;
    uint64_t size;
    uint8_t op;
} EASYBD_PACKED;

struct EasyBDResponseHeader {
    uint64_t cid;
    int64_t res; /* >= 0: bytes (read: bytes read/to follow; write: bytes written); < 0: -errno */
} EASYBD_PACKED;

#ifdef __cplusplus
}
#endif

#endif /* EASYBD_PROTOCOL_H */
