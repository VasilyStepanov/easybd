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
 *   struct EasybdRequestHeader (fixed size)
 *   + `size` bytes of payload immediately following, but ONLY for
 *     EASYBD_OP_WRITE. EASYBD_OP_READ carries no request payload.
 *
 * Response:
 *   struct EasybdResponseHeader (fixed size), echoing the request's cid
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
 */

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
    EASYBD_OP_READ = 0,
    EASYBD_OP_WRITE = 1,
};

#if defined(__GNUC__) || defined(__clang__)
#define EASYBD_PACKED __attribute__((packed))
#else
#error "easybd/protocol.h requires a compiler supporting __attribute__((packed))"
#endif

struct EasybdRequestHeader {
    uint64_t cid;
    uint64_t offset;
    uint64_t size;
    uint8_t op;
} EASYBD_PACKED;

struct EasybdResponseHeader {
    uint64_t cid;
    int64_t res; /* >= 0: bytes (read: bytes read/to follow; write: bytes written); < 0: -errno */
} EASYBD_PACKED;

#ifdef __cplusplus
}
#endif

#endif /* EASYBD_PROTOCOL_H */
