#pragma once

#include <cstddef>
#include <span>
#include <vector>

#include <easyio/recv_stream.hpp>
#include <easyio/task.hpp>

namespace easyio {

// Reconstructs exact byte counts out of a RecvStream's raw, boundary-
// agnostic chunks -- a chunk may contain less than one message, or a whole
// message plus the start of the next one. Backend-agnostic: built entirely
// on the RecvStream interface.
class FramedReader {
public:
    explicit FramedReader(RecvStream& stream) noexcept : _stream(stream) {}

    // Reads exactly n bytes, returning a span into this reader's own buffer
    // that stays valid until the next read_exact() call. Throws
    // std::system_error on a stream error, or std::runtime_error if the
    // connection closes before n bytes arrive (a truncated message).
    Task<std::span<const std::byte>> read_exact(size_t n);

    // Attempts to read exactly n bytes as a sequence of live, backend-owned
    // chunks pinned in place -- no copy -- instead of read_exact()'s
    // gather-copy into this reader's own buffer. Meant only for a large
    // payload a caller wants to hand straight to further I/O (e.g. a
    // vectored disk write) without an extra copy; never for small
    // structured fields like a message header, which should keep using
    // read_exact() as before.
    //
    // Returns false (leaving `out`/`pins` untouched) without reading
    // anything if this reader can't currently attempt a zero-copy read --
    // either the backend doesn't support pinning at all (e.g. the libc
    // backend, or io_uring singleshot), or there are leftover bytes from a
    // previous read_exact() call still sitting in this reader's buffer
    // (already copied out of chunks whose ring memory has long since been
    // returned -- there's nothing live left to hand back for that
    // portion). Either way the caller must fall back to read_exact() for
    // the whole read.
    //
    // On success, appends every constituent chunk (in order) to `out` and
    // one PinHandle per stream call that contributed to them, to `pins`;
    // the caller owns all of them and must release() every one exactly
    // once, whenever it's actually done with the memory (typically once an
    // I/O op reading directly out of it has completed). If a chunk that
    // would satisfy part of this read also carries bytes belonging to
    // whatever comes *after* it on the wire (chunks aren't message-
    // aligned -- see RecvStream's doc comment), this call declines instead
    // of trying to split it: there's no way to hand the surplus back to
    // this reader's ordinary buffer once taken from the stream this way,
    // so pretending to succeed would silently drop bytes belonging to the
    // next message. Callers should expect this to decline often and always
    // have a read_exact()-based fallback ready.
    Task<bool> try_read_exact_zerocopy(
        size_t n, std::vector<RecvStream::Chunk>& out, std::vector<RecvStream::PinHandle>& pins);

private:
    RecvStream& _stream;
    std::vector<std::byte> _buf;
    size_t _consumed = 0;
};

} // namespace easyio
