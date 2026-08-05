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

    // Like read_exact(), but copies the n bytes directly into `dest`
    // (which must have room for n bytes) instead of into this reader's own
    // buffer. Saves a copy for a caller that already has its own
    // destination ready (e.g. a page-aligned buffer it's about to hand to
    // a direct-I/O write) -- read_exact() would first gather the same
    // bytes into this reader's buffer and the caller would then have to
    // copy them out a second time; this does it in one pass instead.
    // Same error/EOF behavior as read_exact().
    Task<void> read_exact_into(void* dest, size_t n);

private:
    RecvStream& _stream;
    std::vector<std::byte> _buf;
    size_t _consumed = 0;
};

} // namespace easyio
