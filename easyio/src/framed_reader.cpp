#include <easyio/framed_reader.hpp>

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <system_error>

namespace easyio {

namespace {
// Upper bound on how many already-arrived chunks next_batch() drains in one
// call. Just a capacity for FramedReader's stack buffer, not a correctness
// limit: a backend with more than this many chunks pending simply gets
// drained over more than one next_batch() call. Sized generously above
// EASYBD_MAX_PAYLOAD_SIZE / typical recv_stream entry_size (4MiB / 128KiB =
// 32) so a whole max-size message's worth of chunks fit in a single batch
// whenever they've already all arrived together.
constexpr size_t kBatchCapacity = 64;
} // namespace

Task<std::span<const std::byte>> FramedReader::read_exact(size_t n) {
    _buf.erase(_buf.begin(), _buf.begin() + static_cast<ptrdiff_t>(_consumed));
    _consumed = 0;

    RecvStream::Chunk batch[kBatchCapacity];
    while (_buf.size() < n) {
        size_t count = co_await _stream.next_batch(batch, n - _buf.size());
        for (size_t i = 0; i < count; ++i) {
            const RecvStream::Chunk& chunk = batch[i];
            if (chunk.error != 0) {
                throw std::system_error(-chunk.error, std::generic_category(), "recv");
            }
            if (chunk.data.empty()) {
                throw std::runtime_error("easyio: connection closed mid-message");
            }
            _buf.insert(_buf.end(), chunk.data.begin(), chunk.data.end());
        }
    }

    _consumed = n;
    co_return std::span<const std::byte>(_buf.data(), n);
}

Task<void> FramedReader::read_exact_into(void* dest, size_t n) {
    _buf.erase(_buf.begin(), _buf.begin() + static_cast<ptrdiff_t>(_consumed));
    _consumed = 0;

    size_t got = 0;
    if (!_buf.empty()) {
        // Leftover bytes from a previous read's overshoot (see below) --
        // hand them straight to the caller's buffer instead of re-fetching
        // them from the stream.
        got = std::min(_buf.size(), n);
        std::memcpy(dest, _buf.data(), got);
        _consumed = got; // erased at the top of the next read_exact*() call
    }

    RecvStream::Chunk batch[kBatchCapacity];
    while (got < n) {
        size_t count = co_await _stream.next_batch(batch, n - got);
        for (size_t i = 0; i < count; ++i) {
            const RecvStream::Chunk& chunk = batch[i];
            if (chunk.error != 0) {
                throw std::system_error(-chunk.error, std::generic_category(), "recv");
            }
            if (chunk.data.empty()) {
                throw std::runtime_error("easyio: connection closed mid-message");
            }
            size_t take = std::min(chunk.data.size(), n - got);
            std::memcpy(static_cast<std::byte*>(dest) + got, chunk.data.data(), take);
            got += take;
            if (take < chunk.data.size()) {
                // This chunk also carries bytes belonging to whatever
                // follows this read on the wire (chunks aren't message-
                // aligned) -- unlike read_exact(), this method has nowhere
                // else to put them, so stash the remainder in _buf for
                // whichever read_exact()/read_exact_into() call comes
                // next, same as read_exact()'s own overshoot handling.
                // By this point any prior leftover _buf content has
                // necessarily already been fully drained into `dest`
                // above (that's the only way this loop could still be
                // running), so it's safe to fully replace it here --
                // _consumed must reset to 0 too, since it's now stale
                // relative to this entirely-fresh content.
                _buf.assign(chunk.data.begin() + take, chunk.data.end());
                _consumed = 0;
            }
        }
    }
}

} // namespace easyio
