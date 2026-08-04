#include <easyio/framed_reader.hpp>

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

} // namespace easyio
