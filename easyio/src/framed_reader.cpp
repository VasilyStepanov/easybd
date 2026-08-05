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

Task<bool> FramedReader::try_read_exact_zerocopy(
    size_t n, std::vector<RecvStream::Chunk>& out, std::vector<RecvStream::PinHandle>& pins) {
    _buf.erase(_buf.begin(), _buf.begin() + static_cast<ptrdiff_t>(_consumed));
    _consumed = 0;
    if (!_buf.empty()) {
        // Leftover bytes from a previous read are already sitting here,
        // copied out of chunks whose ring memory has long since been
        // returned -- nothing live left to hand back for that portion.
        co_return false;
    }

    // Used when backing out after already taking *real* data from the
    // stream (currently: only the overshoot case below) -- copies
    // everything gathered into `out` so far, plus batch[from..count) (this
    // round's chunks that were never added to `out`, e.g. because an
    // overshoot was detected partway through them), into this reader's
    // ordinary buffer. That's exactly what a plain read_exact() would
    // itself have accumulated by this point, so nothing taken here is ever
    // lost: the caller's read_exact() fallback either needs no further I/O
    // at all (if this had already gathered all n bytes' worth) or picks up
    // right where this left off. Only then releases every pin and clears
    // both output params.
    auto preserve_and_abandon = [&](const RecvStream::Chunk* batch, size_t from, size_t count) {
        for (const auto& chunk : out) {
            _buf.insert(_buf.end(), chunk.data.begin(), chunk.data.end());
        }
        for (size_t i = from; i < count; ++i) {
            _buf.insert(_buf.end(), batch[i].data.begin(), batch[i].data.end());
        }
        for (auto& p : pins) {
            _stream.release(std::move(p));
        }
        out.clear();
        pins.clear();
    };

    try {
        size_t total = 0;
        RecvStream::Chunk batch[kBatchCapacity];
        while (total < n) {
            size_t count = co_await _stream.next_batch(batch, n - total);
            RecvStream::PinHandle pin = _stream.pin_last();
            if (!pin) {
                // Can't pin -- unsupported backend, or (per pin_last()'s
                // doc comment) this round drained no real chunk to begin
                // with, just an EOF/error sentinel that next round's
                // read_exact() will see and handle identically. Either
                // way, nothing real was taken this round, so there's
                // nothing new to preserve beyond whatever's already in
                // `out` from earlier rounds.
                preserve_and_abandon(batch, 0, 0);
                co_return false;
            }
            pins.push_back(std::move(pin));

            for (size_t i = 0; i < count; ++i) {
                const RecvStream::Chunk& chunk = batch[i];
                if (chunk.error != 0) {
                    // Caught below. Connection's failing regardless of
                    // this read's own bookkeeping (handle_connection tears
                    // down the whole connection on any exception here), so
                    // there's nothing worth preserving -- see the catch
                    // block.
                    throw std::system_error(-chunk.error, std::generic_category(), "recv");
                }
                if (chunk.data.empty()) {
                    throw std::runtime_error("easyio: connection closed mid-message");
                }
                if (total + chunk.data.size() > n) {
                    // This chunk (and, in this same batch, everything
                    // after it) carries bytes belonging to whatever
                    // follows this read on the wire -- chunks aren't
                    // message-aligned, so there's no way to split just the
                    // part this read needs out of pinned memory. Preserve
                    // all of it into _buf instead of discarding it.
                    preserve_and_abandon(batch, i, count);
                    co_return false;
                }
                total += chunk.data.size();
                out.push_back(chunk);
            }
        }
    } catch (...) {
        // A stream error/EOF chunk, or pin_last() itself throwing (see its
        // doc comment). Unlike the overshoot case above, nothing here is
        // worth preserving into _buf: the connection is being torn down
        // either way (handle_connection's read loop ends on any exception
        // from this call), so nothing will ever come back to read it.
        for (auto& p : pins) {
            _stream.release(std::move(p));
        }
        out.clear();
        pins.clear();
        throw;
    }

    co_return true;
}

} // namespace easyio
