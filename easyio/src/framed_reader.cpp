#include <easyio/framed_reader.hpp>

#include <stdexcept>
#include <system_error>

namespace easyio {

Task<std::span<const std::byte>> FramedReader::read_exact(size_t n) {
    _buf.erase(_buf.begin(), _buf.begin() + static_cast<ptrdiff_t>(_consumed));
    _consumed = 0;

    while (_buf.size() < n) {
        auto chunk = co_await _stream.next();
        if (chunk.error != 0) {
            throw std::system_error(-chunk.error, std::generic_category(), "recv");
        }
        if (chunk.data.empty()) {
            throw std::runtime_error("easyio: connection closed mid-message");
        }
        _buf.insert(_buf.end(), chunk.data.begin(), chunk.data.end());
    }

    _consumed = n;
    co_return std::span<const std::byte>(_buf.data(), n);
}

} // namespace easyio
