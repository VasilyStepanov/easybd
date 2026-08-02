#include <easybd/client.h>

#include <cerrno>
#include <new>
#include <system_error>

#include <easyio/queue.hpp>

#include "client.hpp"

namespace {

easyio::Backend to_easyio_backend(EasyBDBackend backend) {
    switch (backend) {
    case EASYBD_BACKEND_IO_URING:
        return easyio::Backend::IoUring;
    case EASYBD_BACKEND_LIBC:
        return easyio::Backend::Libc;
    }
    throw std::invalid_argument("easybd: unknown backend");
}

} // namespace

extern "C" {

int easybd_io_uring_available(void) { return easyio::io_uring_available() ? 1 : 0; }

int easybd_client_create(
    const char* host, uint16_t port, EasyBDBackend backend, unsigned int queue_depth,
    int feature_multishot, EasyBDClient** out) {
    try {
        auto* client = new easybd::Client(
            host, port, to_easyio_backend(backend), queue_depth, feature_multishot != 0);
        *out = reinterpret_cast<EasyBDClient*>(client);
        return 0;
    } catch (const std::system_error& e) {
        return -e.code().value();
    } catch (const std::bad_alloc&) {
        return -ENOMEM;
    } catch (const std::exception&) {
        return -EINVAL;
    } catch (...) {
        return -EINVAL;
    }
}

void easybd_client_destroy(EasyBDClient* client) {
    delete reinterpret_cast<easybd::Client*>(client); // NOLINT(cppcoreguidelines-owning-memory)
}

int easybd_client_pread(
    EasyBDClient* client, void* buf, size_t size, uint64_t offset, EasyBDCallback cb,
    void* user_data) {
    try {
        return reinterpret_cast<easybd::Client*>(client)->pread(buf, size, offset, cb, user_data);
    } catch (const std::system_error& e) {
        return -e.code().value();
    } catch (const std::bad_alloc&) {
        return -ENOMEM;
    } catch (...) {
        return -EINVAL;
    }
}

int easybd_client_pwrite(
    EasyBDClient* client, const void* buf, size_t size, uint64_t offset, EasyBDCallback cb,
    void* user_data) {
    try {
        return reinterpret_cast<easybd::Client*>(client)->pwrite(
            buf, size, offset, cb, user_data);
    } catch (const std::system_error& e) {
        return -e.code().value();
    } catch (const std::bad_alloc&) {
        return -ENOMEM;
    } catch (...) {
        return -EINVAL;
    }
}

int easybd_client_wait(EasyBDClient* client, int timeout_ms) {
    try {
        return reinterpret_cast<easybd::Client*>(client)->wait(timeout_ms);
    } catch (const std::system_error& e) {
        return -e.code().value();
    } catch (const std::bad_alloc&) {
        return -ENOMEM;
    } catch (...) {
        return -EINVAL;
    }
}

} // extern "C"
