#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <thread>

#include <getopt.h>

#include <easyio/queue.hpp>

#include "server.hpp"

namespace {

constexpr unsigned int kDefaultQueueDepth = 128;

void print_usage(const char* argv0) {
    std::fprintf(
        stderr,
        "Usage: %s -b HOST:PORT -f FILE [options]\n"
        "\n"
        "Options:\n"
        "  -b, --bind HOST:PORT     address to listen on (required)\n"
        "  -f, --file PATH          backing file/block device to serve (required)\n"
        "  -q, --queue-type TYPE    io_uring or libc%s\n"
        "      --queue-depth N      per-worker queue depth (default: %u)\n"
        "  -j, --threads N          worker thread count (default: number of CPUs)\n"
        "  -h, --help               show this help and exit\n"
        "  -v, --version            show version and exit\n",
        argv0, easyio::io_uring_available() ? " (default: io_uring)" : " (only libc: built --without-liburing)",
        kDefaultQueueDepth);
}

// Splits "host:port"; host may not itself contain ':' (no IPv6 support --
// matches the plain AF_INET-only listen socket this server opens).
void parse_bind_addr(const std::string& s, std::string& host, uint16_t& port) {
    auto pos = s.rfind(':');
    if (pos == std::string::npos || pos == 0 || pos + 1 == s.size()) {
        throw std::invalid_argument("easybd-server: --bind must be HOST:PORT");
    }
    host = s.substr(0, pos);
    std::string port_str = s.substr(pos + 1);
    for (char c : port_str) {
        if (!isdigit(static_cast<unsigned char>(c))) {
            throw std::invalid_argument("easybd-server: --bind port must be numeric");
        }
    }
    long p = std::strtol(port_str.c_str(), nullptr, 10);
    if (p <= 0 || p > 65535) {
        throw std::invalid_argument("easybd-server: --bind port out of range");
    }
    port = static_cast<uint16_t>(p);
}

easyio::Backend parse_queue_type(const std::string& s) {
    if (s == "io_uring") {
        if (!easyio::io_uring_available()) {
            throw std::invalid_argument(
                "easybd-server: queue-type io_uring requested but this build was "
                "configured with --without-liburing");
        }
        return easyio::Backend::IoUring;
    }
    if (s == "libc") {
        return easyio::Backend::Libc;
    }
    throw std::invalid_argument("easybd-server: --queue-type must be io_uring or libc");
}

} // namespace

int main(int argc, char** argv) {
    easybd::ServerConfig config;
    config.queue_depth = kDefaultQueueDepth;
    config.threads = std::max(1U, std::thread::hardware_concurrency());
    config.backend = easyio::io_uring_available() ? easyio::Backend::IoUring : easyio::Backend::Libc;

    bool have_bind = false;
    bool have_file = false;

    static const struct option longopts[] = { // NOLINT(modernize-avoid-c-arrays): getopt_long's C API
        {"bind", required_argument, nullptr, 'b'},
        {"file", required_argument, nullptr, 'f'},
        {"queue-type", required_argument, nullptr, 'q'},
        {"queue-depth", required_argument, nullptr, 'Q'},
        {"threads", required_argument, nullptr, 'j'},
        {"help", no_argument, nullptr, 'h'},
        {"version", no_argument, nullptr, 'v'},
        {nullptr, 0, nullptr, 0},
    };

    try {
        int opt;
        while ((opt = getopt_long(argc, argv, "b:f:q:j:hv", longopts, nullptr)) != -1) {
            switch (opt) {
            case 'b': {
                std::string host;
                uint16_t port = 0;
                parse_bind_addr(optarg, host, port);
                config.bind_host = host;
                config.bind_port = port;
                have_bind = true;
                break;
            }
            case 'f':
                config.file_path = optarg;
                have_file = true;
                break;
            case 'q':
                config.backend = parse_queue_type(optarg);
                break;
            case 'Q':
                config.queue_depth = static_cast<unsigned int>(std::strtoul(optarg, nullptr, 10));
                break;
            case 'j':
                config.threads = static_cast<unsigned int>(std::strtoul(optarg, nullptr, 10));
                break;
            case 'h':
                print_usage(argv[0]);
                return 0;
            case 'v':
                std::printf("easybd-server 0.1.0\n");
                return 0;
            default:
                print_usage(argv[0]);
                return 2;
            }
        }

        if (optind < argc) {
            std::fprintf(stderr, "easybd-server: unexpected positional argument '%s'\n", argv[optind]);
            print_usage(argv[0]);
            return 2;
        }
        if (!have_bind || !have_file) {
            std::fprintf(stderr, "easybd-server: --bind and --file are required\n");
            print_usage(argv[0]);
            return 2;
        }
        if (config.threads == 0) {
            std::fprintf(stderr, "easybd-server: --threads must be >= 1\n");
            return 2;
        }

        std::fprintf(
            stderr, "easybd-server: listening on %s:%u, file=%s, backend=%s, threads=%u, depth=%u\n",
            config.bind_host.c_str(), config.bind_port, config.file_path.c_str(),
            std::string(easyio::backend_name(config.backend)).c_str(), config.threads,
            config.queue_depth);

        easybd::run_server(config);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "easybd-server: %s\n", e.what());
        return 1;
    }

    return 0;
}
