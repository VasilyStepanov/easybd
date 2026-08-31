# easybd

`easybd` is a deliberately minimal block-access-over-TCP protocol, server
and client, built as a benchmarking harness for comparing I/O backends
(plain libc syscalls vs. io_uring, plain io_uring recv vs. io_uring
multishot recv) under a network round trip, not as a production remote
block device.

The wire protocol (`include/easybd/protocol.h`) is intentionally bare:
no magic number, no version field, native byte order, no auth/TLS. It's
meant to run on a trusted LAN (or a single host, or two containers on the
same docker network) between a purpose-built client and server that are
always upgraded together — the usual reasons to pay for a more robust wire
format don't apply to a benchmarking tool.

## Layout

- `easyio/` — the async I/O core (`easyio::Queue`): one implementation
  backed by plain libc syscalls, one by io_uring (with an optional
  multishot-recv mode). Both backends expose the same interface, so the
  server and client are written once and pick a backend at runtime.
- `server/` — `easybd-server`: listens on a TCP port, serves reads/writes
  against a backing file or block device (opened `O_DIRECT` by default).
- `client/` — `libeasybd`: a C API (`include/easybd/client.h`) wrapping one
  TCP connection + one `easyio::Queue`, used by the
  [easybd_fio](https://github.com/VasilyStepanov/easybd_fio) fork (fio
  with an `easybd` ioengine) to actually drive load.
- `bench/` — scripts that build the whole stack and run/report a standard
  fio job matrix through it (see [Benchmarking](#benchmarking) below).
- `docker/`, `docker-compose.yml` — a two-container topology (a real
  client/server split instead of both on localhost) for the same
  benchmarking scripts.

## Building

Requires a C++20 compiler, autotools, pkg-config, and
[`liburing`](https://github.com/axboe/liburing) >= 2.3 (unless built
`--without-liburing`, which restricts you to the libc backend only).

```
./autogen.sh
./configure --prefix=/path/to/install   # add --without-liburing or --disable-tests if needed
make
make install
```

This builds `server/easybd-server` and `libeasybd` (+ its pkg-config file,
`easybd.pc`) but not a client to drive it — see below.

## Running the server by hand

```
server/easybd-server --bind 0.0.0.0:39900 --file /path/to/backing-file-or-device \
  --queue-type io_uring --feature-multishot
```

Run `server/easybd-server --help` for the full flag list (`--queue-type
libc|io_uring`, `--threads`, `--queue-depth`, `--direct 0|1`, ...).

## Benchmarking

Benchmarking needs a fio build with the easybd ioengine compiled in — a
separate repo, [easybd_fio](https://github.com/VasilyStepanov/easybd_fio)
— on top of an easybd install. `bench/build.sh` does both:

```
eval "$(bench/build.sh)"
# now sets EASYBD_SERVER_BIN, EASYBD_LIB_DIR, EASYBD_FIO_BIN
```

`bench/run.sh` then runs the standard 10-profile job matrix (random 4k
read/write at qd1 and qd16×1/qd16×8, sequential 4M read/write; see
`bench/lib.sh`) and saves one JSON result per profile:

```
bench/run.sh --mode easybd --queue-type io_uring --multishot --sync 0 \
  --file /path/to/backing-file-or-device --out results/io_uring_ms_sync0 \
  --server-bin "$EASYBD_SERVER_BIN" --lib-dir "$EASYBD_LIB_DIR" --fio-bin "$EASYBD_FIO_BIN"
```

This mode starts/stops `easybd-server` itself, on loopback, for the given
`--queue-type`/`--multishot` combination. There's also `--mode raw` (fio
straight against `--file`, no easybd server in front of it — a raw-device
baseline) and, for comparing across durability settings, just rerun with
`--sync 1`.

Turn one or more `--out` directories into a single comparison table with
`bench/report.sh`:

```
bench/report.sh --title "libc vs io_uring vs io_uring-ms, sync=0" \
  libc=results/libc_sync0 io_uring=results/io_uring_sync0 io_uring-ms=results/io_uring_ms_sync0
```

Run any of these scripts with `--help` for the full option list (CPU
pinning via `--client-cpu`/`--server-cpu`, `--runtime`/`--ramp`, retry
behavior for the libc backend's known hang under load, etc).

### Two-container (docker-compose) benchmarking

The above all runs client and server on the same host. `docker-compose.yml`
instead splits them into two containers on a compose network — a `server`
service (backing file in a named volume) and a `client` service (fio +
easybd ioengine + the `bench/` scripts), so the client genuinely talks to
the server over TCP/IP rather than loopback:

```
docker compose build
docker compose up -d server
docker compose run --rm client \
  bench/run.sh --mode easybd --no-server --host server \
    --queue-type io_uring --multishot --sync 0 \
    --fio-bin "$EASYBD_FIO_BIN" --lib-dir "$EASYBD_LIB_DIR" \
    --out results/io_uring_ms_sync0
docker compose run --rm client bench/report.sh mine=results/io_uring_ms_sync0
```

`--no-server --host server` tells `bench/run.sh` to skip managing a server
of its own and just run the client-side matrix against the one already
listening at the `server` compose service (see `bench/run.sh --help`).
Results land in `./results` on the host (bind-mounted into the client
container).

To benchmark a different server-side queue-type/backend combination,
recreate `server` with different `EASYBD_*` env vars (see
`docker/server-entrypoint.sh` for the full list), e.g.:

```
EASYBD_QUEUE_TYPE=libc docker compose up -d --force-recreate server
```

Both services need `security_opt: seccomp:unconfined` (already set in
`docker-compose.yml`) — without it, io_uring's setup syscall is blocked by
Docker's default seccomp profile and both the server and the client (which
also uses io_uring for its own connection, independent of the server's
`--queue-type`) fail to connect at all.

## Known caveats

- The libc backend has a pre-existing, not-fully-root-caused hang under
  load (see `bench/run.sh`'s doc comment and its `--max-retries` workaround).
- The wire protocol has no IPv6 support (client and server are both
  IPv4-only).
