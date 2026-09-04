# Builds and runs easybd-server -- the "server" side of the two-container
# docker-compose benchmarking topology (see docker-compose.yml at the repo
# root; pairs with docker/client.Dockerfile). Build context is the repo
# root: `docker build -f docker/server.Dockerfile .`

FROM debian:bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential autoconf automake libtool pkg-config \
      liburing-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN ./autogen.sh \
    && ./configure --prefix=/usr/local --disable-tests \
    && make -j"$(nproc)" \
    && make install

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      liburing2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/easybd-server /usr/local/bin/easybd-server
COPY docker/server-entrypoint.sh /usr/local/bin/server-entrypoint.sh

EXPOSE 39900
ENTRYPOINT ["/usr/local/bin/server-entrypoint.sh"]
