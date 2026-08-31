# Builds a fio with the easybd ioengine compiled in (the easybd_fio fork,
# see bench/build.sh) plus libeasybd.so, and packages them alongside
# bench/*.sh -- the "client" side of the two-container docker-compose
# benchmarking topology (see docker-compose.yml at the repo root; pairs
# with docker/server.Dockerfile). Build context is the repo root:
# `docker build -f docker/client.Dockerfile .`

FROM debian:bookworm AS builder

ARG FIO_REPO=https://github.com/VasilyStepanov/easybd_fio.git
ARG FIO_REF=

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential autoconf automake libtool pkg-config git \
      liburing-dev zlib1g-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src/easybd
COPY . .
RUN ./autogen.sh \
    && ./configure --prefix=/opt/easybd --disable-tests \
    && make -j"$(nproc)" \
    && make install

WORKDIR /src/fio
RUN git clone "$FIO_REPO" . \
    && if [ -n "$FIO_REF" ]; then git checkout "$FIO_REF"; fi \
    && PKG_CONFIG_PATH=/opt/easybd/lib/pkgconfig ./configure \
    && grep -q '^CONFIG_EASYBD' config-host.mak \
    && make -j"$(nproc)"

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      liburing2 bash jq \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/fio/fio /usr/local/bin/fio-easybd
# COPY --from dereferences symlinks, so grab only the real (fully-versioned)
# shared object and recreate the SONAME/dev symlinks ourselves -- otherwise
# libeasybd.so/.so.0 end up as full extra copies instead of links.
COPY --from=builder /opt/easybd/lib/libeasybd.so.0.0.0 /usr/local/lib/
RUN ln -s libeasybd.so.0.0.0 /usr/local/lib/libeasybd.so.0 \
    && ln -s libeasybd.so.0.0.0 /usr/local/lib/libeasybd.so \
    && ldconfig

WORKDIR /workspace
COPY bench/ bench/

ENV EASYBD_FIO_BIN=/usr/local/bin/fio-easybd
ENV EASYBD_LIB_DIR=/usr/local/lib
ENV LD_LIBRARY_PATH=/usr/local/lib

ENTRYPOINT ["bash"]
