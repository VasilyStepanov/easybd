#!/bin/bash
# Entrypoint for docker/server.Dockerfile. Creates the backing file (a
# plain file inside the EASYBD_FILE volume -- not a real block device, but
# good enough for comparing queue-type/backend combinations, which is what
# bench/lib.sh's job matrix measures) if it doesn't exist yet, then execs
# easybd-server. Extra arguments (docker-compose "command:", or
# `docker run ... <image> --extra-flag`) are appended after the flags built
# from the EASYBD_* env vars below, so they win on any conflicting flag.
set -euo pipefail

: "${EASYBD_BIND:=0.0.0.0:39900}"
: "${EASYBD_FILE:=/data/easybd.img}"
: "${EASYBD_FILE_SIZE:=4G}"
: "${EASYBD_QUEUE_TYPE:=io_uring}"
: "${EASYBD_MULTISHOT:=0}"
: "${EASYBD_THREADS:=}"
: "${EASYBD_QUEUE_DEPTH:=}"
: "${EASYBD_DIRECT:=}"

if [[ ! -e "$EASYBD_FILE" ]]; then
  echo "server-entrypoint: creating backing file $EASYBD_FILE ($EASYBD_FILE_SIZE)" >&2
  mkdir -p "$(dirname "$EASYBD_FILE")"
  fallocate -l "$EASYBD_FILE_SIZE" "$EASYBD_FILE" 2>/dev/null \
    || truncate -s "$EASYBD_FILE_SIZE" "$EASYBD_FILE"
fi

args=(--bind "$EASYBD_BIND" --file "$EASYBD_FILE" --queue-type "$EASYBD_QUEUE_TYPE")
[[ -n "$EASYBD_THREADS" ]] && args+=(--threads "$EASYBD_THREADS")
[[ "$EASYBD_MULTISHOT" == "1" ]] && args+=(--feature-multishot)
[[ -n "$EASYBD_QUEUE_DEPTH" ]] && args+=(--queue-depth "$EASYBD_QUEUE_DEPTH")
[[ -n "$EASYBD_DIRECT" ]] && args+=(--direct "$EASYBD_DIRECT")

exec easybd-server "${args[@]}" "$@"
