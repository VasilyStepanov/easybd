#!/bin/bash
# Builds everything bench/run.sh needs on a fresh checkout/fresh machine:
#
#   1. easybd itself (this repo) -- configure/make/install into --prefix,
#      giving us server/easybd-server plus libeasybd + its pkg-config file
#      (the fio ioengine build below needs the latter to find easybd).
#   2. Unless --skip-fio, the easybd_fio fork -- a separate repo (fio with
#      the easybd ioengine, engines/easybd.c, compiled in) -- cloned into
#      --fio-dir if it isn't there already, then built against the
#      install from step 1 via PKG_CONFIG_PATH.
#
# On success, prints the paths bench/run.sh's --server-bin/--lib-dir/
# --fio-bin want, in a form you can eval directly:
#
#   eval "$(bench/build.sh)"
#   bench/run.sh --mode easybd --queue-type libc --sync 1 --file /dev/sdX \
#     --server-bin "$EASYBD_SERVER_BIN" --lib-dir "$EASYBD_LIB_DIR" \
#     --fio-bin "$EASYBD_FIO_BIN" --out results/sync1/libc
#
# Usage:
#   build.sh [--prefix DIR] [--fio-dir DIR] [--fio-repo URL] [--fio-ref REF]
#            [--jobs N] [--skip-fio]
#
# Options:
#   --prefix DIR    (default: <repo>/.bench-install) easybd install prefix.
#   --fio-dir DIR    (default: <repo>/.bench-fio) where easybd_fio is
#                     cloned/built. Left alone (not re-cloned) if it
#                     already exists -- rerun `git pull` in there yourself
#                     first if you want a newer easybd_fio.
#   --fio-repo URL    (default: git@github.com:VasilyStepanov/easybd_fio.git)
#                      Only used the first time (when cloning --fio-dir).
#   --fio-ref REF      git checkout this ref in --fio-dir after cloning
#                       (default: whatever the clone's default branch is).
#   --jobs N            (default: nproc) parallel build jobs.
#   --skip-fio            Only build easybd itself; skip cloning/building
#                          easybd_fio (e.g. if you already have a fio
#                          binary built separately). Only
#                          EASYBD_SERVER_BIN/EASYBD_LIB_DIR are printed.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

prefix="$REPO_ROOT/.bench-install"
fio_dir="$REPO_ROOT/.bench-fio"
fio_repo="git@github.com:VasilyStepanov/easybd_fio.git"
fio_ref=""
jobs=$(nproc 2>/dev/null || echo 4)
skip_fio=0

usage() {
  sed -n '2,39p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) prefix=$2; shift 2 ;;
    --fio-dir) fio_dir=$2; shift 2 ;;
    --fio-repo) fio_repo=$2; shift 2 ;;
    --fio-ref) fio_ref=$2; shift 2 ;;
    --jobs) jobs=$2; shift 2 ;;
    --skip-fio) skip_fio=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

log() { echo "[build] $*" >&2; }

log "building easybd (prefix=$prefix)"
cd "$REPO_ROOT"
[[ -x ./configure ]] || ./autogen.sh
./configure --prefix="$prefix" >&2
make -j"$jobs" >&2
make install >&2

server_bin="$REPO_ROOT/server/easybd-server"
lib_dir="$prefix/lib"

if [[ "$skip_fio" == "1" ]]; then
  echo "EASYBD_SERVER_BIN=$server_bin"
  echo "EASYBD_LIB_DIR=$lib_dir"
  exit 0
fi

if [[ ! -d "$fio_dir" ]]; then
  log "cloning $fio_repo into $fio_dir"
  git clone "$fio_repo" "$fio_dir" >&2
fi

cd "$fio_dir"
[[ -n "$fio_ref" ]] && git checkout "$fio_ref" >&2

log "building easybd_fio (PKG_CONFIG_PATH=$prefix/lib/pkgconfig)"
PKG_CONFIG_PATH="$prefix/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}" ./configure >&2
if ! grep -q '^CONFIG_EASYBD' config-host.mak 2>/dev/null; then
  log "ERROR: fio's configure did not detect the easybd engine."
  log "Check that pkg-config can see $prefix/lib/pkgconfig/easybd.pc, e.g.:"
  log "  PKG_CONFIG_PATH=$prefix/lib/pkgconfig pkg-config --cflags --libs easybd"
  exit 1
fi
make -j"$jobs" >&2

echo "EASYBD_SERVER_BIN=$server_bin"
echo "EASYBD_LIB_DIR=$lib_dir"
echo "EASYBD_FIO_BIN=$fio_dir/fio"
