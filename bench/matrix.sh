#!/bin/bash
# Runs the full backend x durability sweep -- libc / io_uring /
# io_uring-multishot each at --sync 0 and --sync 1 -- through the
# docker-compose two-container topology (see docker-compose.yml), and
# renders the two comparison tables (one per sync value) with
# bench/report.sh. This is the "run everything, give me the tables" entry
# point; bench/run.sh/report.sh directly are still what you want for a
# single ad hoc run (see their own --help).
#
# Requires docker compose to already be able to build/run this repo's
# images (docker-compose.yml at the repo root) -- run this from the repo
# root, or anywhere; it cd's there itself.
#
# Usage:
#   bench/matrix.sh [--out DIR] [--skip-build] [--runtime SECONDS]
#                    [--ramp SECONDS] [-- EXTRA_RUN_SH_ARGS...]
#
# Options:
#   --out DIR         (default: results/matrix-<timestamp>) where each
#                       backend x sync combo's <job>.json/.err files and
#                       the two rendered sync0.md/sync1.md tables go.
#                       Bind-mounted from the client container, so this is
#                       a path on the host, not inside any container.
#   --skip-build       Skip `docker compose build` (already built images).
#   --runtime SECONDS   (default 60) forwarded to bench/run.sh.
#   --ramp SECONDS       (default 20) forwarded to bench/run.sh.
#   -- ARGS...            anything after a literal `--` is appended
#                          verbatim to every bench/run.sh invocation (e.g.
#                          --client-cpu). Rarely needed -- the defaults
#                          already match this topology (--host server
#                          --no-server are hardcoded below, not overridable
#                          this way).
#
# Runtime: 3 backends x 2 sync values = 6 bench/run.sh calls, each running
# the standard 10-profile matrix -- with the default --runtime 60 --ramp
# 20 that's roughly 80s/job x 10 jobs x 6 = ~80 minutes total, plus a
# `docker compose build` up front unless --skip-build.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

out="results/matrix-$(date +%Y%m%d-%H%M%S)"
skip_build=0
runtime=60
ramp=20
extra_args=()

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

usage() {
  sed -n '2,32p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) out=$2; shift 2 ;;
    --skip-build) skip_build=1; shift ;;
    --runtime) runtime=$2; shift 2 ;;
    --ramp) ramp=$2; shift 2 ;;
    -h|--help) usage ;;
    --) shift; extra_args=("$@"); break ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

# label queue_type multishot
BACKENDS=(
  "libc        libc      0"
  "io_uring    io_uring  0"
  "io_uring-ms io_uring  1"
)

if [[ "$skip_build" == "0" ]]; then
  log "building images"
  docker compose build
fi

mkdir -p "$out"

labels=()
for backend in "${BACKENDS[@]}"; do
  read -r label queue_type multishot <<<"$backend"
  labels+=("$label")

  log "starting server: queue-type=$queue_type multishot=$multishot"
  EASYBD_QUEUE_TYPE="$queue_type" EASYBD_MULTISHOT="$multishot" \
    docker compose up -d --force-recreate server
  # `up -d` returns once the container starts, not once the server inside
  # it has finished its own startup -- give it a moment.
  sleep 2

  multishot_flag=()
  [[ "$multishot" == "1" ]] && multishot_flag=(--multishot)

  for sync in 0 1; do
    combo_out="$out/${label}_sync${sync}"
    log "[$label sync=$sync] running matrix -> $combo_out"
    docker compose run --rm client \
      bench/run.sh --mode easybd --no-server --host server \
        --queue-type "$queue_type" "${multishot_flag[@]}" \
        --sync "$sync" --runtime "$runtime" --ramp "$ramp" \
        --out "$combo_out" "${extra_args[@]}"
  done
done

log "stopping server"
docker compose stop server

for sync in 0 1; do
  log "rendering sync=$sync table -> $out/sync${sync}.md"
  pairs=()
  for label in "${labels[@]}"; do
    pairs+=("$label=$out/${label}_sync${sync}")
  done
  docker compose run --rm client bench/report.sh \
    --title "libc vs io_uring vs io_uring-ms, sync=$sync" \
    --out "$out/sync${sync}.md" \
    "${pairs[@]}"
done

log "done -- tables in $out/sync0.md and $out/sync1.md"
