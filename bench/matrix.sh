#!/bin/bash
# Runs the full backend x durability sweep -- libc / io_uring /
# io_uring-multishot each at --sync 0 and --sync 1 -- and renders the two
# comparison tables (one per sync value) with bench/report.sh. This is the
# "run everything, give me the tables" entry point; bench/run.sh/
# report.sh directly are still what you want for a single ad hoc run (see
# their own --help).
#
# Meant to run *inside* the docker-compose client container, against the
# three always-up server-libc/server-io-uring/server-io-uring-ms services
# (see docker-compose.yml) -- no Docker access needed, this script never
# shells out to `docker` itself:
#
#   docker compose build      # once, from the host -- see docker-compose.yml
#   docker compose run --rm client bench/matrix.sh
#
# (`client`'s `depends_on` starts the three server-* services
# automatically if they aren't already up; this script does not stop them
# afterwards -- `docker compose down` once you're done.)
#
# Usage:
#   bench/matrix.sh [--out DIR] [--runtime SECONDS] [--ramp SECONDS]
#                    [-- EXTRA_RUN_SH_ARGS...]
#
# Options:
#   --out DIR         (default: results/matrix-<timestamp>) where each
#                       backend x sync combo's <job>.json/.err files and
#                       the two rendered sync0.md/sync1.md tables go.
#   --runtime SECONDS   (default 60) forwarded to bench/run.sh.
#   --ramp SECONDS       (default 20) forwarded to bench/run.sh.
#   -- ARGS...            anything after a literal `--` is appended
#                          verbatim to every bench/run.sh invocation (e.g.
#                          --client-cpu). Rarely needed -- --host/
#                          --no-server/--queue-type/--multishot are
#                          hardcoded below (one per backend), not
#                          overridable this way.
#
# Runtime: 3 backends x 2 sync values = 6 bench/run.sh calls, each running
# the standard 12-profile matrix -- with the default --runtime 60 --ramp
# 20 that's roughly 80s/job x 12 jobs x 6 = ~96 minutes total.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

out="results/matrix-$(date +%Y%m%d-%H%M%S)"
runtime=60
ramp=20
extra_args=()

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

usage() {
  sed -n '2,29p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) out=$2; shift 2 ;;
    --runtime) runtime=$2; shift 2 ;;
    --ramp) ramp=$2; shift 2 ;;
    -h|--help) usage ;;
    --) shift; extra_args=("$@"); break ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

# label queue_type multishot host
BACKENDS=(
  "libc        libc      0 server-libc"
  "io_uring    io_uring  0 server-io-uring"
  "io_uring-ms io_uring  1 server-io-uring-ms"
)

mkdir -p "$out"

labels=()
for backend in "${BACKENDS[@]}"; do
  read -r label queue_type multishot host <<<"$backend"
  labels+=("$label")

  multishot_flag=()
  [[ "$multishot" == "1" ]] && multishot_flag=(--multishot)

  for sync in 0 1; do
    combo_out="$out/${label}_sync${sync}"
    log "[$label sync=$sync] running matrix against $host -> $combo_out"
    "$SCRIPT_DIR/run.sh" --mode easybd --no-server --host "$host" \
      --queue-type "$queue_type" "${multishot_flag[@]}" \
      --sync "$sync" --runtime "$runtime" --ramp "$ramp" \
      --out "$combo_out" "${extra_args[@]}"
  done
done

for sync in 0 1; do
  log "rendering sync=$sync table -> $out/sync${sync}.md"
  pairs=()
  for label in "${labels[@]}"; do
    pairs+=("$label=$out/${label}_sync${sync}")
  done
  "$SCRIPT_DIR/report.sh" \
    --title "libc vs io_uring vs io_uring-ms, sync=$sync" \
    --out "$out/sync${sync}.md" \
    "${pairs[@]}"
done

log "done -- tables in $out/sync0.md and $out/sync1.md"
