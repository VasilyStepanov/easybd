#!/bin/bash
# Renders a markdown comparison table from one or more bench/run.sh --out
# directories -- one row per profile in the standard job matrix (see
# bench/lib.sh), one column per label=dir pair given on the command line.
# A profile missing/incomplete in a given directory (job never finished,
# see bench/run.sh's --max-retries) renders as N/A in that cell.
#
# Usage:
#   report.sh [--title STR] [--out FILE.md] label1=DIR1 [label2=DIR2 [label3=DIR3 ...]]
#
# Examples:
#   # easybd libc vs io_uring vs io_uring-multishot, sync=1
#   report.sh --title "libc vs io_uring vs io_uring-ms, sync=1 (BLK)" \
#     --out sync1_blk.md \
#     libc=results/sync1_blk/libc \
#     io_uring=results/sync1_blk/uring \
#     io_uring-ms=results/sync1_blk/uringms
#
#   # easybd io_uring-multishot vs raw nvme/tcp
#   report.sh --title "io_uring multishot vs raw nvme/tcp, sync=1" \
#     --out multishot_vs_nvmetcp.md \
#     "io_uring multishot"=results/multishot/blk_uringms \
#     "nvme/tcp"=results/multishot/nvmetcp
#
# Output goes to stdout always; --out additionally saves a copy to that file.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/lib.sh
source "$SCRIPT_DIR/lib.sh"

title=""
out_file=""

usage() {
  sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --title) title=$2; shift 2 ;;
    --out) out_file=$2; shift 2 ;;
    -h|--help) usage ;;
    *) break ;;
  esac
done

[[ $# -eq 0 ]] && { echo "at least one label=dir pair is required" >&2; usage; }

names=()
dirs=()
for pair in "$@"; do
  if [[ "$pair" != *=* ]]; then
    echo "expected label=dir, got: $pair" >&2
    usage
  fi
  names+=("${pair%%=*}")
  dirs+=("${pair#*=}")
done

render() {
  [[ -n "$title" ]] && { echo "## $title"; echo; }

  local header="| job |"
  local sep="|---|"
  for n in "${names[@]}"; do
    header+=" $n |"
    sep+="--:|"
  done
  echo "$header"
  echo "$sep"

  for job in "${JOBS[@]}"; do
    read -r jname rw _ _ _ <<<"$job"
    local direction; direction=$(job_direction "$rw")
    local row="| $jname |"
    for d in "${dirs[@]}"; do
      row+=" $(cell "$d/${jname}.json" "$direction") |"
    done
    echo "$row"
  done
  echo
}

if [[ -n "$out_file" ]]; then
  render | tee "$out_file"
else
  render
fi
