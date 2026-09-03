#!/bin/bash
# Runs the standard 10-profile fio job matrix (see bench/lib.sh) against one
# target and saves each job's fio --output-format=json result into --out.
# Use bench/report.sh afterwards to turn one or more --out directories into
# a comparison table.
#
# Nothing here gets built for you -- on a fresh checkout, run bench/build.sh
# first (or pass --server-bin/--lib-dir/--fio-bin yourself if you already
# have everything built some other way).
#
# Two modes:
#
#   --mode easybd   Starts an easybd-server for the given --queue-type/
#                    --multishot combination against --file, runs the
#                    matrix through it over TCP, then stops it. This is
#                    what you want for "easybd libc/io_uring/io_uring-ms".
#
#   --mode raw       Runs fio directly against --file with no easybd
#                     server in front of it (plain io_uring ioengine).
#                     This is what you want for a raw-device baseline,
#                     e.g. an nvme/tcp-attached device.
#
# Usage:
#   run.sh --mode easybd --queue-type libc|io_uring [--multishot] \
#          --sync 0|1 --file PATH --out DIR [options...]
#   run.sh --mode raw --sync 0|1 --file PATH --out DIR [options...]
#
# Options (all modes):
#   --sync 0|1          required. fio --sync value (durable write or not).
#   --file PATH          required. Backing device/file for the target.
#   --out DIR             required. Where to write <job>.json/.err per profile.
#   --sudo                 Run the server/fio invocation under sudo (needed
#                            for a real block device you don't own, e.g.
#                            /dev/nvme0n1p17).
#   --runtime SECONDS     (default 60) fio --runtime.
#   --ramp SECONDS         (default 20) fio --ramp_time.
#   --client-cpu LIST      (default: unset, no taskset) e.g. "4-7".
#   --fio-bin PATH          (default: "fio" from PATH for --mode raw; for
#                             --mode easybd, defaults to $EASYBD_FIO_BIN if
#                             set -- e.g. already exported by `eval
#                             "$(bench/build.sh)"`, or baked into
#                             docker/client.Dockerfile's image -- and is
#                             REQUIRED otherwise, since there is no
#                             universal default: it's a separate fio build
#                             with the easybd ioengine compiled in, not
#                             part of this repo)
#
# Options (--mode easybd only):
#   --queue-type libc|io_uring   required.
#   --multishot                    io_uring-multishot instead of plain
#                                   io_uring (only meaningful with
#                                   --queue-type io_uring).
#   --host HOST                    (default 127.0.0.1) address the fio client
#                                    connects to. Only useful with
#                                    --no-server (see below) -- when this
#                                    script also owns the server it always
#                                    runs on the loopback address.
#   --port N                       (default 39900) TCP port for this server.
#   --no-server                     Don't start/stop an easybd-server at all
#                                    -- just run the client-side matrix
#                                    against an already-running one at
#                                    --host:--port (e.g. a separate machine
#                                    or docker-compose service). --file,
#                                    --server-cpu, --server-bin, --threads
#                                    and --max-retries are all ignored, and
#                                    --sudo no longer applies to a server
#                                    this script never touches.
#   --server-cpu LIST              (default: unset, no taskset) e.g. "0-3".
#   --server-bin PATH              (default: <repo>/server/easybd-server)
#   --lib-dir PATH                 (default: $EASYBD_LIB_DIR if set, else
#                                   <repo>/client/.libs) LD_LIBRARY_PATH
#                                   for the fio client's easybd ioengine
#                                   (libeasybd.so).
#   --threads N                    (default 4) server --threads.
#   --max-retries N                 (default 3) on a job timing out, restart
#                                    the server and retry this many times
#                                    before giving up and leaving that job's
#                                    result missing (report.sh renders it as
#                                    N/A). See "known caveat" below.
#
# Known caveat: the libc backend has a pre-existing, not-fully-root-caused
# hang under load (an accept()/RecvStream interaction -- see git log for
# "accept_loop retries any accept() failure"; that fix closes one failure
# mode but does not fully eliminate the hang). --max-retries works around
# it by restarting the server and retrying; a job that still times out on
# every retry is left without a result, which report.sh shows as N/A. This
# retry loop is unavailable with --no-server (there is no server here to
# restart), so a hung job just times out once and is left as N/A.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
# shellcheck source=bench/lib.sh
source "$SCRIPT_DIR/lib.sh"

mode=""
queue_type=""
multishot=0
sync=""
file=""
out=""
use_sudo=0
runtime=60
ramp=20
client_cpu=""
server_cpu=""
fio_bin="${EASYBD_FIO_BIN:-}"
server_bin="$REPO_ROOT/server/easybd-server"
lib_dir="${EASYBD_LIB_DIR:-$REPO_ROOT/client/.libs}"
host="127.0.0.1"
port=39900
threads=4
max_retries=3
manage_server=1

usage() {
  sed -n '2,88p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) mode=$2; shift 2 ;;
    --queue-type) queue_type=$2; shift 2 ;;
    --multishot) multishot=1; shift ;;
    --sync) sync=$2; shift 2 ;;
    --file) file=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    --sudo) use_sudo=1; shift ;;
    --runtime) runtime=$2; shift 2 ;;
    --ramp) ramp=$2; shift 2 ;;
    --client-cpu) client_cpu=$2; shift 2 ;;
    --server-cpu) server_cpu=$2; shift 2 ;;
    --fio-bin) fio_bin=$2; shift 2 ;;
    --server-bin) server_bin=$2; shift 2 ;;
    --lib-dir) lib_dir=$2; shift 2 ;;
    --host) host=$2; shift 2 ;;
    --port) port=$2; shift 2 ;;
    --threads) threads=$2; shift 2 ;;
    --max-retries) max_retries=$2; shift 2 ;;
    --no-server) manage_server=0; shift ;;
    -h|--help) usage ;;
    *) echo "unknown option: $1" >&2; usage ;;
  esac
done

[[ -z "$mode" ]] && { echo "--mode is required" >&2; usage; }
[[ -z "$sync" ]] && { echo "--sync is required" >&2; usage; }
[[ -z "$out" ]] && { echo "--out is required" >&2; usage; }
if [[ "$mode" == "easybd" ]]; then
  [[ -z "$queue_type" ]] && { echo "--queue-type is required for --mode easybd" >&2; usage; }
  [[ -z "$fio_bin" ]] && {
    echo "--fio-bin is required for --mode easybd (the fio build with the" >&2
    echo "easybd ioengine compiled in -- not part of this repo)" >&2
    exit 1
  }
  [[ "$manage_server" == "1" && -z "$file" ]] && { echo "--file is required for --mode easybd unless --no-server" >&2; usage; }
elif [[ "$mode" == "raw" ]]; then
  fio_bin=${fio_bin:-fio}
  [[ -z "$file" ]] && { echo "--file is required for --mode raw" >&2; usage; }
  [[ "$manage_server" == "0" ]] && { echo "--no-server has no effect with --mode raw (there is no server here)" >&2; usage; }
else
  echo "--mode must be 'easybd' or 'raw'" >&2
  usage
fi

mkdir -p "$out"
timeout_s=$((ramp + runtime + 30))

maybe_sudo() {
  if [[ "$use_sudo" == "1" ]]; then
    sudo "$@"
  else
    "$@"
  fi
}

build_cmd() {
  # build_cmd <arrayname> <cpulist> <cmd...> -- fills arrayname with
  # `taskset -c cpulist cmd...` if cpulist is non-empty, else just
  # `cmd...`. A plain array, not a function, because this command line
  # ends up as an argument vector to env/timeout/sudo -- those exec(3)
  # their argv directly, so a shell *function* name placed there (as
  # opposed to a real binary like taskset) can never be resolved and
  # fails with "No such file or directory".
  local -n _build_cmd_out=$1
  local cpulist=$2; shift 2
  if [[ -n "$cpulist" ]]; then
    _build_cmd_out=(taskset -c "$cpulist" "$@")
  else
    _build_cmd_out=("$@")
  fi
}

start_server() {
  local logf="$out/server.log"
  local extra=()
  [[ "$queue_type" == "io_uring" && "$multishot" == "1" ]] && extra+=(--feature-multishot)
  local cmd
  build_cmd cmd "$server_cpu" \
    "$server_bin" --bind "127.0.0.1:${port}" --file "$file" --queue-type "$queue_type" \
    --threads "$threads" "${extra[@]}"
  maybe_sudo env "LD_LIBRARY_PATH=$lib_dir" "${cmd[@]}" >"$logf" 2>&1 &
  disown
  for _ in $(seq 1 20); do
    grep -q "listening on" "$logf" 2>/dev/null && return 0
    sleep 0.5
  done
  log "server failed to start on port $port -- see $logf"
  return 1
}

stop_server() {
  maybe_sudo pkill -9 -f "easybd-server --bind 127.0.0.1:${port} " 2>/dev/null
  return 0
}

run_one_easybd() {
  local out_json=$1 rw=$2 bs=$3 iodepth=$4 numjobs=$5
  local cmd
  build_cmd cmd "$client_cpu" "$fio_bin" \
    --name=job --ioengine=easybd --filename="${host}\\:${port}" \
    --size=1800M --rw="$rw" --bs="$bs" --direct=1 --iodepth="$iodepth" --numjobs="$numjobs" \
    --thread --easybd_queue_type="$queue_type" --easybd_feature_multishot="$multishot" \
    --sync="$sync" --group_reporting --time_based=1 --runtime="$runtime" --ramp_time="$ramp" \
    --output-format=json --output="$out_json"
  LD_LIBRARY_PATH="$lib_dir" timeout -k 10 "$timeout_s" "${cmd[@]}" >"${out_json%.json}.err" 2>&1
}

run_one_raw() {
  local out_json=$1 rw=$2 bs=$3 iodepth=$4 numjobs=$5
  local cmd
  build_cmd cmd "$client_cpu" "$fio_bin" \
    --name=job --ioengine=io_uring --filename="$file" \
    --size=1800M --rw="$rw" --bs="$bs" --direct=1 --sync="$sync" --iodepth="$iodepth" \
    --numjobs="$numjobs" --group_reporting --time_based=1 --runtime="$runtime" \
    --ramp_time="$ramp" --output-format=json --output="$out_json"
  maybe_sudo timeout -k 10 "$timeout_s" "${cmd[@]}" >"${out_json%.json}.err" 2>&1
}

n=0
total=${#JOBS[@]}
failed=()

if [[ "$mode" == "easybd" && "$manage_server" == "1" ]]; then
  start_server || exit 1
fi

for job in "${JOBS[@]}"; do
  read -r jname rw bs iodepth numjobs <<<"$job"
  n=$((n + 1))
  out_json="$out/${jname}.json"

  if json_valid "$out_json"; then
    log "[$n/$total] $jname (skip, already have valid results)"
    continue
  fi

  log "[$n/$total] $jname"
  if [[ "$mode" == "raw" ]]; then
    run_one_raw "$out_json" "$rw" "$bs" "$iodepth" "$numjobs"
    rc=$?
  else
    run_one_easybd "$out_json" "$rw" "$bs" "$iodepth" "$numjobs"
    rc=$?
    tries=0
    while [[ $rc -ne 0 && "$manage_server" == "1" && "$queue_type" == "libc" && $tries -lt $max_retries ]]; do
      tries=$((tries + 1))
      log "  job failed (rc=$rc), restarting server and retrying ($tries/$max_retries)"
      stop_server
      sleep 2
      start_server
      sleep 1
      run_one_easybd "$out_json" "$rw" "$bs" "$iodepth" "$numjobs"
      rc=$?
    done
  fi

  if [[ $rc -ne 0 ]]; then
    log "  $jname did not complete (rc=$rc) -- report.sh will show N/A"
    failed+=("$jname")
  fi
done

if [[ "$mode" == "easybd" && "$manage_server" == "1" ]]; then
  stop_server
fi

if [[ ${#failed[@]} -gt 0 ]]; then
  log "done, but ${#failed[@]}/${total} job(s) never completed: ${failed[*]}"
  exit 1
fi
log "done, all $total jobs completed"
