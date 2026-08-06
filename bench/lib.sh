# Shared helpers for bench/run.sh and bench/report.sh. Not standalone --
# sourced by both, so the job matrix and the JSON-extraction logic used to
# fill a table cell never drift out of sync with each other.

log() { echo "[$(date +%H:%M:%S 2>/dev/null || echo '?')] $*" >&2; }

# The standard profile matrix used for easybd benchmarking: name, rw, bs,
# iodepth, numjobs. Covers random 4k at qd1/qd16 (single connection and 8
# pipelined connections) and sequential 4M reads/writes, read and write.
JOBS=(
  "randread_qd1_nj1   randread 4k 1  1"
  "randread_qd16_nj1  randread 4k 16 1"
  "randread_qd16_nj8  randread 4k 16 8"
  "randwrite_qd1_nj1  randwrite 4k 1  1"
  "randwrite_qd16_nj1 randwrite 4k 16 1"
  "randwrite_qd16_nj8 randwrite 4k 16 8"
  "seq4m_read_nj1  read 4M 4 1"
  "seq4m_read_nj8  read 4M 4 8"
  "seq4m_write_nj1 write 4M 4 1"
  "seq4m_write_nj8 write 4M 4 8"
)

# job_direction <rw> -> read|write -- which side of a job's fio JSON
# ("jobs"[0].read / .write) actually has the nonzero numbers.
job_direction() {
  case "$1" in
    *write*) echo write ;;
    *) echo read ;;
  esac
}

json_valid() {
  [[ -s "$1" ]] && jq -e '.jobs[0]' "$1" >/dev/null 2>&1
}

# cell <json_file> <direction: read|write> -> "IOPS / MiB/s / stddev%",
# or "N/A" if the file is missing/incomplete (e.g. the job never finished).
cell() {
  local f=$1 dir=$2
  if ! json_valid "$f"; then
    echo "N/A"
    return
  fi
  jq -r --arg d "$dir" '
    .jobs[0][$d] as $j
    | ($j.iops) as $iops
    | ($j.bw/1024) as $bw
    | (if $j.iops_mean and $j.iops_mean != 0 then ($j.iops_stddev/$j.iops_mean*100) else 0 end) as $sd
    | "\($iops|round) / \($bw*10|round/10) / \($sd*10|round/10)%"
  ' "$f" 2>/dev/null || echo "N/A"
}
