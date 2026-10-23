#!/bin/bash
#SBATCH --job-name=decompress
#SBATCH --account=project_462001516
#SBATCH --partition=debug
#SBATCH --time=00:29:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=32G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Decompress every .zst file under one or more directories with zstd. Removes
# the compressed files only after a successful decompression, unless --keep.
#
#   sbatch decompress_files.sh [options] /path/to/dir [/path/to/dir ...]
#
# Options
#   -s, --suffix SUF   only decompress files whose names end in SUF.zst (e.g. .jsonl)
#   -j, --jobs N       number of files decompressed concurrently (default: all allocated CPUs)
#   -i, --interval S   seconds between progress reports per file (default 60)
#   -k, --keep         keep the .zst files after decompression (default: remove)
#   -d, --maxdepth N   only descend N levels below each DIR, as with `find -maxdepth`;
#                      1 means files directly in DIR (default: no limit)
#
# Notes
#   • Subdirectories are searched recursively at any depth unless --maxdepth
#     is given. Each file is decompressed in place, next to the .zst.
#     Symbolic links are not followed.
#   • Each file is decompressed **independently** so failures affect only
#     that file. The script exits non-zero if any file failed.
#   • zstd decompresses a file on a single thread, so parallelism comes only
#     from decompressing several files at once (xargs -P). By default one file
#     runs per allocated CPU. With many concurrent jobs the filesystem, not
#     the CPU, is usually the limit; lower --jobs if throughput stops growing.
#   • Each file is written to FILE.partial and renamed to FILE before FILE.zst
#     is removed. zstd verifies the content checksum and the stored size
#     written by compress_files.sh while decoding, so a corrupt or truncated
#     .zst fails instead of producing a short FILE. A job killed by the time
#     limit therefore never leaves a truncated FILE, and resubmitting the same
#     command continues where it stopped.
#   • A file is skipped as failed if FILE already exists next to FILE.zst,
#     since it is unclear which one is complete. Check and remove one by hand.
#     This also applies when rerunning over files already decompressed with
#     --keep.
###############################################################################

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 [-s SUFFIX] [-j JOBS] [-i SECONDS] [-k] [-d MAXDEPTH] /path/to/dir [/path/to/dir ...]" >&2
  exit 1
}

mkdir -p ./logs

# ---- Parse and validate user input -----------------------------------------
SUFFIX=""
JOBS=""         # Number of files decompressed concurrently, default set below
INTERVAL=60     # Seconds between progress reports
KEEP=false      # Keep the .zst files after decompression
MAXDEPTH=""     # Empty means search subdirectories at any depth
DIRS=()

while (( $# > 0 )); do
  case "$1" in
    -s|--suffix)   SUFFIX=${2:-}; shift 2 || usage ;;
    -j|--jobs)     JOBS=${2:-}; shift 2 || usage ;;
    -i|--interval) INTERVAL=${2:-}; shift 2 || usage ;;
    -k|--keep)     KEEP=true; shift ;;
    -d|--maxdepth) MAXDEPTH=${2:-}; shift 2 || usage ;;
    -h|--help)     usage ;;
    -*)            echo "Error: unknown option '$1'" >&2; usage ;;
    *)             DIRS+=("$1"); shift ;;
  esac
done

(( ${#DIRS[@]} > 0 )) || usage
for d in "${DIRS[@]}"; do
  if [[ ! -d "$d" ]]; then
    echo "Error: '$d' is not a directory" >&2
    exit 1
  fi
done

THREADS=${SLURM_CPUS_PER_TASK:-1}   # Total cores Slurm gives us
JOBS=${JOBS:-$THREADS}

if ! [[ "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: jobs must be a positive integer, got '$JOBS'" >&2
  exit 1
fi
if ! [[ "$INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: interval must be a positive integer, got '$INTERVAL'" >&2
  exit 1
fi
if [[ -n "$MAXDEPTH" ]] && ! [[ "$MAXDEPTH" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: maxdepth must be a positive integer, got '$MAXDEPTH'" >&2
  exit 1
fi

# Resolve the directories so that overlapping arguments (dir and dir/sub, or
# dir and dir/) yield identical paths, which are deduplicated below. Otherwise
# two concurrent jobs could write the same output file.
mapfile -d '' DIRS < <(realpath -ez -- "${DIRS[@]}" | sort -zu)

# ---- Logging helpers ---------------------------------------------------------
# Each message is written with a single printf so lines from concurrent jobs
# do not interleave.
log()       { printf '%s %s\n' "$(date '+%F %T')" "$*"; }
log_error() { printf '%s ERROR %s\n' "$(date '+%F %T')" "$*" >&2; }
human()     { numfmt --to=iec-i --suffix=B --format='%.1f' "$1"; }
duration()  { printf '%dh%02dm%02ds' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# ---- Per-file worker -----------------------------------------------------------
# report_progress PID TAG FILE SIZE START
# Logs how far zstd process PID has read into its stdin every $INTERVAL
# seconds. It runs in the background and is killed as soon as zstd exits, so
# small files do not wait for a polling interval, which matters with many of
# them. sleep gets no inherited descriptors, so a sleep left behind by the
# kill holds no log open.
report_progress() {
  local pid=$1 tag=$2 src=$3 size=$4 start=$5 pos elapsed
  while sleep "$INTERVAL" < /dev/null > /dev/null 2>&1; do
    pos=$(awk '/^pos:/ {print $2}' "/proc/$pid/fdinfo/0" 2>/dev/null) || break
    elapsed=$(( SECONDS - start ))
    if [[ -n "$pos" ]] && (( pos > 0 && elapsed > 0 )); then
      log "$tag $src: $(awk -v p="$pos" -v s="$size" 'BEGIN {printf "%.1f", 100 * p / s}')%" \
          "($(human "$pos") / $(human "$size") compressed," \
          "$(human $(( pos / elapsed )))/s," \
          "ETA $(duration $(( (size - pos) * elapsed / pos ))))"
    fi
  done
}

# decompress_one INDEX FILE
# Runs in a separate bash started by xargs, so it reports failure through its
# return value and the ledger instead of relying on `set -e`.
decompress_one() {
  local idx=$1 src=$2
  local tag="[$idx/$TOTAL_FILES]"
  local dst="${src%.zst}"
  local tmp="$dst.partial"

  local size start
  size=$(stat -c %s "$src")

  if [[ -e "$dst" ]]; then
    log_error "$tag $src: $dst already exists; not overwritten"
    printf 'FAIL\t%s\t0\t%s\n' "$size" "$src" >> "$LEDGER"
    return 1
  fi
  if [[ -e "$tmp" ]]; then
    log "$tag $src: overwriting leftover $tmp from an interrupted run"
  fi

  start=$SECONDS
  log "$tag START $src ($(human "$size"))"

  # zstd reads the file on stdin so its read offset can be followed through
  # /proc/PID/fdinfo/0. Progress is therefore measured in compressed bytes.
  zstd -d --quiet --force -o "$tmp" < "$src" &
  local pid=$!
  report_progress "$pid" "$tag" "$src" "$size" "$start" &
  local reporter=$!

  local status=0 elapsed
  wait "$pid" || status=$?
  kill "$reporter" 2>/dev/null || true
  wait "$reporter" 2>/dev/null || true
  if (( status != 0 )); then
    log_error "$tag $src: zstd exited with status $status; compressed file kept"
    rm -f "$tmp"
    printf 'FAIL\t%s\t0\t%s\n' "$size" "$src" >> "$LEDGER"
    return 1
  fi

  # Mimic `zstd -d --rm`: keep the compressed file's mode and timestamp.
  chmod --reference="$src" "$tmp"
  touch --reference="$src" "$tmp"
  mv -n "$tmp" "$dst"
  if [[ -e "$tmp" ]]; then
    log_error "$tag $src: could not rename $tmp to $dst; compressed file kept"
    printf 'FAIL\t%s\t0\t%s\n' "$size" "$src" >> "$LEDGER"
    return 1
  fi
  if [[ "$KEEP" != true ]]; then
    rm -f "$src"
  fi

  local out
  out=$(stat -c %s "$dst")
  elapsed=$(( SECONDS - start ))
  printf 'OK\t%s\t%s\t%s\n' "$size" "$out" "$src" >> "$LEDGER"
  log "$tag DONE $src: $(human "$size") -> $(human "$out")" \
      "($(duration "$elapsed"), $(human $(( out / (elapsed > 0 ? elapsed : 1) )))/s decompressed)"

  # Overall progress across all files, from the shared ledger.
  local done_files done_bytes run_time
  read -r done_files done_bytes < <(awk -F '\t' '{n++; b += $2} END {printf "%d %d\n", n, b}' "$LEDGER")
  run_time=$(( $(date +%s) - JOB_START ))
  log "Overall: $done_files/$TOTAL_FILES files, $(human "$done_bytes") / $(human "$TOTAL_BYTES") compressed," \
      "elapsed $(duration "$run_time"), ETA $(duration $(( (TOTAL_BYTES - done_bytes) * run_time / (done_bytes > 0 ? done_bytes : 1) )))"
}

# ---- Collect input files -------------------------------------------------------
# The work directory holds the file list and the ledger. It lives on
# node-local /tmp, away from the data.
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
LIST="$WORK_DIR/files"
LEDGER="$WORK_DIR/ledger.tsv"

# • find:   locate regular .zst files, optionally only those ending in SUF.zst
# • sort:   order the work and drop duplicates from overlapping directories
# Written to a file rather than read through a pipe so that a failing find
# (e.g. permission denied) stops the script instead of silently dropping files.
FIND_ARGS=(-type f -name "*$SUFFIX.zst")
if [[ -n "$MAXDEPTH" ]]; then
  # find expects -maxdepth before the tests, otherwise it warns.
  FIND_ARGS=(-maxdepth "$MAXDEPTH" "${FIND_ARGS[@]}")
fi
find "${DIRS[@]}" "${FIND_ARGS[@]}" -print0 > "$LIST"
sort -zu -o "$LIST" "$LIST"
mapfile -d '' FILES < "$LIST"

TOTAL_FILES=${#FILES[@]}
TOTAL_BYTES=0
if (( TOTAL_FILES > 0 )); then
  TOTAL_BYTES=$(xargs -0 stat -c %s < "$LIST" | awk '{s += $1} END {printf "%d", s}')
fi

log "Job ${SLURM_JOB_ID:-none} on $(hostname), $(zstd --version)"
log "Decompressing ${TOTAL_FILES} file(s), $(human "$TOTAL_BYTES") compressed, in" \
    "$(printf "'%s' " "${DIRS[@]}")${MAXDEPTH:+to depth $MAXDEPTH }${SUFFIX:+with suffix '$SUFFIX.zst' }with ${JOBS} job(s)," \
    "$([[ "$KEEP" == true ]] && echo "keeping" || echo "removing") .zst files"

if (( TOTAL_FILES == 0 )); then
  log "Nothing to decompress."
  exit 0
fi

# ---- Main decompression loop ------------------------------------------------
# • xargs:  run decompress_one in parallel, one file per call, up to $JOBS at once
# The ledger records one line per finished file for the overall progress and
# the final summary.
: > "$LEDGER"
JOB_START=$(date +%s)

export KEEP INTERVAL TOTAL_FILES TOTAL_BYTES LEDGER JOB_START
export -f decompress_one report_progress log log_error human duration

for i in "${!FILES[@]}"; do
  printf '%s\0%s\0' "$(( i + 1 ))" "${FILES[$i]}"
done | xargs -0 -n2 -P "$JOBS" bash -c 'decompress_one "$1" "$2"' _ || true

# ---- Summary ---------------------------------------------------------------
read -r ok_files in_bytes out_bytes < <(awk -F '\t' '$1 == "OK" {n++; i += $2; o += $3} END {printf "%d %d %d\n", n, i, o}' "$LEDGER")
mapfile -t FAILED < <(awk -F '\t' '$1 == "FAIL" {print $4}' "$LEDGER")
RUN_TIME=$(( $(date +%s) - JOB_START ))

log "Decompressed ${ok_files}/${TOTAL_FILES} file(s) in $(duration "$RUN_TIME"):" \
    "$(human "$in_bytes") -> $(human "$out_bytes")"

if (( ${#FAILED[@]} > 0 || ok_files != TOTAL_FILES )); then
  log_error "${#FAILED[@]} file(s) failed, $(( TOTAL_FILES - ok_files - ${#FAILED[@]} )) not processed:"
  for f in "${FAILED[@]}"; do
    log_error "  $f"
  done
  exit 1
fi

log "Done."
