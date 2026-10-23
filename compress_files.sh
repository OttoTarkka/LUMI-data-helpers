#!/bin/bash
#SBATCH --job-name=compress
#SBATCH --account=project_462001516
#SBATCH --partition=small
#SBATCH --time=03:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=32G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Compress every file in a directory with zstd, skipping anything that already
# ends in .zst. Removes originals only after a successful, verified compression.
#
#   sbatch compress_files.sh [options] /path/to/dir
#
# Options
#   -l, --level N      zstd level (1-19, default 19)
#   -s, --suffix SUF   only compress files whose names end in SUF (e.g. .jsonl)
#   -j, --jobs N       number of files compressed concurrently, raise for many small files (default 1)
#   -i, --interval S   seconds between progress reports per file (default 60)
#   -d, --maxdepth N   only descend N levels below DIR, as with `find -maxdepth`;
#                      1 means files directly in DIR (default: no limit)
#
# Notes
#   • Subdirectories are searched recursively at any depth unless --maxdepth
#     is given. Each file is compressed in place, next to the original.
#     Symbolic links are not followed.
#   • Each file is compressed **independently** so failures affect only
#     that file. The script exits non-zero if any file failed.
#   • Both intra-file threading (-T) and inter-file parallelism (xargs -P)
#     are used. The CPUs Slurm allocates are divided evenly between the
#     concurrent jobs. One job using all CPUs suits a few large files; raise
#     --jobs for many small files, which cannot use many threads each.
#   • Speed is dominated by the level. Measured on multilingual JSONL text,
#     single thread:
#     level 19 ≈ 2 MB/s
#     level 15 ≈ 6 MB/s
#     level 12 ≈ 35 MB/s, Output is ~15% larger at 12 than at 19.
#     level 9 ≈ 70 MB/s
#     level 3 ≈ 330 MB/s.
#     zstd needs roughly 70 MB of memory per thread at level 19.
#   • Each file is written to FILE.zst.partial, tested with `zstd -t`, then
#     renamed to FILE.zst before the original is removed. A job killed by the
#     time limit therefore never leaves a truncated .zst, and resubmitting the
#     same command continues where it stopped.
#   • A file is skipped as failed if FILE.zst already exists next to FILE,
#     since it is unclear which one is complete. Check and remove one by hand.
###############################################################################

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 [-l LEVEL] [-s SUFFIX] [-j JOBS] [-i SECONDS] [-d MAXDEPTH] /path/to/dir" >&2
  exit 1
}

mkdir -p ./logs

# ---- Parse and validate user input -----------------------------------------
LEVEL=19        # Default to a high compression level
SUFFIX=""
JOBS=1          # Number of files compressed concurrently
INTERVAL=60     # Seconds between progress reports
MAXDEPTH=""     # Empty means search subdirectories at any depth
DIR=""

while (( $# > 0 )); do
  case "$1" in
    -l|--level)    LEVEL=${2:-}; shift 2 || usage ;;
    -s|--suffix)   SUFFIX=${2:-}; shift 2 || usage ;;
    -j|--jobs)     JOBS=${2:-}; shift 2 || usage ;;
    -i|--interval) INTERVAL=${2:-}; shift 2 || usage ;;
    -d|--maxdepth) MAXDEPTH=${2:-}; shift 2 || usage ;;
    -h|--help)     usage ;;
    -*)            echo "Error: unknown option '$1'" >&2; usage ;;
    *)
      if [[ -n "$DIR" ]]; then
        echo "Error: only one directory may be given" >&2
        usage
      fi
      DIR=$1; shift ;;
  esac
done

[[ -n "$DIR" ]] || usage
if [[ ! -d "$DIR" ]]; then
  echo "Error: '$DIR' is not a directory" >&2
  exit 1
fi
if ! [[ "$LEVEL" =~ ^[0-9]+$ ]] || (( LEVEL < 1 || LEVEL > 19 )); then
  echo "Error: level must be an integer from 1 to 19, got '$LEVEL'" >&2
  exit 1
fi
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

THREADS=${SLURM_CPUS_PER_TASK:-1}   # Total cores Slurm gives us

# Divide CPUs between concurrent zstd processes instead of letting every
# process use all allocated CPUs and its own full memory footprint.
if (( JOBS > THREADS )); then
  JOBS=$THREADS
fi
THREADS_PER_JOB=$(( THREADS / JOBS ))

# ---- Logging helpers ---------------------------------------------------------
# Each message is written with a single printf so lines from concurrent jobs
# do not interleave.
log()       { printf '%s %s\n' "$(date '+%F %T')" "$*"; }
log_error() { printf '%s ERROR %s\n' "$(date '+%F %T')" "$*" >&2; }
human()     { numfmt --to=iec-i --suffix=B --format='%.1f' "$1"; }
duration()  { printf '%dh%02dm%02ds' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# ---- Per-file worker -----------------------------------------------------------
# compress_one INDEX FILE
# Runs in a separate bash started by xargs, so it reports failure through its
# return value and the ledger instead of relying on `set -e`.
compress_one() {
  local idx=$1 src=$2
  local tag="[$idx/$TOTAL_FILES]"
  local dst="$src.zst"
  local tmp="$dst.partial"

  if [[ -e "$dst" ]]; then
    log_error "$tag $src: $dst already exists; not overwritten"
    printf 'FAIL\t0\t0\t%s\n' "$src" >> "$LEDGER"
    return 1
  fi
  if [[ -e "$tmp" ]]; then
    log "$tag $src: overwriting leftover $tmp from an interrupted run"
  fi

  local size start
  size=$(stat -c %s "$src")
  start=$SECONDS
  log "$tag START $src ($(human "$size"))"

  # zstd reads the file on stdin so its read offset can be followed through
  # /proc/PID/fdinfo/0. --stream-size makes zstd fail if the byte count read
  # differs from the size measured above, and stores it in the frame header.
  zstd "-$LEVEL" "-T$THREADS_PER_JOB" --quiet --force --stream-size="$size" \
    -o "$tmp" < "$src" &
  local pid=$! last=$SECONDS pos elapsed
  while kill -0 "$pid" 2>/dev/null; do
    sleep 2
    if (( SECONDS - last >= INTERVAL )); then
      last=$SECONDS
      pos=$(awk '/^pos:/ {print $2}' "/proc/$pid/fdinfo/0" 2>/dev/null) || pos=""
      elapsed=$(( SECONDS - start ))
      if [[ -n "$pos" ]] && (( pos > 0 && elapsed > 0 )); then
        log "$tag $src: $(awk -v p="$pos" -v s="$size" 'BEGIN {printf "%.1f", 100 * p / s}')%" \
            "($(human "$pos") / $(human "$size")," \
            "$(human $(( pos / elapsed )))/s," \
            "ETA $(duration $(( (size - pos) * elapsed / pos ))))"
      fi
    fi
  done

  local status=0
  wait "$pid" || status=$?
  if (( status != 0 )); then
    log_error "$tag $src: zstd exited with status $status; original kept"
    rm -f "$tmp"
    printf 'FAIL\t%s\t0\t%s\n' "$size" "$src" >> "$LEDGER"
    return 1
  fi

  # Decompress to nowhere to check the content checksum and stored size.
  if ! zstd -t --quiet "$tmp"; then
    log_error "$tag $src: $tmp failed verification; original kept"
    rm -f "$tmp"
    printf 'FAIL\t%s\t0\t%s\n' "$size" "$src" >> "$LEDGER"
    return 1
  fi

  # Mimic `zstd --rm`: keep the original's mode and timestamp, then delete it.
  chmod --reference="$src" "$tmp"
  touch --reference="$src" "$tmp"
  mv -n "$tmp" "$dst"
  if [[ -e "$tmp" ]]; then
    log_error "$tag $src: could not rename $tmp to $dst; original kept"
    printf 'FAIL\t%s\t0\t%s\n' "$size" "$src" >> "$LEDGER"
    return 1
  fi
  rm -f "$src"

  local out
  out=$(stat -c %s "$dst")
  elapsed=$(( SECONDS - start ))
  printf 'OK\t%s\t%s\t%s\n' "$size" "$out" "$src" >> "$LEDGER"
  log "$tag DONE $src: $(human "$size") -> $(human "$out")" \
      "(ratio $(awk -v i="$size" -v o="$out" 'BEGIN {printf "%.2f", i / o}')," \
      "$(duration "$elapsed"), $(human $(( size / (elapsed > 0 ? elapsed : 1) )))/s)"

  # Overall progress across all files, from the shared ledger.
  local done_files done_bytes run_time
  read -r done_files done_bytes < <(awk -F '\t' '{n++; b += $2} END {printf "%d %d\n", n, b}' "$LEDGER")
  run_time=$(( $(date +%s) - JOB_START ))
  log "Overall: $done_files/$TOTAL_FILES files, $(human "$done_bytes") / $(human "$TOTAL_BYTES")," \
      "elapsed $(duration "$run_time"), ETA $(duration $(( (TOTAL_BYTES - done_bytes) * run_time / (done_bytes > 0 ? done_bytes : 1) )))"
}

# ---- Collect input files -------------------------------------------------------
# • find:   locate regular files that do **not** already end in .zst, and
#           skip temporary outputs left by an interrupted run
FIND_ARGS=(-type f ! -name '*.zst' ! -name '*.zst.partial')
if [[ -n "$MAXDEPTH" ]]; then
  # find expects -maxdepth before the tests, otherwise it warns.
  FIND_ARGS=(-maxdepth "$MAXDEPTH" "${FIND_ARGS[@]}")
fi
if [[ -n "$SUFFIX" ]]; then
  FIND_ARGS+=(-name "*$SUFFIX")
fi
mapfile -d '' FILES < <(find "$DIR" "${FIND_ARGS[@]}" -print0 | sort -z)

TOTAL_FILES=${#FILES[@]}
TOTAL_BYTES=0
if (( TOTAL_FILES > 0 )); then
  TOTAL_BYTES=$(printf '%s\0' "${FILES[@]}" | xargs -0 stat -c %s | awk '{s += $1} END {printf "%d", s}')
fi

log "Job ${SLURM_JOB_ID:-none} on $(hostname), $(zstd --version)"
log "Compressing ${TOTAL_FILES} file(s), $(human "$TOTAL_BYTES"), in '$DIR'" \
    "${MAXDEPTH:+to depth $MAXDEPTH }${SUFFIX:+with suffix '$SUFFIX' }with zstd -${LEVEL}," \
    "${JOBS} job(s), ${THREADS_PER_JOB} thread(s) per job"

if (( TOTAL_FILES == 0 )); then
  log "Nothing to compress."
  exit 0
fi

# ---- Main compression loop --------------------------------------------------
# • xargs:  run compress_one in parallel, one file per call, up to $JOBS at once
# • zstd:   uses only its allocated share of CPUs
# The ledger records one line per finished file for the overall progress and
# the final summary. It lives on node-local /tmp, away from the data.
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
LEDGER="$WORK_DIR/ledger.tsv"
: > "$LEDGER"
JOB_START=$(date +%s)

export LEVEL THREADS_PER_JOB INTERVAL TOTAL_FILES TOTAL_BYTES LEDGER JOB_START
export -f compress_one log log_error human duration

for i in "${!FILES[@]}"; do
  printf '%s\0%s\0' "$(( i + 1 ))" "${FILES[$i]}"
done | xargs -0 -n2 -P "$JOBS" bash -c 'compress_one "$1" "$2"' _ || true

# ---- Summary ---------------------------------------------------------------
read -r ok_files in_bytes out_bytes < <(awk -F '\t' '$1 == "OK" {n++; i += $2; o += $3} END {printf "%d %d %d\n", n, i, o}' "$LEDGER")
mapfile -t FAILED < <(awk -F '\t' '$1 == "FAIL" {print $4}' "$LEDGER")
RUN_TIME=$(( $(date +%s) - JOB_START ))

log "Compressed ${ok_files}/${TOTAL_FILES} file(s) in $(duration "$RUN_TIME"):" \
    "$(human "$in_bytes") -> $(human "$out_bytes")"

if (( ${#FAILED[@]} > 0 || ok_files != TOTAL_FILES )); then
  log_error "${#FAILED[@]} file(s) failed, $(( TOTAL_FILES - ok_files - ${#FAILED[@]} )) not processed:"
  for f in "${FAILED[@]}"; do
    log_error "  $f"
  done
  exit 1
fi

log "Done."
