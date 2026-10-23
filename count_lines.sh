#!/bin/bash
#SBATCH --job-name=count
#SBATCH --account=project_462001516
#SBATCH --partition=small
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=8G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Count the lines and bytes of every file under one or more directories, or of
# single files. .zst files are decompressed on the fly, so their counts
# describe the uncompressed content. Nothing is written next to the data.
#
#   sbatch count_lines.sh [options] PATH [PATH ...]
#
# Options
#   -s, --suffix SUF   only count files whose names end in SUF or SUF.zst (e.g. .jsonl)
#   -j, --jobs N       number of files counted concurrently (default: all allocated CPUs)
#   -d, --maxdepth N   only descend N levels below each directory, as with
#                      `find -maxdepth`; 1 means files directly in it (default: no limit)
#   -o, --output FILE  also write the per-file counts to FILE as TSV
#
# Notes
#   • PATH may be a directory or a single file. Subdirectories are searched
#     recursively unless --maxdepth is given. Symbolic links are not followed.
#   • Lines are counted as newline characters, like `wc -l`. In a well-formed
#     JSONL file this is the number of records; a last record without a final
#     newline is not counted. validate_jsonl.py reports such files.
#   • Each file is counted by one process, so parallelism comes from counting
#     several files at once. Decompressing is CPU-bound; plain files are
#     limited by the filesystem.
#   • A file that cannot be read or decompressed is reported as failed, left
#     out of the totals, and the script exits non-zero.
#   • The TSV has the columns path, bytes_on_disk, bytes, lines, sorted by
#     path. It is written to FILE.partial and renamed when complete. An
#     existing FILE is not overwritten.
###############################################################################

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 [-s SUFFIX] [-j JOBS] [-d MAXDEPTH] [-o OUTPUT] PATH [PATH ...]" >&2
  exit 1
}

mkdir -p ./logs

# ---- Parse and validate user input -----------------------------------------
SUFFIX=""
JOBS=""         # Number of files counted concurrently, default set below
MAXDEPTH=""     # Empty means search subdirectories at any depth
OUTPUT=""       # Optional TSV report
PATHS=()

while (( $# > 0 )); do
  case "$1" in
    -s|--suffix)   SUFFIX=${2:-}; shift 2 || usage ;;
    -j|--jobs)     JOBS=${2:-}; shift 2 || usage ;;
    -d|--maxdepth) MAXDEPTH=${2:-}; shift 2 || usage ;;
    -o|--output)   OUTPUT=${2:-}; shift 2 || usage ;;
    -h|--help)     usage ;;
    -*)            echo "Error: unknown option '$1'" >&2; usage ;;
    *)             PATHS+=("$1"); shift ;;
  esac
done

(( ${#PATHS[@]} > 0 )) || usage
for p in "${PATHS[@]}"; do
  if [[ ! -d "$p" && ! -f "$p" ]]; then
    echo "Error: '$p' is not a directory or a regular file" >&2
    exit 1
  fi
done

THREADS=${SLURM_CPUS_PER_TASK:-1}   # Total cores Slurm gives us
JOBS=${JOBS:-$THREADS}

if ! [[ "$JOBS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: jobs must be a positive integer, got '$JOBS'" >&2
  exit 1
fi
if [[ -n "$MAXDEPTH" ]] && ! [[ "$MAXDEPTH" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: maxdepth must be a positive integer, got '$MAXDEPTH'" >&2
  exit 1
fi
if [[ -n "$OUTPUT" ]]; then
  if [[ ! -d "$(dirname -- "$OUTPUT")" ]]; then
    echo "Error: output directory '$(dirname -- "$OUTPUT")' does not exist" >&2
    exit 1
  fi
  if [[ -e "$OUTPUT" ]]; then
    echo "Error: '$OUTPUT' already exists; not overwritten" >&2
    exit 1
  fi
fi

# Resolve the paths so that overlapping arguments yield identical file paths,
# which are deduplicated below and therefore counted only once.
mapfile -d '' PATHS < <(realpath -ez -- "${PATHS[@]}" | sort -zu)

# ---- Logging helpers ---------------------------------------------------------
# Each message is written with a single printf so lines from concurrent jobs
# do not interleave.
log()       { printf '%s %s\n' "$(date '+%F %T')" "$*"; }
log_error() { printf '%s ERROR %s\n' "$(date '+%F %T')" "$*" >&2; }
human()     { numfmt --to=iec-i --suffix=B --format='%.1f' "$1"; }
duration()  { printf '%dh%02dm%02ds' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# ---- Per-file worker -----------------------------------------------------------
# count_one INDEX FILE
# Runs in a separate bash started by xargs, so it reports failure through its
# return value and the ledger instead of relying on `set -e`.
count_one() {
  local idx=$1 src=$2
  local tag="[$idx/$TOTAL_FILES]"
  local disk counts lines bytes

  if ! disk=$(stat -c %s -- "$src"); then
    log_error "$tag $src: cannot stat"
    printf 'FAIL\t0\t0\t0\t%s\n' "$src" >> "$LEDGER"
    return 1
  fi

  # wc -lc prints the newline count and the byte count. pipefail makes a
  # failing zstd (corrupt or truncated input) fail the whole count.
  if [[ "$src" == *.zst ]]; then
    counts=$(set -o pipefail; zstd -dcq -- "$src" | wc -lc) || counts=""
  else
    counts=$(wc -lc < "$src") || counts=""
  fi
  if [[ -z "$counts" ]]; then
    log_error "$tag $src: could not be read; not counted"
    printf 'FAIL\t%s\t0\t0\t%s\n' "$disk" "$src" >> "$LEDGER"
    return 1
  fi
  read -r lines bytes <<< "$counts"

  printf 'OK\t%s\t%s\t%s\t%s\n' "$disk" "$bytes" "$lines" "$src" >> "$LEDGER"
  local on_disk=""
  if [[ "$src" == *.zst ]]; then
    on_disk=" ($(human "$disk") on disk)"
  fi
  log "$tag $src: $lines lines, $(human "$bytes")$on_disk"

  # Overall progress across all files, from the shared ledger.
  local done_files done_bytes run_time
  read -r done_files done_bytes < <(awk -F '\t' '{n++; b += $2} END {printf "%d %d\n", n, b}' "$LEDGER")
  run_time=$(( $(date +%s) - JOB_START ))
  log "Overall: $done_files/$TOTAL_FILES files, $(human "$done_bytes") / $(human "$TOTAL_BYTES") on disk," \
      "elapsed $(duration "$run_time"), ETA $(duration $(( (TOTAL_BYTES - done_bytes) * run_time / (done_bytes > 0 ? done_bytes : 1) )))"
}

# ---- Collect input files -------------------------------------------------------
# The work directory holds the file list and the ledger. It lives on
# node-local /tmp, away from the data.
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT
LIST="$WORK_DIR/files"
LEDGER="$WORK_DIR/ledger.tsv"

# • find:   locate regular files, optionally only those ending in SUF or
#           SUF.zst, and skip temporary outputs left by interrupted runs
# • sort:   order the work and drop duplicates from overlapping paths
# Written to a file rather than read through a pipe so that a failing find
# (e.g. permission denied) stops the script instead of silently dropping files.
FIND_ARGS=(-type f ! -name '*.partial')
if [[ -n "$MAXDEPTH" ]]; then
  # find expects -maxdepth before the tests, otherwise it warns.
  FIND_ARGS=(-maxdepth "$MAXDEPTH" "${FIND_ARGS[@]}")
fi
if [[ -n "$SUFFIX" ]]; then
  FIND_ARGS+=(\( -name "*$SUFFIX" -o -name "*$SUFFIX.zst" \))
fi
find "${PATHS[@]}" "${FIND_ARGS[@]}" -print0 > "$LIST"
LC_ALL=C sort -zu -o "$LIST" "$LIST"
mapfile -d '' FILES < "$LIST"

TOTAL_FILES=${#FILES[@]}
TOTAL_BYTES=0
if (( TOTAL_FILES > 0 )); then
  TOTAL_BYTES=$(xargs -0 stat -c %s < "$LIST" | awk '{s += $1} END {printf "%d", s}')
fi

log "Job ${SLURM_JOB_ID:-none} on $(hostname), $(zstd --version)"
log "Counting ${TOTAL_FILES} file(s), $(human "$TOTAL_BYTES") on disk, in" \
    "$(printf "'%s' " "${PATHS[@]}")${MAXDEPTH:+to depth $MAXDEPTH }${SUFFIX:+with suffix '$SUFFIX' }with ${JOBS} job(s)"

if (( TOTAL_FILES == 0 )); then
  log "Nothing to count."
  exit 0
fi

# ---- Main counting loop -----------------------------------------------------
# • xargs:  run count_one in parallel, one file per call, up to $JOBS at once
# The ledger records one line per finished file for the overall progress, the
# final summary and the TSV report.
: > "$LEDGER"
JOB_START=$(date +%s)

export TOTAL_FILES TOTAL_BYTES LEDGER JOB_START
export -f count_one log log_error human duration

for i in "${!FILES[@]}"; do
  printf '%s\0%s\0' "$(( i + 1 ))" "${FILES[$i]}"
done | xargs -0 -n2 -P "$JOBS" bash -c 'count_one "$1" "$2"' _ || true

# ---- Report ----------------------------------------------------------------
if [[ -n "$OUTPUT" ]]; then
  TMP="$OUTPUT.partial"
  {
    printf 'path\tbytes_on_disk\tbytes\tlines\n'
    awk -F '\t' -v OFS='\t' '$1 == "OK" {print $5, $2, $3, $4}' "$LEDGER" | LC_ALL=C sort
  } > "$TMP"
  if ! mv -n "$TMP" "$OUTPUT" || [[ -e "$TMP" ]]; then
    log_error "could not rename $TMP to $OUTPUT"
    exit 1
  fi
  log "Wrote per-file counts to '$OUTPUT'"
fi

# ---- Summary ---------------------------------------------------------------
read -r ok_files disk_bytes out_bytes out_lines < <(awk -F '\t' '$1 == "OK" {n++; d += $2; b += $3; l += $4} END {printf "%d %d %d %d\n", n, d, b, l}' "$LEDGER")
mapfile -t FAILED < <(awk -F '\t' '$1 == "FAIL" {print $5}' "$LEDGER")
RUN_TIME=$(( $(date +%s) - JOB_START ))

log "Counted ${ok_files}/${TOTAL_FILES} file(s) in $(duration "$RUN_TIME"):" \
    "${out_lines} lines, $(human "$out_bytes") uncompressed, $(human "$disk_bytes") on disk"

if (( ${#FAILED[@]} > 0 || ok_files != TOTAL_FILES )); then
  log_error "${#FAILED[@]} file(s) failed, $(( TOTAL_FILES - ok_files - ${#FAILED[@]} )) not processed:"
  for f in "${FAILED[@]}"; do
    log_error "  $f"
  done
  exit 1
fi

log "Done."
