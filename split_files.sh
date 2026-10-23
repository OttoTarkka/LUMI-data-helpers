#!/bin/bash
#SBATCH --job-name=split
#SBATCH --account=project_462001516
#SBATCH --partition=small
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=16G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Split large line-based files (e.g. JSONL) into parts at line boundaries, for
# example to give each task of a job array its own part. A .zst input is
# decompressed on the fly and its parts are compressed again.
#
#   sbatch split_files.sh (-L LINES | -b SIZE) [options] FILE [FILE ...]
#
# Options
#   -L, --lines N      at most N lines per part
#   -b, --bytes SIZE   at most SIZE uncompressed bytes of whole lines per part,
#                      e.g. 500M or 2G (powers of 1024)
#   -o, --output DIR   directory for the parts; only with a single FILE
#                      (default: <stem>_parts next to FILE)
#   -l, --level N      zstd level for the parts of a .zst input (1-19, default 3)
#   -i, --interval S   seconds between progress reports (default 60)
#
# Notes
#   • FILE.jsonl or FILE.jsonl.zst becomes FILE_parts/FILE_00000.jsonl,
#     FILE_00001.jsonl, … (.jsonl.zst for a .zst input). The parts keep the
#     order of the input, so concatenating them gives the input back.
#   • Parts are first written uncompressed into DIR.partial. For a .zst input
#     they are then compressed in parallel, one part per CPU, and tested with
#     `zstd -t`. Only then is DIR.partial renamed to DIR. This needs room for
#     the uncompressed parts while the job runs.
#   • The parts are checked against the input: their total size must match
#     the number of bytes read, and every part but the last must end in a
#     newline. With --bytes, a single line longer than SIZE would otherwise be
#     cut in two; the job then fails and asks for a larger SIZE.
#   • Level 3 compresses at hundreds of MB/s per CPU. Higher levels give
#     smaller parts but are much slower (see compress_files.sh).
#   • A FILE whose output directory already exists is skipped as failed. The
#     others are still split, and resubmitting the same command continues
#     with the files that are left.
###############################################################################

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 (-L LINES | -b SIZE) [-o OUTPUT] [-l LEVEL] [-i SECONDS] FILE [FILE ...]" >&2
  exit 1
}

mkdir -p ./logs

# ---- Parse and validate user input -----------------------------------------
LINES=""
BYTES=""
OUTPUT=""
LEVEL=3         # zstd level for the parts of a .zst input
INTERVAL=60     # Seconds between progress reports
FILES=()

while (( $# > 0 )); do
  case "$1" in
    -L|--lines)    LINES=${2:-}; shift 2 || usage ;;
    -b|--bytes)    BYTES=${2:-}; shift 2 || usage ;;
    -o|--output)   OUTPUT=${2:-}; shift 2 || usage ;;
    -l|--level)    LEVEL=${2:-}; shift 2 || usage ;;
    -i|--interval) INTERVAL=${2:-}; shift 2 || usage ;;
    -h|--help)     usage ;;
    -*)            echo "Error: unknown option '$1'" >&2; usage ;;
    *)             FILES+=("$1"); shift ;;
  esac
done

(( ${#FILES[@]} > 0 )) || usage
for f in "${FILES[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "Error: '$f' is not a regular file" >&2
    exit 1
  fi
done
if [[ -n "$LINES" && -n "$BYTES" ]] || [[ -z "$LINES" && -z "$BYTES" ]]; then
  echo "Error: give exactly one of --lines and --bytes" >&2
  usage
fi
if [[ -n "$LINES" ]] && ! [[ "$LINES" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: lines must be a positive integer, got '$LINES'" >&2
  exit 1
fi
if [[ -n "$BYTES" ]]; then
  if ! [[ "$BYTES" =~ ^[1-9][0-9]*[KMGT]?$ ]]; then
    echo "Error: bytes must be a positive integer with an optional K, M, G or T suffix, got '$BYTES'" >&2
    exit 1
  fi
  BYTES=$(numfmt --from=iec "$BYTES")
fi
if [[ -n "$OUTPUT" ]] && (( ${#FILES[@]} > 1 )); then
  echo "Error: --output can only be used with a single FILE" >&2
  exit 1
fi
if ! [[ "$LEVEL" =~ ^[0-9]+$ ]] || (( LEVEL < 1 || LEVEL > 19 )); then
  echo "Error: level must be an integer from 1 to 19, got '$LEVEL'" >&2
  exit 1
fi
if ! [[ "$INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: interval must be a positive integer, got '$INTERVAL'" >&2
  exit 1
fi

THREADS=${SLURM_CPUS_PER_TASK:-1}   # Total cores Slurm gives us

# ---- Logging helpers ---------------------------------------------------------
log()       { printf '%s %s\n' "$(date '+%F %T')" "$*"; }
log_error() { printf '%s ERROR %s\n' "$(date '+%F %T')" "$*" >&2; }
human()     { numfmt --to=iec-i --suffix=B --format='%.1f' "$1"; }
duration()  { printf '%dh%02dm%02ds' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# report_progress PIDFILE FILE SIZE
# Every $INTERVAL seconds, logs how far the process whose PID is in PIDFILE has
# read into its stdin, which is FILE. It is killed once the split finishes.
# sleep gets no inherited descriptors, so a sleep left behind by the kill
# holds no log open.
report_progress() {
  local pidfile=$1 src=$2 size=$3 start=$SECONDS pid pos elapsed
  while sleep "$INTERVAL" < /dev/null > /dev/null 2>&1; do
    pid=$(cat "$pidfile" 2>/dev/null) || continue
    pos=$(awk '/^pos:/ {print $2}' "/proc/$pid/fdinfo/0" 2>/dev/null) || break
    elapsed=$(( SECONDS - start ))
    if [[ -n "$pos" ]] && (( pos > 0 && elapsed > 0 )); then
      log "$src: $(awk -v p="$pos" -v s="$size" 'BEGIN {printf "%.1f", 100 * p / s}')%" \
          "($(human "$pos") / $(human "$size") on disk," \
          "$(human $(( pos / elapsed )))/s," \
          "ETA $(duration $(( (size - pos) * elapsed / pos ))))"
    fi
  done
}

# ---- Per-file worker -----------------------------------------------------------
# split_one INDEX FILE
# Called in a subshell with `set -e` in effect, so any failing command ends
# the subshell and only this FILE is reported as failed.
split_one() {
  local idx=$1 src=$2
  local tag="[$idx/${#FILES[@]}]"

  # data.jsonl.zst -> name data.jsonl, stem data, extension .jsonl
  local dir name stem ext compressed=false
  dir=$(dirname -- "$src")
  name=$(basename -- "$src")
  if [[ "$name" == *.zst ]]; then
    compressed=true
    name=${name%.zst}
  fi
  stem=${name%.*}
  ext=${name#"$stem"}
  if [[ -z "$stem" ]]; then
    # A name such as '.jsonl' has no stem; keep the whole name as the stem.
    stem=$name
    ext=""
  fi

  local out=${OUTPUT:-$dir/${stem}_parts}
  local tmp="$out.partial"
  if [[ -e "$out" ]]; then
    log_error "$tag $src: $out already exists; not overwritten"
    return 1
  fi
  if [[ ! -d "$(dirname -- "$out")" ]]; then
    log_error "$tag $src: output directory '$(dirname -- "$out")' does not exist"
    return 1
  fi
  if [[ -e "$tmp" ]]; then
    log "$tag $src: removing leftover $tmp from an interrupted run"
    rm -rf -- "$tmp"
  fi
  mkdir -- "$tmp"
  # Leave no incomplete parts behind if this subshell fails. The path is
  # expanded now, because the local $tmp is gone when the trap runs.
  trap "rm -rf -- $(printf '%q' "$tmp")" EXIT

  local size start
  size=$(stat -c %s -- "$src")
  start=$SECONDS
  log "$tag START $src ($(human "$size")) -> $out/"

  # The input is read on stdin by cat or zstd, whose PID is saved so its read
  # offset can be followed for progress. tee copies the stream to a FIFO,
  # where wc counts the lines and bytes that went into split.
  local pidfile="$WORK_DIR/reader.pid" fifo="$WORK_DIR/tap"
  rm -f "$pidfile" "$fifo"
  mkfifo "$fifo"
  wc -lc < "$fifo" > "$WORK_DIR/counts" &
  local counter=$!
  report_progress "$pidfile" "$tag $src" "$size" &
  local reporter=$!

  local split_args=(--numeric-suffixes=0 --suffix-length=5 --additional-suffix="$ext")
  if [[ -n "$LINES" ]]; then
    split_args+=(--lines="$LINES")
  else
    # Whole lines up to BYTES per part. A longer line is cut, caught below.
    split_args+=(--line-bytes="$BYTES")
  fi

  local reader=(cat)
  if [[ "$compressed" == true ]]; then
    reader=(zstd -dcq)
  fi
  { printf '%s' "$BASHPID" > "$pidfile"; exec "${reader[@]}"; } < "$src" \
    | tee "$fifo" \
    | split "${split_args[@]}" - "$tmp/${stem}_"
  wait "$counter"
  kill "$reporter" 2>/dev/null || true
  wait "$reporter" 2>/dev/null || true

  local in_lines in_bytes
  read -r in_lines in_bytes < "$WORK_DIR/counts"

  # ---- Verify the plain parts ----
  local parts=()
  mapfile -d '' parts < <(find "$tmp" -type f -print0 | LC_ALL=C sort -z)
  local part_bytes=0 last_index=$(( ${#parts[@]} - 1 )) i
  for i in "${!parts[@]}"; do
    part_bytes=$(( part_bytes + $(stat -c %s -- "${parts[$i]}") ))
    # wc -l counts the newlines in the last byte: 0 means it is not a newline.
    if (( i < last_index )) && [[ $(tail -c 1 -- "${parts[$i]}" | wc -l) -eq 0 ]]; then
      log_error "$tag $src: ${parts[$i]} does not end in a newline; a line is" \
                "longer than --bytes $(human "$BYTES") and was cut. Use a larger --bytes."
      return 1
    fi
  done
  if (( part_bytes != in_bytes )); then
    log_error "$tag $src: the parts hold $part_bytes bytes, but $in_bytes were read"
    return 1
  fi

  # ---- Compress the parts of a .zst input ----
  if [[ "$compressed" == true ]] && (( ${#parts[@]} > 0 )); then
    log "$tag $src: compressing ${#parts[@]} part(s) with zstd -$LEVEL, $THREADS at a time"
    # --rm removes each plain part only after its .zst was written.
    printf '%s\0' "${parts[@]}" | xargs -0 -n1 -P "$THREADS" zstd -q -T1 "-$LEVEL" --rm
    printf '%s.zst\0' "${parts[@]}" | xargs -0 -n1 -P "$THREADS" zstd -tq
  fi

  if ! mv -nT -- "$tmp" "$out" || [[ -e "$tmp" ]]; then
    log_error "$tag $src: could not rename $tmp to $out"
    return 1
  fi
  trap - EXIT

  local elapsed=$(( SECONDS - start ))
  log "$tag DONE $src: ${in_lines} lines, $(human "$in_bytes") uncompressed," \
      "into ${#parts[@]} part(s) in $(duration "$elapsed")"
}

# ---- Main loop -----------------------------------------------------------------
# The work directory holds the FIFO, the PID file and the counts. It lives on
# node-local /tmp, away from the data.
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

log "Job ${SLURM_JOB_ID:-none} on $(hostname), $(zstd --version)"
log "Splitting ${#FILES[@]} file(s) into parts of at most" \
    "$([[ -n "$LINES" ]] && echo "$LINES lines" || echo "$(human "$BYTES")")" \
    "${OUTPUT:+into '$OUTPUT' }using ${THREADS} CPU(s)"

FAILED=()
JOB_START=$(date +%s)
for i in "${!FILES[@]}"; do
  # `set -e` is ignored inside functions called from `if` or `||`, so the
  # worker runs as a plain statement in a subshell and its status is read
  # afterwards.
  set +e
  ( set -e; split_one "$(( i + 1 ))" "${FILES[$i]}" )
  status=$?
  set -e
  if (( status != 0 )); then
    log_error "[$(( i + 1 ))/${#FILES[@]}] ${FILES[$i]}: failed; input kept, no parts written"
    FAILED+=("${FILES[$i]}")
  fi
done

# ---- Summary ---------------------------------------------------------------
RUN_TIME=$(( $(date +%s) - JOB_START ))
log "Split $(( ${#FILES[@]} - ${#FAILED[@]} ))/${#FILES[@]} file(s) in $(duration "$RUN_TIME")"

if (( ${#FAILED[@]} > 0 )); then
  log_error "${#FAILED[@]} file(s) failed:"
  for f in "${FAILED[@]}"; do
    log_error "  $f"
  done
  exit 1
fi

log "Done."
