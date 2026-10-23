#!/bin/bash
#SBATCH --job-name=unpack
#SBATCH --account=project_462001516
#SBATCH --partition=small
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Unpack .tar.zst archives, such as those made by pack_files.sh, or plain .tar
# archives. Removes each archive only after a successful extraction, unless
# --keep.
#
#   sbatch unpack_files.sh [options] ARCHIVE [ARCHIVE ...]
#
# Options
#   -o, --output DIR   directory to unpack into (default: next to each archive)
#   -i, --interval S   seconds between progress reports (default 60)
#   -k, --keep         keep the archives after unpacking (default: remove)
#
# Notes
#   • Each archive is first extracted into a temporary directory next to its
#     destination. zstd verifies the archive's checksum while decoding, and
#     tar fails on a truncated archive. Only after a complete extraction are
#     the top-level entries moved into place, so a failed or killed job never
#     leaves a half-unpacked directory.
#   • An archive is skipped as failed if any of its top-level entries already
#     exists in the destination, since it is unclear which copy is complete.
#     Nothing is overwritten. This also applies when rerunning over archives
#     already unpacked with --keep.
#   • Archives are unpacked one at a time. zstd and tar each use one CPU, so
#     more CPUs do not make it faster.
#   • Absolute paths and '..' in member names are refused by GNU tar, so an
#     archive cannot write outside the destination.
###############################################################################

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 [-o OUTPUT] [-i SECONDS] [-k] ARCHIVE [ARCHIVE ...]" >&2
  exit 1
}

mkdir -p ./logs

# ---- Parse and validate user input -----------------------------------------
OUTPUT=""
INTERVAL=60     # Seconds between progress reports
KEEP=false      # Keep the archives after unpacking
ARCHIVES=()

while (( $# > 0 )); do
  case "$1" in
    -o|--output)   OUTPUT=${2:-}; shift 2 || usage ;;
    -i|--interval) INTERVAL=${2:-}; shift 2 || usage ;;
    -k|--keep)     KEEP=true; shift ;;
    -h|--help)     usage ;;
    -*)            echo "Error: unknown option '$1'" >&2; usage ;;
    *)             ARCHIVES+=("$1"); shift ;;
  esac
done

(( ${#ARCHIVES[@]} > 0 )) || usage
for a in "${ARCHIVES[@]}"; do
  if [[ ! -f "$a" ]]; then
    echo "Error: '$a' is not a regular file" >&2
    exit 1
  fi
  if [[ "$a" != *.tar.zst && "$a" != *.tar ]]; then
    echo "Error: '$a' is not a .tar.zst or .tar archive" >&2
    exit 1
  fi
done
if [[ -n "$OUTPUT" && ! -d "$OUTPUT" ]]; then
  echo "Error: output directory '$OUTPUT' does not exist" >&2
  exit 1
fi
if ! [[ "$INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: interval must be a positive integer, got '$INTERVAL'" >&2
  exit 1
fi

mapfile -d '' ARCHIVES < <(realpath -ez -- "${ARCHIVES[@]}" | sort -zu)

# ---- Logging helpers ---------------------------------------------------------
log()       { printf '%s %s\n' "$(date '+%F %T')" "$*"; }
log_error() { printf '%s ERROR %s\n' "$(date '+%F %T')" "$*" >&2; }
human()     { numfmt --to=iec-i --suffix=B --format='%.1f' "$1"; }
duration()  { printf '%dh%02dm%02ds' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# report_progress PIDFILE LABEL SIZE
# Every $INTERVAL seconds, logs how far the process whose PID is in PIDFILE has
# read into its stdin, which is the archive. It is killed once tar finishes.
# sleep gets no inherited descriptors, so a sleep left behind by the kill
# holds no log open.
report_progress() {
  local pidfile=$1 label=$2 size=$3 start=$SECONDS pid pos elapsed
  while sleep "$INTERVAL" < /dev/null > /dev/null 2>&1; do
    pid=$(cat "$pidfile" 2>/dev/null) || continue
    pos=$(awk '/^pos:/ {print $2}' "/proc/$pid/fdinfo/0" 2>/dev/null) || break
    elapsed=$(( SECONDS - start ))
    if [[ -n "$pos" ]] && (( pos > 0 && elapsed > 0 )); then
      log "$label: $(awk -v p="$pos" -v s="$size" 'BEGIN {printf "%.1f", 100 * p / s}')%" \
          "($(human "$pos") / $(human "$size") of the archive," \
          "$(human $(( pos / elapsed )))/s," \
          "ETA $(duration $(( (size - pos) * elapsed / pos ))))"
    fi
  done
}

# ---- Per-archive worker --------------------------------------------------------
# unpack_one INDEX ARCHIVE
# Called in a subshell with `set -e` in effect, so any failing command ends
# the subshell and only this ARCHIVE is reported as failed.
unpack_one() {
  local idx=$1 src=$2
  local tag="[$idx/${#ARCHIVES[@]}]"
  local dest name
  dest=$(realpath -e -- "${OUTPUT:-$(dirname -- "$src")}")
  name=$(basename -- "$src")

  # The temporary directory sits in the destination, so moving the entries
  # into place is a rename on the same filesystem.
  local tmp="$dest/${name%.tar*}.unpack.partial"
  if [[ -e "$tmp" ]]; then
    log "$tag $src: removing leftover $tmp from an interrupted run"
    rm -rf -- "$tmp"
  fi
  mkdir -- "$tmp"
  # Leave no half-extracted data behind if this subshell fails. The path is
  # expanded now, because the local $tmp is gone when the trap runs.
  trap "rm -rf -- $(printf '%q' "$tmp")" EXIT

  local size start
  size=$(stat -c %s -- "$src")
  start=$SECONDS
  log "$tag START $src ($(human "$size")) -> $dest/"

  # The reader's PID is saved so its read offset can be followed for progress.
  local reader=(cat)
  if [[ "$src" == *.zst ]]; then
    reader=(zstd -dcq)
  fi
  local pidfile="$WORK_DIR/reader.pid"
  rm -f "$pidfile"
  report_progress "$pidfile" "$tag $src" "$size" &
  local reporter=$!
  { printf '%s' "$BASHPID" > "$pidfile"; exec "${reader[@]}"; } < "$src" \
    | tar -C "$tmp" -xf -
  kill "$reporter" 2>/dev/null || true
  wait "$reporter" 2>/dev/null || true

  # Check every top-level entry before moving any, so a conflict leaves the
  # destination untouched.
  local entries=() e conflicts=()
  mapfile -d '' entries < <(find "$tmp" -mindepth 1 -maxdepth 1 -printf '%f\0' | LC_ALL=C sort -z)
  for e in "${entries[@]}"; do
    if [[ -e "$dest/$e" || -L "$dest/$e" ]]; then
      conflicts+=("$dest/$e")
    fi
  done
  if (( ${#conflicts[@]} > 0 )); then
    log_error "$tag $src: already exists, not overwritten: ${conflicts[*]}"
    return 1
  fi

  local files bytes
  read -r files bytes < <(find "$tmp" -type f -printf '%s\n' | awk '{n++; s += $1} END {printf "%d %d\n", n, s}')

  for e in "${entries[@]}"; do
    if ! mv -n -- "$tmp/$e" "$dest/" || [[ -e "$tmp/$e" || -L "$tmp/$e" ]]; then
      log_error "$tag $src: could not move $tmp/$e to $dest/; the rest stays in $tmp"
      trap - EXIT
      return 1
    fi
  done
  rmdir -- "$tmp"
  trap - EXIT

  if [[ "$KEEP" != true ]]; then
    rm -f -- "$src"
  fi

  local elapsed=$(( SECONDS - start ))
  log "$tag DONE $src: ${files} files, $(human "$bytes"), as ${entries[*]:-nothing}" \
      "($(duration "$elapsed")$([[ "$KEEP" != true ]] && echo ", archive removed"))"
}

# ---- Main loop -----------------------------------------------------------------
# The work directory holds the PID file. It lives on node-local /tmp.
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

log "Job ${SLURM_JOB_ID:-none} on $(hostname), $(zstd --version), $(tar --version | head -n 1)"
log "Unpacking ${#ARCHIVES[@]} archive(s)${OUTPUT:+ into '$OUTPUT'}," \
    "$([[ "$KEEP" == true ]] && echo "keeping" || echo "removing") the archives"

FAILED=()
JOB_START=$(date +%s)
for i in "${!ARCHIVES[@]}"; do
  # `set -e` is ignored inside functions called from `if` or `||`, so the
  # worker runs as a plain statement in a subshell and its status is read
  # afterwards.
  set +e
  ( set -e; unpack_one "$(( i + 1 ))" "${ARCHIVES[$i]}" )
  status=$?
  set -e
  if (( status != 0 )); then
    log_error "[$(( i + 1 ))/${#ARCHIVES[@]}] ${ARCHIVES[$i]}: failed; archive kept"
    FAILED+=("${ARCHIVES[$i]}")
  fi
done

# ---- Summary ---------------------------------------------------------------
RUN_TIME=$(( $(date +%s) - JOB_START ))
log "Unpacked $(( ${#ARCHIVES[@]} - ${#FAILED[@]} ))/${#ARCHIVES[@]} archive(s) in $(duration "$RUN_TIME")"

if (( ${#FAILED[@]} > 0 )); then
  log_error "${#FAILED[@]} archive(s) failed:"
  for a in "${FAILED[@]}"; do
    log_error "  $a"
  done
  exit 1
fi

log "Done."
