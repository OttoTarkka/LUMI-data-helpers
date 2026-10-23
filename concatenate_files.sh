#!/bin/bash
#SBATCH --job-name=concatenate
#SBATCH --account=project_462001516
#SBATCH --partition=small
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Concatenate the matching files in a directory into one file, making sure
# every input ends in a newline so JSONL records from different files never
# merge.
#
#   sbatch concatenate_files.sh [options] /path/to/dir
#
# Options
#   -p, --pattern GLOB  only concatenate files whose names match GLOB
#                       (default '*.jsonl'); quote it so the shell does not
#                       expand it
#   -o, --output FILE   output file
#                       (default DIR/../<name of DIR>_concatenated.jsonl, i.e.
#                       next to DIR, so it is never picked up as an input)
#   -i, --interval S    seconds between overall progress reports (default 60)
#   -d, --maxdepth N    also descend into subdirectories up to N levels below
#                       DIR, as with `find -maxdepth` (default 1: only files
#                       directly in DIR)
#
# Notes
#   • Unlike compress_files.sh and decompress_files.sh, subdirectories are
#     ignored by default. Use e.g. -d 2 to also include files one level down.
#     Symbolic links are not followed.
#   • Files are concatenated in byte-wise (C locale) path order, so the output
#     is identical on every run regardless of the user's locale.
#   • Inputs are copied with cat, so speed is limited by the filesystem. A
#     newline is added only after inputs whose last byte is not one.
#   • The output is written to OUTPUT.partial, its size is checked against the
#     inputs, and only then is it renamed to OUTPUT. A job that fails or is
#     killed by the time limit therefore never leaves a truncated OUTPUT, and
#     resubmitting the same command starts over.
#   • The script refuses to run if OUTPUT already exists, so rerunning never
#     duplicates records. Remove it by hand to rebuild it.
#   • Compressed inputs are copied as they are. Run decompress_files.sh first.
###############################################################################

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 [-p PATTERN] [-o OUTPUT] [-i SECONDS] [-d MAXDEPTH] /path/to/dir" >&2
  exit 1
}

mkdir -p ./logs

# ---- Parse and validate user input -----------------------------------------
PATTERN='*.jsonl'
OUTPUT=""
INTERVAL=60     # Seconds between progress reports
MAXDEPTH=1      # Only files directly in DIR
DIR=""

while (( $# > 0 )); do
  case "$1" in
    -p|--pattern)  PATTERN=${2:-}; shift 2 || usage ;;
    -o|--output)   OUTPUT=${2:-}; shift 2 || usage ;;
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
if [[ -z "$PATTERN" || "$PATTERN" == */* ]]; then
  echo "Error: pattern must be a non-empty file name glob without '/', got '$PATTERN'" >&2
  exit 1
fi
if ! [[ "$INTERVAL" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: interval must be a positive integer, got '$INTERVAL'" >&2
  exit 1
fi
if ! [[ "$MAXDEPTH" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: maxdepth must be a positive integer, got '$MAXDEPTH'" >&2
  exit 1
fi

# Resolve DIR first so that '.' or a trailing slash still gives a proper name.
DIR_ABS=$(realpath -e -- "$DIR")
if [[ -z "$OUTPUT" ]]; then
  OUTPUT="$(dirname -- "$DIR_ABS")/$(basename -- "$DIR_ABS")_concatenated.jsonl"
fi
if [[ ! -d "$(dirname -- "$OUTPUT")" ]]; then
  echo "Error: output directory '$(dirname -- "$OUTPUT")' does not exist" >&2
  exit 1
fi
if [[ -e "$OUTPUT" ]]; then
  echo "Error: '$OUTPUT' already exists; not overwritten" >&2
  exit 1
fi
TMP="$OUTPUT.partial"

# ---- Logging helpers ---------------------------------------------------------
log()       { printf '%s %s\n' "$(date '+%F %T')" "$*"; }
log_error() { printf '%s ERROR %s\n' "$(date '+%F %T')" "$*" >&2; }
human()     { numfmt --to=iec-i --suffix=B --format='%.1f' "$1"; }
duration()  { printf '%dh%02dm%02ds' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# ---- Collect input files -------------------------------------------------------
# The work directory holds the file list and sizes. It lives on node-local
# /tmp, away from the data. The partial output is removed if the script stops
# early; after a successful rename it no longer exists.
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"; rm -f "$TMP"' EXIT
LIST="$WORK_DIR/files"
SIZES="$WORK_DIR/sizes"

# • find:   locate regular files up to $MAXDEPTH levels below DIR that match
#           the pattern, skipping temporary outputs left by an interrupted run
# • sort:   fix the order independently of the locale
# Written to a file rather than read through a pipe so that a failing find
# (e.g. permission denied) stops the script instead of silently dropping files.
find "$DIR" -maxdepth "$MAXDEPTH" -type f -name "$PATTERN" ! -name '*.partial' -print0 > "$LIST"
LC_ALL=C sort -z -o "$LIST" "$LIST"
mapfile -d '' FILES < "$LIST"

TOTAL_FILES=${#FILES[@]}
TOTAL_BYTES=0
FILE_SIZES=()
if (( TOTAL_FILES > 0 )); then
  xargs -0 stat -c %s < "$LIST" > "$SIZES"
  mapfile -t FILE_SIZES < "$SIZES"
  TOTAL_BYTES=$(awk '{s += $1} END {printf "%d", s}' "$SIZES")
fi
if (( ${#FILE_SIZES[@]} != TOTAL_FILES )); then
  log_error "got ${#FILE_SIZES[@]} sizes for ${TOTAL_FILES} file(s)"
  exit 1
fi

log "Job ${SLURM_JOB_ID:-none} on $(hostname)"
log "Concatenating ${TOTAL_FILES} file(s), $(human "$TOTAL_BYTES"), matching '$PATTERN'" \
    "in '$DIR' to depth $MAXDEPTH into '$OUTPUT'"

if (( TOTAL_FILES == 0 )); then
  log "Nothing to concatenate."
  exit 0
fi

# ---- Main concatenation loop ------------------------------------------------
# The output is opened once on fd 3 and every input is appended to it.
if [[ -e "$TMP" ]]; then
  log "Overwriting leftover $TMP from an interrupted run"
fi
exec 3> "$TMP"

JOB_START=$(date +%s)
last=$SECONDS
done_bytes=0
newlines_added=0

for i in "${!FILES[@]}"; do
  f=${FILES[$i]}
  size=${FILE_SIZES[$i]}
  log "[$(( i + 1 ))/$TOTAL_FILES] $f ($(human "$size"))"

  cat -- "$f" >&3
  # wc -l counts the newlines in the last byte: 0 means it is not a newline.
  if (( size > 0 )) && [[ $(tail -c 1 -- "$f" | wc -l) -eq 0 ]]; then
    printf '\n' >&3
    newlines_added=$(( newlines_added + 1 ))
  fi
  done_bytes=$(( done_bytes + size ))

  if (( SECONDS - last >= INTERVAL )); then
    last=$SECONDS
    run_time=$(( $(date +%s) - JOB_START ))
    log "Overall: $(( i + 1 ))/$TOTAL_FILES files, $(human "$done_bytes") / $(human "$TOTAL_BYTES")," \
        "elapsed $(duration "$run_time"), ETA $(duration $(( (TOTAL_BYTES - done_bytes) * run_time / (done_bytes > 0 ? done_bytes : 1) )))"
  fi
done

exec 3>&-

# ---- Verify and publish ----------------------------------------------------
# A size mismatch means an input changed while it was being copied.
expected=$(( TOTAL_BYTES + newlines_added ))
actual=$(stat -c %s "$TMP")
if (( actual != expected )); then
  log_error "$TMP is $actual bytes, expected $expected; inputs changed during the run?"
  exit 1
fi

if ! mv -n "$TMP" "$OUTPUT" || [[ -e "$TMP" ]]; then
  log_error "could not rename $TMP to $OUTPUT"
  exit 1
fi

RUN_TIME=$(( $(date +%s) - JOB_START ))
if (( newlines_added > 0 )); then
  log "Added a missing final newline after ${newlines_added} file(s)"
fi
log "Concatenated ${TOTAL_FILES} file(s) in $(duration "$RUN_TIME"): $(human "$actual") -> '$OUTPUT'" \
    "($(human $(( actual / (RUN_TIME > 0 ? RUN_TIME : 1) )))/s)"
log "Done."
