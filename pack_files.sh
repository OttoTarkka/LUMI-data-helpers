#!/bin/bash
#SBATCH --job-name=pack
#SBATCH --account=project_462001516
#SBATCH --partition=small
#SBATCH --time=03:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=32G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Pack each given directory into a single DIR.tar.zst archive next to it, to
# cut the number of files on Lustre. unpack_files.sh restores it.
#
#   sbatch pack_files.sh [options] DIR [DIR ...]
#
# Options
#   -l, --level N      zstd level (1-19, default 19)
#   -o, --output FILE  archive path; only with a single DIR (default: DIR.tar.zst)
#   -i, --interval S   seconds between progress reports (default 60)
#   -r, --remove       delete DIR once the archive has been verified against it
#                      (default: keep DIR)
#
# Notes
#   • Unlike compress_files.sh, the source is kept by default, because
#     removing a whole directory tree is harder to undo than one file.
#   • The archive holds DIR itself, so unpacking it recreates DIR. Members are
#     stored in name order, so packing the same data gives the same archive.
#     Symbolic links are stored as links; their targets are not packed.
#   • Files already compressed (e.g. .zst) are packed as they are. Compressing
#     them again gains little, so a lower --level saves time for such data.
#   • The archive is written to FILE.partial, tested with `zstd -t`, then
#     renamed to FILE. With --remove, it is also compared with DIR first:
#     every file's content, size and metadata must match (`tar --compare`) and
#     both must list the same entries, so files added to DIR while it was
#     packed are not lost. Any difference keeps DIR and fails the job.
#   • A DIR whose archive already exists is skipped as failed. The others are
#     still packed, and resubmitting the same command continues with the
#     directories that are left.
#   • zstd uses every allocated CPU. tar reads the files one at a time, so
#     directories with very many small files are limited by the filesystem.
###############################################################################

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 [-l LEVEL] [-o OUTPUT] [-i SECONDS] [-r] DIR [DIR ...]" >&2
  exit 1
}

mkdir -p ./logs

# ---- Parse and validate user input -----------------------------------------
LEVEL=19        # Default to a high compression level
OUTPUT=""
INTERVAL=60     # Seconds between progress reports
REMOVE=false    # Delete DIR after a verified archive
DIRS=()

while (( $# > 0 )); do
  case "$1" in
    -l|--level)    LEVEL=${2:-}; shift 2 || usage ;;
    -o|--output)   OUTPUT=${2:-}; shift 2 || usage ;;
    -i|--interval) INTERVAL=${2:-}; shift 2 || usage ;;
    -r|--remove)   REMOVE=true; shift ;;
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
if [[ -n "$OUTPUT" ]] && (( ${#DIRS[@]} > 1 )); then
  echo "Error: --output can only be used with a single DIR" >&2
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

# Resolve the directories so that '.' or a trailing slash still gives a proper
# archive name, and so that duplicates are packed only once.
mapfile -d '' DIRS < <(realpath -ez -- "${DIRS[@]}" | sort -zu)

THREADS=${SLURM_CPUS_PER_TASK:-1}   # Total cores Slurm gives us

# ---- Logging helpers ---------------------------------------------------------
log()       { printf '%s %s\n' "$(date '+%F %T')" "$*"; }
log_error() { printf '%s ERROR %s\n' "$(date '+%F %T')" "$*" >&2; }
human()     { numfmt --to=iec-i --suffix=B --format='%.1f' "$1"; }
duration()  { printf '%dh%02dm%02ds' $(( $1 / 3600 )) $(( $1 % 3600 / 60 )) $(( $1 % 60 )); }

# report_progress PIDFILE LABEL TOTAL
# Every $INTERVAL seconds, logs how many bytes the tar process whose PID is in
# PIDFILE has read so far (rchar in /proc/PID/io), out of TOTAL. It is killed
# once tar finishes. sleep gets no inherited descriptors, so a sleep left
# behind by the kill holds no log open.
report_progress() {
  local pidfile=$1 label=$2 total=$3 start=$SECONDS pid pos elapsed
  while sleep "$INTERVAL" < /dev/null > /dev/null 2>&1; do
    pid=$(cat "$pidfile" 2>/dev/null) || continue
    pos=$(awk '/^rchar:/ {print $2}' "/proc/$pid/io" 2>/dev/null) || break
    elapsed=$(( SECONDS - start ))
    if [[ -n "$pos" ]] && (( pos > 0 && elapsed > 0 && total > 0 )); then
      log "$label: $(awk -v p="$pos" -v s="$total" 'BEGIN {printf "%.1f", 100 * p / s}')%" \
          "($(human "$pos") / $(human "$total") read," \
          "$(human $(( pos / elapsed )))/s," \
          "ETA $(duration $(( pos < total ? (total - pos) * elapsed / pos : 0 ))))"
    fi
  done
}

# ---- Per-directory worker ------------------------------------------------------
# pack_one INDEX DIR
# Called in a subshell with `set -e` in effect, so any failing command ends
# the subshell and only this DIR is reported as failed.
pack_one() {
  local idx=$1 src=$2
  local tag="[$idx/${#DIRS[@]}]"
  local parent name
  parent=$(dirname -- "$src")
  name=$(basename -- "$src")

  local out=${OUTPUT:-$parent/$name.tar.zst}
  local tmp="$out.partial"
  if [[ -e "$out" ]]; then
    log_error "$tag $src: $out already exists; not overwritten"
    return 1
  fi
  if [[ ! -d "$(dirname -- "$out")" ]]; then
    log_error "$tag $src: output directory '$(dirname -- "$out")' does not exist"
    return 1
  fi
  # An archive inside DIR would end up packing itself.
  local out_dir
  out_dir=$(realpath -e -- "$(dirname -- "$out")")
  if [[ "$out_dir/" == "$src/"* ]]; then
    log_error "$tag $src: the archive $out must not be inside the directory it packs"
    return 1
  fi
  if [[ -e "$tmp" ]]; then
    log "$tag $src: overwriting leftover $tmp from an interrupted run"
  fi
  # Leave no incomplete archive behind if this subshell fails. The path is
  # expanded now, because the local $tmp is gone when the trap runs.
  trap "rm -f -- $(printf '%q' "$tmp")" EXIT

  local files bytes start
  read -r files bytes < <(find "$src" -type f -printf '%s\n' | awk '{n++; s += $1} END {printf "%d %d\n", n, s}')
  start=$SECONDS
  log "$tag START $src ($files files, $(human "$bytes")) -> $out"

  # tar's PID is saved so the bytes it has read can be followed for progress.
  local pidfile="$WORK_DIR/tar.pid"
  rm -f "$pidfile"
  report_progress "$pidfile" "$tag $src" "$bytes" &
  local reporter=$!
  { printf '%s' "$BASHPID" > "$pidfile"; exec tar -C "$parent" --sort=name -cf - -- "$name"; } \
    | zstd -q --force "-T$THREADS" "-$LEVEL" -o "$tmp"
  kill "$reporter" 2>/dev/null || true
  wait "$reporter" 2>/dev/null || true

  zstd -tq -- "$tmp"

  if [[ "$REMOVE" == true ]]; then
    log "$tag $src: comparing the archive with $src before removing it"
    # Content, size and metadata of every archived entry.
    zstd -dcq -- "$tmp" | tar -C "$parent" --compare -f -
    # The same entries on both sides. tar lists directories with a trailing
    # slash, which is dropped to match find.
    if ! cmp -s \
        <(zstd -dcq -- "$tmp" | tar --quoting-style=literal -tf - | sed 's:/$::' | LC_ALL=C sort) \
        <(cd "$parent" && find "$name" | LC_ALL=C sort); then
      log_error "$tag $src: the archive and $src list different entries; $src kept"
      return 1
    fi
  fi

  if ! mv -n -- "$tmp" "$out" || [[ -e "$tmp" ]]; then
    log_error "$tag $src: could not rename $tmp to $out"
    return 1
  fi
  trap - EXIT

  if [[ "$REMOVE" == true ]]; then
    rm -rf -- "$src"
  fi

  local size elapsed
  size=$(stat -c %s -- "$out")
  elapsed=$(( SECONDS - start ))
  log "$tag DONE $src: $(human "$bytes") -> $(human "$size")" \
      "(ratio $(awk -v i="$bytes" -v o="$size" 'BEGIN {printf "%.2f", (o > 0 ? i / o : 0)}')," \
      "$(duration "$elapsed")$([[ "$REMOVE" == true ]] && echo ", directory removed"))"
}

# ---- Main loop -----------------------------------------------------------------
# The work directory holds the PID file. It lives on node-local /tmp.
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

log "Job ${SLURM_JOB_ID:-none} on $(hostname), $(zstd --version), $(tar --version | head -n 1)"
log "Packing ${#DIRS[@]} directory(ies) with zstd -${LEVEL} and ${THREADS} thread(s)," \
    "$([[ "$REMOVE" == true ]] && echo "removing" || echo "keeping") the directories"

FAILED=()
JOB_START=$(date +%s)
for i in "${!DIRS[@]}"; do
  # `set -e` is ignored inside functions called from `if` or `||`, so the
  # worker runs as a plain statement in a subshell and its status is read
  # afterwards.
  set +e
  ( set -e; pack_one "$(( i + 1 ))" "${DIRS[$i]}" )
  status=$?
  set -e
  if (( status != 0 )); then
    log_error "[$(( i + 1 ))/${#DIRS[@]}] ${DIRS[$i]}: failed; directory kept, no archive written"
    FAILED+=("${DIRS[$i]}")
  fi
done

# ---- Summary ---------------------------------------------------------------
RUN_TIME=$(( $(date +%s) - JOB_START ))
log "Packed $(( ${#DIRS[@]} - ${#FAILED[@]} ))/${#DIRS[@]} directory(ies) in $(duration "$RUN_TIME")"

if (( ${#FAILED[@]} > 0 )); then
  log_error "${#FAILED[@]} directory(ies) failed:"
  for d in "${FAILED[@]}"; do
    log_error "  $d"
  done
  exit 1
fi

log "Done."
