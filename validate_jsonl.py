#!/usr/bin/env python3
#SBATCH --job-name=validate
#SBATCH --account=project_462001516
#SBATCH --partition=small
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=32G
#SBATCH -o ./logs/%x_%j.out
#SBATCH -e ./logs/%x_%j.err

###############################################################################
# Check that JSONL files are well formed: every line is valid UTF-8 and a JSON
# object with the required fields, there are no empty lines, and the file ends
# in a newline. .zst files are decompressed on the fly. Nothing is modified.
#
#   sbatch validate_jsonl.py [options] PATH [PATH ...]
#
# Options
#   -s, --suffix SUF       only check files whose names end in SUF or SUF.zst
#                          (default .jsonl)
#   -f, --field NAME       require this top-level field in every record;
#                          repeat for several fields
#   -j, --jobs N           number of files checked concurrently
#                          (default: all allocated CPUs)
#   -d, --maxdepth N       only descend N levels below each directory, as with
#                          `find -maxdepth`; 1 means files directly in it
#                          (default: no limit)
#   -m, --max-errors N     errors logged per file; all are counted (default 10)
#   -i, --interval S       seconds between progress reports per file (default 60)
#
# Notes
#   • PATH may be a directory or a single file. Subdirectories are searched
#     recursively unless --maxdepth is given. Symbolic links are not followed.
#     A file named directly is checked whatever its suffix.
#   • Errors are reported as FILE:LINE: message. The script exits non-zero if
#     any file has an error or could not be read, e.g. a truncated .zst.
#   • NaN and Infinity, which Python's json module accepts but JSON does not,
#     are reported as errors.
#   • Each file is checked by one process, at roughly 100-300 MB/s of JSON.
#     A single huge file is therefore slow; split it with split_files.sh to
#     check the parts in parallel.
#   • Runs on the system Python 3.6 of LUMI's compute nodes and needs only the
#     standard library and the zstd command.
###############################################################################

"""Validate JSONL files, plain or zstd-compressed, without modifying them."""

import argparse
import json
import logging
import math
import multiprocessing
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import IO, List, NamedTuple, Optional, Sequence, Tuple

LOGGER = logging.getLogger("validate_jsonl")

# Read buffer for the input stream. Large reads keep the per-line overhead
# low on Lustre.
BUFFER_SIZE = 16 * 1024 * 1024

# How often, in lines, a worker checks whether a progress report is due.
PROGRESS_CHECK_LINES = 100000


class FileResult(NamedTuple):
    """Outcome of validating one file.

    Attributes:
        path: The validated file.
        lines: Number of lines read, including a final line without newline.
        size: Number of uncompressed bytes read.
        error_count: Number of problems found, including read failures.
        errors: The first problems found, as "FILE:LINE: message" strings.
    """

    path: Path
    lines: int
    size: int
    error_count: int
    errors: List[str]


class Settings(NamedTuple):
    """Validation settings shared by all workers.

    Attributes:
        fields: Top-level fields every record must have.
        max_errors: Number of error messages kept per file.
        interval: Seconds between progress reports per file.
    """

    fields: Tuple[str, ...]
    max_errors: int
    interval: int


class BelowLevel(logging.Filter):
    """Pass only records below a given level.

    Attributes:
        level: Records at this level or above are dropped.
    """

    def __init__(self, level: int) -> None:
        super().__init__()
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < self.level


def setup_logging() -> None:
    """Log like the shell helpers: info to stdout, errors to stderr.

    Lines are "YYYY-MM-DD HH:MM:SS message", and errors carry an "ERROR"
    prefix, so they land in the job's .out and .err logs respectively.
    """
    datefmt = "%Y-%m-%d %H:%M:%S"
    out = logging.StreamHandler(sys.stdout)
    out.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt))
    out.addFilter(BelowLevel(logging.WARNING))
    err = logging.StreamHandler(sys.stderr)
    err.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt))
    err.setLevel(logging.WARNING)
    LOGGER.addHandler(out)
    LOGGER.addHandler(err)
    LOGGER.setLevel(logging.INFO)


def human(size: float) -> str:
    """Format a byte count with binary prefixes, like `numfmt --to=iec-i`.

    Rounds up, as numfmt does, so that both report the same size.

    Args:
        size: Number of bytes.

    Returns:
        The size as e.g. "1.5GiB".
    """
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024 or unit == "TiB":
            return "{:.1f}{}".format(math.ceil(size * 10) / 10, unit)
        size /= 1024
    raise AssertionError("unreachable")


def duration(seconds: float) -> str:
    """Format a duration like the shell helpers, e.g. "1h02m03s".

    Args:
        seconds: Duration in seconds.

    Returns:
        The formatted duration.
    """
    total = int(seconds)
    return "{}h{:02d}m{:02d}s".format(total // 3600, total % 3600 // 60, total % 60)


def positive_int(text: str) -> int:
    """Parse a positive integer command-line argument.

    Args:
        text: The argument as given.

    Returns:
        The parsed integer.

    Raises:
        argparse.ArgumentTypeError: If the argument is not a positive integer.
    """
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer, got '{}'".format(text))
    if value < 1:
        raise argparse.ArgumentTypeError("must be a positive integer, got '{}'".format(text))
    return value


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse and check the command-line arguments.

    Args:
        argv: Arguments to parse; defaults to sys.argv[1:].

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Check that JSONL files, plain or .zst, are well formed.",
    )
    parser.add_argument("paths", nargs="+", type=Path, metavar="PATH",
                        help="directory to search, or a single file")
    parser.add_argument("-s", "--suffix", default=".jsonl",
                        help="only check files ending in SUF or SUF.zst (default: .jsonl)")
    parser.add_argument("-f", "--field", dest="fields", action="append", default=[],
                        metavar="NAME", help="require this top-level field; repeatable")
    parser.add_argument("-j", "--jobs", type=positive_int, default=None,
                        help="files checked concurrently (default: all allocated CPUs)")
    parser.add_argument("-d", "--maxdepth", type=positive_int, default=None,
                        help="levels below each directory to search (default: no limit)")
    parser.add_argument("-m", "--max-errors", type=positive_int, default=10,
                        help="errors logged per file (default: 10)")
    parser.add_argument("-i", "--interval", type=positive_int, default=60,
                        help="seconds between progress reports per file (default: 60)")
    args = parser.parse_args(argv)

    for path in args.paths:
        if not (path.is_dir() or path.is_file()):
            parser.error("'{}' is not a directory or a regular file".format(path))
    if args.jobs is None:
        args.jobs = int(os.environ.get("SLURM_CPUS_PER_TASK", "1"))
    return args


def find_files(roots: Sequence[Path], suffix: str, maxdepth: Optional[int]) -> List[Path]:
    """Collect the files to validate, like `find` in the shell helpers.

    Args:
        roots: Directories to search, or single files.
        suffix: Only files whose names end in suffix or suffix + ".zst".
        maxdepth: Levels below each directory to search; None for no limit.

    Returns:
        The matching regular files, resolved, deduplicated and sorted
        byte-wise. Symbolic links are not followed, and leftovers named
        *.partial are skipped.

    Raises:
        OSError: If a directory cannot be read, so no file is silently missed.
    """
    suffixes = (suffix, suffix + ".zst")

    def matches(path: Path) -> bool:
        return path.name.endswith(suffixes) and not path.name.endswith(".partial")

    found = set()
    for root in roots:
        root = root.resolve()
        if root.is_file():
            # A file given by name is checked even if its suffix differs.
            found.add(root)
            continue
        # Depth-first walk; depth 1 holds the entries directly in root.
        stack = [(root, 1)]
        while stack:
            directory, depth = stack.pop()
            for entry in directory.iterdir():
                if entry.is_symlink():
                    continue
                if entry.is_dir():
                    if maxdepth is None or depth < maxdepth:
                        stack.append((entry, depth + 1))
                elif entry.is_file() and matches(entry):
                    found.add(entry)
    return sorted(found, key=lambda path: os.fsencode(str(path)))


def open_stream(path: Path) -> Tuple[IO[bytes], Optional[subprocess.Popen]]:
    """Open a file for reading, decompressing it on the fly if it is .zst.

    Args:
        path: The file to read.

    Returns:
        The binary stream and, for a .zst file, the zstd process feeding it,
        whose exit status must be checked once the stream is exhausted.
    """
    if path.name.endswith(".zst"):
        process = subprocess.Popen(
            ["zstd", "-dcq", "--", str(path)],
            stdout=subprocess.PIPE,
            bufsize=BUFFER_SIZE,
        )
        return process.stdout, process
    return path.open("rb", buffering=BUFFER_SIZE), None


def reject_constant(name: str) -> None:
    """Refuse NaN and Infinity, which are not valid JSON.

    Args:
        name: The constant json found, e.g. "NaN".

    Raises:
        ValueError: Always.
    """
    raise ValueError("{} is not valid JSON".format(name))


def check_line(raw: bytes, fields: Tuple[str, ...]) -> Optional[str]:
    """Validate one line without its trailing newline.

    Args:
        raw: The line's bytes.
        fields: Top-level fields the record must have.

    Returns:
        A description of the first problem found, or None if the line is a
        valid record.
    """
    if not raw.strip():
        return "empty line"
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        return "invalid UTF-8 at byte {}".format(error.start + 1)
    try:
        record = json.loads(text, parse_constant=reject_constant)
    except ValueError as error:
        # JSONDecodeError is a ValueError; its message includes the column.
        return "invalid JSON: {}".format(error)
    if not isinstance(record, dict):
        return "not a JSON object but {}".format(type(record).__name__)
    missing = [field for field in fields if field not in record]
    if missing:
        return "missing field(s): {}".format(", ".join(missing))
    return None


def validate_file(task: Tuple[Path, Settings]) -> FileResult:
    """Validate one JSONL file. Runs in a worker process.

    Read failures are recorded as errors instead of raised, so that one bad
    file does not hide the results of the others; the main process fails the
    job if any file has errors.

    Args:
        task: The file to validate and the shared settings.

    Returns:
        The validation result.
    """
    path, settings = task
    errors = []  # type: List[str]
    error_count = 0
    lines = 0
    size = 0

    def record(line_number: int, message: str) -> None:
        nonlocal error_count
        error_count += 1
        if len(errors) < settings.max_errors:
            errors.append("{}:{}: {}".format(path, line_number, message))

    start = time.monotonic()
    last_report = start
    try:
        stream, process = open_stream(path)
    except OSError as error:
        record(0, "cannot open: {}".format(error))
        return FileResult(path, lines, size, error_count, errors)

    last_line_complete = True
    try:
        with stream:
            # A binary stream splits on b"\n" only, unlike text mode, which
            # would also split on "\r" and other line separators.
            for raw in stream:
                lines += 1
                size += len(raw)
                last_line_complete = raw.endswith(b"\n")
                problem = check_line(raw[:-1] if last_line_complete else raw, settings.fields)
                if problem is not None:
                    record(lines, problem)
                if lines % PROGRESS_CHECK_LINES == 0:
                    now = time.monotonic()
                    if now - last_report >= settings.interval:
                        last_report = now
                        LOGGER.info("%s: %d lines, %s, %s/s, %d error(s) so far",
                                    path, lines, human(size),
                                    human(size / max(now - start, 1e-9)), error_count)
    except OSError as error:
        record(lines, "read failed: {}".format(error))
    if process is not None:
        status = process.wait()
        if status != 0:
            record(lines, "zstd exited with status {}; the file is corrupt or truncated".format(status))
    if lines > 0 and not last_line_complete:
        record(lines, "no newline at the end of the file")
    return FileResult(path, lines, size, error_count, errors)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Validate the files and log a summary.

    Args:
        argv: Command-line arguments; defaults to sys.argv[1:].

    Returns:
        0 if every file is valid, 1 otherwise.
    """
    setup_logging()
    args = parse_args(argv)
    settings = Settings(tuple(args.fields), args.max_errors, args.interval)

    files = find_files(args.paths, args.suffix, args.maxdepth)
    total_bytes = sum(path.stat().st_size for path in files)
    LOGGER.info("Job %s on %s, Python %s", os.environ.get("SLURM_JOB_ID", "none"),
                os.uname().nodename, sys.version.split()[0])
    LOGGER.info("Validating %d file(s), %s on disk, in %s%s with suffix '%s'%s, %d job(s)",
                len(files), human(total_bytes),
                " ".join("'{}'".format(path) for path in args.paths),
                " to depth {}".format(args.maxdepth) if args.maxdepth else "",
                args.suffix,
                ", requiring field(s) {}".format(", ".join(args.fields)) if args.fields else "",
                args.jobs)
    if not files:
        LOGGER.info("Nothing to validate.")
        return 0

    # Largest files first, so the longest tasks do not start last.
    order = sorted(files, key=lambda path: path.stat().st_size, reverse=True)
    start = time.monotonic()
    results = []  # type: List[FileResult]
    with multiprocessing.Pool(min(args.jobs, len(files))) as pool:
        tasks = [(path, settings) for path in order]
        for result in pool.imap_unordered(validate_file, tasks):
            results.append(result)
            tag = "[{}/{}]".format(len(results), len(files))
            if result.error_count:
                for message in result.errors:
                    LOGGER.error("%s %s", tag, message)
                if result.error_count > len(result.errors):
                    LOGGER.error("%s %s: %d more error(s) not shown", tag, result.path,
                                 result.error_count - len(result.errors))
            LOGGER.info("%s %s %s: %d lines, %s, %d error(s)", tag,
                        "FAIL" if result.error_count else "OK", result.path,
                        result.lines, human(result.size), result.error_count)

    elapsed = time.monotonic() - start
    failed = sorted((r for r in results if r.error_count),
                    key=lambda r: os.fsencode(str(r.path)))
    LOGGER.info("Validated %d file(s) in %s: %d lines, %s uncompressed",
                len(results), duration(elapsed), sum(r.lines for r in results),
                human(sum(r.size for r in results)))
    if failed:
        LOGGER.error("%d file(s) with errors:", len(failed))
        for result in failed:
            LOGGER.error("  %s (%d error(s))", result.path, result.error_count)
        return 1
    LOGGER.info("Done. All files are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
