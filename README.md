# Data helpers

Scripts for routine work on large data files on LUMI: compressing, splitting,
joining, packing, counting and checking them. Each script is a SLURM batch job
that you submit with `sbatch`. Scripts that read data also accept
zstd-compressed `.zst` files and decompress them on the fly.

| Script | What it does | Default resources |
|---|---|---|
| [`compress_files.sh`](#compress_filessh) | Compresses every file under a directory with zstd and removes the originals | `small`, 128 CPUs, 32G, 3 h |
| [`decompress_files.sh`](#decompress_filessh) | Decompresses every `.zst` file under one or more directories | `debug`, 32 CPUs, 32G, 29 min |
| [`concatenate_files.sh`](#concatenate_filessh) | Joins the matching files in a directory into one file | `small`, 1 CPU, 4G, 1 h |
| [`split_files.sh`](#split_filessh) | Splits large files into parts at line boundaries | `small`, 16 CPUs, 16G, 2 h |
| [`count_lines.sh`](#count_linessh) | Counts the lines and bytes of files, without decompressing them to disk | `small`, 32 CPUs, 8G, 1 h |
| [`validate_jsonl.py`](#validate_jsonlpy) | Checks that JSONL files are well formed and have the required fields | `small`, 32 CPUs, 32G, 2 h |
| [`pack_files.sh`](#pack_filessh) | Packs directories into single `.tar.zst` archives | `small`, 32 CPUs, 32G, 3 h |
| [`unpack_files.sh`](#unpack_filessh) | Unpacks `.tar.zst` and `.tar` archives | `small`, 4 CPUs, 8G, 1 h |

All scripts work the same way:

- **The same command line.** Every script is run as
  `sbatch <script> [options] PATH [PATH ...]`, where the paths are directories,
  files or archives, and an option letter means the same thing in every script
  (see [Shared options](#shared-options)).
- **Nothing half-written.** Output is written to `*.partial` first and renamed
  only once it is complete and checked. A job that fails or runs out of time
  never leaves a truncated file behind.
- **Nothing overwritten.** Existing outputs are never replaced. The file is
  reported as failed instead, and you decide which copy to keep.
- **Failures are loud.** Bad arguments stop the script before it starts. A file
  that fails is listed at the end of the log, and the job exits with an error,
  so `sacct` shows it as `FAILED`.

## Running on LUMI

### Before the first run

Logs go to `./logs` in the `LUMI-data-helpers` directory, so create it first, if it doesn't exist:

```bash
mkdir -p LUMI-data-helpers/logs
```

Always submit from the `LUMI-data-helpers` directory. Relative data paths are then
resolved from there as well; absolute paths avoid any doubt.

The scripts charge the project set in their `#SBATCH --account` line
(`project_462001516`). Change it there if you work under another project.

### Submitting, watching and stopping a job

```bash
sbatch compress_files.sh /scratch/project_462001516/my_data   # prints the job ID
squeue --me                                                   # your queued and running jobs
tail -f ./logs/compress_<jobid>.out                          # progress
cat ./logs/compress_<jobid>.err                              # errors
sacct -j <jobid> --format=JobID,State,Elapsed,MaxRSS          # result after it ends
scancel <jobid>                                               # stop it
```

Each log is named `<job name>_<job ID>`, where the job name is the script's
verb: `compress_…`, `decompress_…`, `concatenate_…`, `split_…`, `count_…`,
`validate_…`, `pack_…` or `unpack_…`. Progress and the final summary go to
`.out`, and errors go to `.err`.

### Changing time, CPUs or partition

Options given to `sbatch` override the `#SBATCH` lines in the script, so you
don't need to edit the script for a single job:

```bash
sbatch --time=12:00:00 compress_files.sh /scratch/project_462001516/big_data
sbatch --partition=small --time=04:00:00 decompress_files.sh /scratch/project_462001516/big_data
sbatch --cpus-per-task=16 --mem=8G compress_files.sh -j 4 /scratch/project_462001516/small_data
```

The scripts read the CPU count from SLURM, so their parallelism follows
`--cpus-per-task` automatically. The `debug` partition allows at most 30
minutes; move larger jobs to `small`.

### If a job stops early

Resubmit the same command:

- `compress_files.sh` and `decompress_files.sh` continue where they stopped.
  Finished files are no longer matched, and leftover `*.partial` files are
  overwritten.
- `split_files.sh`, `pack_files.sh` and `unpack_files.sh` work through their
  inputs one at a time. Inputs already done are reported as failed because
  their output exists, the rest are processed, and leftovers of the
  interrupted input are replaced.
- `concatenate_files.sh` starts over, because it produces a single output.
- `count_lines.sh` and `validate_jsonl.py` only read, so they simply run again.

### Seeing the usage without submitting

`-h` prints the usage line. Run it with `bash` on the login node, since
`sbatch -h` would describe `sbatch` itself:

```bash
bash compress_files.sh -h
```

Do the actual work through `sbatch`, not on the login node.

### Chaining jobs

`--dependency=afterok` starts a job only after another one has finished
successfully. For example, decompress, join and compress the result again:

```bash
DATA=/scratch/project_462001516/labels/fin_Latn

jid=$(sbatch --parsable decompress_files.sh -k "$DATA")
jid=$(sbatch --parsable --dependency=afterok:$jid concatenate_files.sh "$DATA")
sbatch --dependency=afterok:$jid compress_files.sh -d 1 -s "$(basename "$DATA")_concatenated.jsonl" "$(dirname "$DATA")"
```

### Shared options

| Option | Meaning | Used by |
|---|---|---|
| `-s, --suffix SUF` | only files whose names end in `SUF` (count and validate also match `SUF.zst`) | compress, decompress, count, validate |
| `-j, --jobs N` | files processed at the same time | compress, decompress, count, validate |
| `-i, --interval S` | seconds between progress reports | all but count |
| `-d, --maxdepth N` | how many levels below `DIR` to look, as with `find -maxdepth`; `1` means files directly in `DIR` | compress, decompress, concatenate, count, validate |
| `-l, --level N` | zstd compression level, 1–19 | compress, split, pack |
| `-o, --output PATH` | where the output goes | concatenate, split, count, pack, unpack |
| `-k, --keep` | keep the inputs that would otherwise be removed | decompress, unpack |
| `-h, --help` | print the usage line | all |

A new script should reuse these letters for the same meanings (see
[Adding a new helper script](#adding-a-new-helper-script)).

## Scripts

### `compress_files.sh`

Compresses every file under a directory with zstd, at any depth unless `-d` is
given. Each `FILE` becomes `FILE.zst` next to it. Files already ending in `.zst`
are skipped. An original is removed only after its `.zst` has been checked with
`zstd -t`.

```
sbatch compress_files.sh [options] DIR
```

| Option | Default | Meaning |
|---|---|---|
| `-l, --level N` | `19` | zstd level, 1–19 |
| `-s, --suffix SUF` | all files | only compress files ending in `SUF`, e.g. `.jsonl` |
| `-j, --jobs N` | `1` | files compressed at the same time; the CPUs are divided between them |
| `-i, --interval S` | `60` | seconds between progress reports per file |
| `-d, --maxdepth N` | no limit | how many levels below `DIR` to look |

The level sets the speed. Measured on multilingual JSONL text, on one thread:

| Level | Speed per thread | Notes |
|---|---|---|
| 19 | ≈ 2 MB/s | smallest output, ≈ 70 MB memory per thread |
| 15 | ≈ 6 MB/s | |
| 12 | ≈ 35 MB/s | output ≈ 15% larger than at 19 |
| 9 | ≈ 70 MB/s | |
| 3 | ≈ 330 MB/s | |

A few large files run fastest with the default `-j 1`, where one zstd process
uses every CPU. Many small files can't use many threads each, so raise `-j` for
them.

Examples:

```bash
# Compress all JSONL files under a directory at level 19
sbatch compress_files.sh -s .jsonl /scratch/project_462001516/labels

# Many small files: faster level, 16 files at a time (8 threads each)
sbatch compress_files.sh -l 12 -j 16 /scratch/project_462001516/samples

# Only the files directly in the directory, with more time
sbatch --time=12:00:00 compress_files.sh -d 1 /scratch/project_462001516/labels
```

If `FILE.zst` already exists next to `FILE`, that file is reported as failed,
because it's unclear which copy is complete. Check them and remove one by hand.

### `decompress_files.sh`

Decompresses every `.zst` file under one or more directories, at any depth
unless `-d` is given. Each `FILE.zst` becomes `FILE` next to it. zstd checks the
checksum and size stored by `compress_files.sh`, so a corrupt or truncated
`.zst` fails instead of producing a short file. The `.zst` is removed only after
a successful decompression, and kept with `-k`.

```
sbatch decompress_files.sh [options] DIR [DIR ...]
```

| Option | Default | Meaning |
|---|---|---|
| `-s, --suffix SUF` | all `.zst` files | only decompress files ending in `SUF.zst`, e.g. `.jsonl` |
| `-j, --jobs N` | all allocated CPUs | files decompressed at the same time |
| `-i, --interval S` | `60` | seconds between progress reports per file |
| `-k, --keep` | remove | keep the `.zst` files |
| `-d, --maxdepth N` | no limit | how many levels below each `DIR` to look |

zstd decompresses each file on a single thread, so the script speeds up by
decompressing many files at once. With many jobs the filesystem, not the CPU,
usually sets the speed; lower `-j` if adding more doesn't help.

Examples:

```bash
# Decompress everything under one directory
sbatch decompress_files.sh /scratch/project_462001516/labels

# Several directories (the shell expands the wildcard), keeping the .zst files
sbatch decompress_files.sh -k /scratch/project_462001516/labels_*

# Only JSONL files, on the small partition for a large job
sbatch --partition=small --time=04:00:00 decompress_files.sh -s .jsonl /scratch/project_462001516/labels
```

If `FILE` already exists next to `FILE.zst`, that file is reported as failed.
This also happens when you rerun over files already decompressed with `-k`.

### `concatenate_files.sh`

Joins the matching files in a directory into one file. A newline is added after
any file that doesn't end in one, so JSONL records from different files never
merge. Files are joined in a fixed path order, so the output is the same on
every run.

Unlike the other scripts, this one reads **only the files directly in `DIR`**
by default. Use `-d` to include subdirectories.

```
sbatch concatenate_files.sh [options] DIR
```

| Option | Default | Meaning |
|---|---|---|
| `-p, --pattern GLOB` | `'*.jsonl'` | only files whose names match `GLOB`; quote it |
| `-o, --output FILE` | `DIR/../<name of DIR>_concatenated.jsonl` | output file; its directory must exist |
| `-i, --interval S` | `60` | seconds between overall progress reports |
| `-d, --maxdepth N` | `1` | how many levels below `DIR` to look |

The script refuses to start if the output already exists, so a rerun never
duplicates records. Remove the output by hand to rebuild it. Compressed files
are copied as they are, so decompress them first.

Examples:

```bash
# Join the JSONL files in labels/fin_Latn into labels/fin_Latn_concatenated.jsonl
sbatch concatenate_files.sh /scratch/project_462001516/labels/fin_Latn

# Join descriptor files from the subdirectories into a chosen output file
sbatch concatenate_files.sh -d 2 -p 'descriptors_*.jsonl' \
    -o /scratch/project_462001516/all/fin_Latn_harmonized.jsonl \
    /scratch/project_462001516/descriptors/fin_Latn
```

With `-d 2` the files are joined in path order, so files in a subdirectory can
come before files directly in `DIR` (`DIR/a/x.jsonl` sorts before
`DIR/top.jsonl`).

### `split_files.sh`

Splits large line-based files, such as JSONL, into parts at line boundaries,
for example to give each task of a job array its own part. `data.jsonl` becomes
`data_parts/data_00000.jsonl`, `data_00001.jsonl`, and so on. A `.zst` input is
decompressed on the fly and its parts are compressed again
(`data_00000.jsonl.zst`, …). The parts keep the input's order, so joining them
gives the input back.

```
sbatch split_files.sh (-L LINES | -b SIZE) [options] FILE [FILE ...]
```

| Option | Default | Meaning |
|---|---|---|
| `-L, --lines N` | | at most `N` lines per part |
| `-b, --bytes SIZE` | | at most `SIZE` uncompressed bytes of whole lines per part, e.g. `500M` or `2G` |
| `-o, --output DIR` | `<stem>_parts` next to `FILE` | directory for the parts; only with a single `FILE` |
| `-l, --level N` | `3` | zstd level for the parts of a `.zst` input |
| `-i, --interval S` | `60` | seconds between progress reports |

Give exactly one of `-L` and `-b`. The parts are checked before they are
published: their total size must match what was read, and every part but the
last must end in a newline. With `-b`, a single line longer than `SIZE` would be
cut in two; the job then fails and asks for a larger `SIZE`.

For a `.zst` input, the parts are first written uncompressed, then compressed in
parallel, one part per CPU. The job therefore needs room for the uncompressed
parts while it runs. Level 3 is fast; higher levels give smaller parts but are
much slower (see [`compress_files.sh`](#compress_filessh)).

Examples:

```bash
# Parts of 100,000 lines each
sbatch split_files.sh -L 100000 /scratch/project_462001516/labels/fin_Latn.jsonl

# Parts of about 1 GiB from each of several compressed files
sbatch split_files.sh -b 1G /scratch/project_462001516/hplt/*.jsonl.zst

# One array task per part
PARTS=/scratch/project_462001516/labels/fin_Latn_parts
sbatch --array=0-$(( $(ls "$PARTS" | wc -l) - 1 )) my_array_job.sh
```

The number of parts depends on the data, so size the job array from the
directory afterwards, as in the last example. If the output directory already
exists, that file is reported as failed and the others are still split.

### `count_lines.sh`

Counts the lines and bytes of every file under one or more directories, or of
single files. `.zst` files are decompressed on the fly, so their counts describe
the uncompressed content, and nothing is written next to the data.

```
sbatch count_lines.sh [options] PATH [PATH ...]
```

| Option | Default | Meaning |
|---|---|---|
| `-s, --suffix SUF` | all files | only count files ending in `SUF` or `SUF.zst` |
| `-j, --jobs N` | all allocated CPUs | files counted at the same time |
| `-d, --maxdepth N` | no limit | how many levels below each directory to look |
| `-o, --output FILE` | none | also write per-file counts to `FILE` as TSV |

Lines are counted like `wc -l`, as newline characters. In a well-formed JSONL
file that is the number of records; a last record without a final newline isn't
counted, and `validate_jsonl.py` reports such files. The TSV has the columns
`path`, `bytes_on_disk`, `bytes` and `lines`. A file that can't be read or
decompressed, such as a truncated `.zst`, is reported as failed.

Examples:

```bash
# Total number of records in all JSONL files, compressed or not
sbatch count_lines.sh -s .jsonl /scratch/project_462001516/labels

# Per-file counts as a table
sbatch count_lines.sh -s .jsonl -o /scratch/project_462001516/labels_counts.tsv /scratch/project_462001516/labels
```

### `validate_jsonl.py`

Checks that JSONL files are well formed, without changing them. Every line must
be valid UTF-8 and a JSON object with the required fields, there must be no
empty lines, and the file must end in a newline. `.zst` files are decompressed
on the fly, and a corrupt or truncated `.zst` is reported. `NaN` and `Infinity`,
which Python accepts but JSON does not, count as errors.

```
sbatch validate_jsonl.py [options] PATH [PATH ...]
```

| Option | Default | Meaning |
|---|---|---|
| `-s, --suffix SUF` | `.jsonl` | only check files ending in `SUF` or `SUF.zst` |
| `-f, --field NAME` | none | require this top-level field in every record; repeat for several |
| `-j, --jobs N` | all allocated CPUs | files checked at the same time |
| `-d, --maxdepth N` | no limit | how many levels below each directory to look |
| `-m, --max-errors N` | `10` | errors logged per file; all are counted |
| `-i, --interval S` | `60` | seconds between progress reports per file |

Errors are logged as `FILE:LINE: message`, and the job fails if any file has
one. Each file is checked by one process at roughly 100–300 MB/s, so a single
huge file is slow; split it first to check the parts in parallel. The script
runs on the system Python 3.6 of LUMI's compute nodes and needs only the
standard library, so no container is needed.

Examples:

```bash
# Check all labelled files and require an id and a text field
sbatch validate_jsonl.py -f id -f text /scratch/project_462001516/labels

# Check one file, logging up to 100 errors
sbatch validate_jsonl.py -m 100 /scratch/project_462001516/labels/fin_Latn.jsonl.zst
```

### `pack_files.sh`

Packs each given directory into a single `DIR.tar.zst` archive next to it.
Lustre is slow with many small files, and LUMI limits how many files a project
may have, so packing finished data helps with both. The archive holds `DIR`
itself, so [`unpack_files.sh`](#unpack_filessh) recreates it.

```
sbatch pack_files.sh [options] DIR [DIR ...]
```

| Option | Default | Meaning |
|---|---|---|
| `-l, --level N` | `19` | zstd level |
| `-o, --output FILE` | `DIR.tar.zst` | archive path; only with a single `DIR` |
| `-i, --interval S` | `60` | seconds between progress reports |
| `-r, --remove` | keep | delete `DIR` once the archive has been verified against it |

Unlike `compress_files.sh`, this script keeps the source by default, because a
whole directory tree is harder to restore than one file. Every archive is
tested with `zstd -t`. With `-r`, the archive is also compared with `DIR`
before `DIR` is deleted: every file's content, size and metadata must match,
and both must list the same entries. Any difference, such as a file added
while packing, keeps `DIR` and fails the job.

Examples:

```bash
# Pack one directory into labels/fin_Latn.tar.zst and keep the directory
sbatch pack_files.sh /scratch/project_462001516/labels/fin_Latn

# Pack every language directory separately and remove them once verified
sbatch pack_files.sh -r /scratch/project_462001516/labels/*/

# Data that is already compressed: a low level saves time
sbatch pack_files.sh -l 3 /scratch/project_462001516/hplt_samples
```

`decompress_files.sh` treats a `.tar.zst` like any other `.zst` and would turn
it into a `.tar`, so use `unpack_files.sh` for archives.

### `unpack_files.sh`

Unpacks `.tar.zst` archives, such as those from `pack_files.sh`, and plain
`.tar` archives. Each archive is unpacked next to itself, and removed only
after a successful extraction unless `-k` is given.

```
sbatch unpack_files.sh [options] ARCHIVE [ARCHIVE ...]
```

| Option | Default | Meaning |
|---|---|---|
| `-o, --output DIR` | next to each archive | directory to unpack into |
| `-i, --interval S` | `60` | seconds between progress reports |
| `-k, --keep` | remove | keep the archives |

Each archive is first extracted into a temporary directory, and its contents
are moved into place only after a complete extraction. If any top-level entry
of the archive already exists in the destination, nothing is moved and the
archive is reported as failed.

Examples:

```bash
# Unpack and remove the archive
sbatch unpack_files.sh /scratch/project_462001516/labels/fin_Latn.tar.zst

# Unpack several archives into another directory and keep them
sbatch unpack_files.sh -k -o /scratch/project_462001516/work /scratch/project_462001516/labels/*.tar.zst
```

## Adding a new helper script

1. **Start from the closest existing script.** Copy `decompress_files.sh` or
   `count_lines.sh` for work done file by file in parallel,
   `split_files.sh` or `pack_files.sh` for inputs handled one at a time,
   `concatenate_files.sh` for a single output stream, and `validate_jsonl.py`
   for Python. Python scripts must run on the system Python 3.6 of LUMI's
   compute nodes with only the standard library, or load a container.
2. **Keep the header.** Set the `#SBATCH` lines, with a short verb as
   `--job-name` and logs in `./logs/%x_%j.out` and `.err`. Below them, keep the
   comment block with the usage line, the options and notes on behaviour.
3. **Keep the command line.** Use `sbatch <script> [options] DIR`, and reuse
   the letters in [Shared options](#shared-options) for the same meanings.
   Check every argument before starting work, and support `-h`.
4. **Keep the safety rules.** Write to `*.partial` and rename when done, never
   overwrite existing output, log with the shared `log` and `log_error`
   helpers, and exit with an error if anything failed.
5. **Document it here.** Add a row to the table at the top, add any new shared
   options to [Shared options](#shared-options), and add a section under
   [Scripts](#scripts) using the template below.

Template for a new section:

````markdown
### `new_script.sh`

One or two sentences: what it reads, what it writes, and what it removes.

```
sbatch new_script.sh [options] DIR
```

| Option | Default | Meaning |
|---|---|---|
| `-x, --example N` | `1` | what it changes |

Examples:

```bash
# What this example does
sbatch new_script.sh /scratch/project_462001516/my_data
```

What happens with existing outputs, and what a rerun does.
````
