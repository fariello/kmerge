# kmerge
A high-performance, strictly memory-bounded C-based K-way merge utility for line-based files (e.g., CSV, JSONL). 

`kmerge` streams theoretically infinite volumes of pre-sorted line data into a singular lexicographically sorted output sequence without linearly consuming RAM (unlike traditional approaches like `sort -m` which often stagger under extreme bounds).

## Why `kmerge`?
When dealing with Big Data schemas on HPC clusters (like merging thousands of independent Reddit `.zst` scrapes converted to JSONL or CSV), streaming files utilizing an optimized Min-Heap tournament tree structure prevents `OOM` (Out-Of-Memory) limits. 
- Fast `O(K)` footprint avoiding recursive stack boundaries natively.
- Dynamically throttles "jumbo" buffers smoothly accommodating occasional highly irregular data rows without pre-allocating gigabytes of idle pointers.
- Safe failovers specifically engineered against arbitrary I/O disconnects or `ENOSPC` disk errors.

## Installation
Just compile natively mapped over GCC optimizing explicitly for your architecture. There are no external dependencies.
```bash
make
# or manually:
gcc -Wall -Wextra -O3 -march=native -flto -funroll-loops -std=c11 -D_POSIX_C_SOURCE=200809L -o kmerge kmerge.c
```

## Usage
**Important:** Your files *must* independently be lexicographically sorted *prior* to merging. `kmerge` is a stream-merger, not an absolute sorter.

```bash
./kmerge [OPTIONS] FILE1 FILE2 ...
```

### Options

| Flag | Name | Description | Default |
|:---|:---|:---|:---|
| `-o` | `--output` | Path to the destination file. If omitted, outputs natively to `stdout` for downstream Bash piping. | `stdout` |
| `-e` | `--expected-length` | The initial baseline allocation size in bytes per line. If a line is longer, it dynamically scales up to `jumbo-threshold`. | `16384` (16 KB) |
| `-j` | `--jumbo-threshold` | Maximum explicit string length in bytes allowed before natively throwing an `EXIT_FAILURE` safeguard. | `100` MB |
| `-p` | `--progress` | Emit telemetry tracking lines successfully emitted directly cleanly out to `stderr`. | `false` |
| `-h` / `-?` | `--help` | Detailed flag breakdowns and syntax mapping natively. |  |

### Example
Merge 4 chunk datasets explicitly sizing boundaries to 8KB while emitting progress natively out to `merged.csv`:
```bash
./kmerge -e 8192 -p -o merged.csv chunk1.csv chunk2.csv chunk3.csv chunk4.csv
```

## Internal Architecture
- **Jumbo Safeties:** Input lines usually inhabit standard constraints (e.g., `< 16KB`). When a monstrous line traverses the feed, `kmerge` briefly shifts its memory mode over into "Jumbo" capacities mathematically accommodating the string, then forcefully shreds the geometry right back down to native original bounds to conserve parallel RAM cleanly.
- **Fail Safes:** Native error handling captures trailing file boundaries accurately preserving anomalies avoiding accidental padding concatenation on missing `\n` formats. Explicit tracking logs structural fail hooks on file space limits seamlessly.
