# fileSage

A small collection of tools for scanning filesystems into an SQLite database and performing deduplication/hardlink operations.

This repository contains:

- `fscan.py` — command-line scanner that walks a filesystem tree and records file metadata into an SQLite database. Supports content hashing, resume, and run logging.

- `fsgui.py` — Qt-based GUI that wraps compare/transfer/hardlink features and drives `fscan.py` where appropriate.

- `disk_to_iso.sh` — a helper script that generates a per-disk imaging script which runs `fdisk -l` and creates `dd | gzip` commands for each partition.

- `util/fdisk-l` — a small wrapper around `fdisk` that ensures `-l` is present and filters out loopbacks from the output.

## Quick start

Requirements
- Python 3.8+ (development used 3.11 on Linux)
- PyQt5 or PySide6 to run the GUI (`fsgui.py`) — install one of these with `pip install PyQt5` or `pip install PySide6`.
- Standard Unix tools: `fdisk`, `dd`, `gzip`, `sed`, `lsblk` (optional)

Running the scanner (CLI)
```
# Basic scan (required ROOT argument)
python3 fscan.py /path/to/root --database /path/to/fscan.db

# Compute content hashes (may be slow)
python3 fscan.py -H /path/to/root --database /path/to/fscan.db

# Resume a run by id
python3 fscan.py --resume 42 --database /path/to/fscan.db

# Print bundled license text
python3 fscan.py --license
```

Notes
- `ROOT` is required for normal scans. It may be omitted for early-only actions like `--license`, `--print-log`, `--resume`, `--skip-resume`, or `--version`.
- The scanner writes a `scan_runs` row for each run and stores per-file metadata in the `files` table.

Running the GUI
```
python3 fsgui.py
```
The GUI exposes compare/transfer and hardlink/dedupe operations. It prefers `PyQt5` and falls back to `PySide6` if available.

Hardlink dry-run
- The HARDLINK tab includes a "Minimum size" filter to skip small files during hardlink operations. The DRY RUN uses the scan in the database only and won't touch the filesystem.

Utilities

- `disk_to_iso.sh`
  - Usage: `./disk_to_iso.sh [-f] <device> <target_dir>`
  - Generates a script named `disk_<device>_to_iso.sh` which:
    - Writes `fdisk -l /dev/<device>` output to `<target_dir>/disk_<device>_geometry.txt` (requires root for fdisk).
    - Creates a pre-partition image (bytes from sector 0 up to the first partition start) and per-partition `dd | gzip` commands that write to `<target_dir>/diskimage_<device>_*.iso.gz`.
  - `-f` / `--force` will create the target directory if it does not exist.
  - The generated script includes `set -euo pipefail` and explicit checks so it aborts if `fdisk` or any `dd` pipeline fails.

- `util/fdisk-l`
  - A small wrapper that runs `fdisk` ensuring `-l` is present and filters out loop-device blocks using `sed`.
  - Example: `./util/fdisk-l /dev/sda` (the wrapper appends `-l` if missing).

Development notes
- The project uses an SQLite schema defined in `fscan.py` (`SCHEMA`). Migrations are performed opportunistically at startup to add new columns.
- Tests (if present) and static checks should be run in a Python virtual environment. Example:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # if you create one
python -m py_compile fscan.py fsgui.py
pytest -q
```

Contributing
- Fixes and improvements are welcome. 

License
- The project bundles license text in `lib/LICENSE_fscan.py` (if present). Use `python3 fscan.py --license` to print it.


