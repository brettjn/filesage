#!/usr/bin/env python3
import os
import sys
import sqlite3
import argparse
import hashlib
import time
from collections import deque
import socket
import uuid
import subprocess
import json
import signal

try:
  from lib.LICENSE_fscan import LICENSE_TEXT
except:
  pass

try:
  from lib.ANNOUNCE_fscan import ANNOUNCE_TEXT
except:
  pass

"""
fileSage  Copyright (C) 2025  Brett J. Nelson
This program comes with ABSOLUTELY NO WARRANTY;
This is free software, and you are welcome to redistribute it
under certain conditions; use --licence for details.

fscan.py - scan a filesystem and record file metadata into an SQLite database.

Usage:
  python fscan.py [-H] ROOT [DBFILE]

ROOT is required and specifies the filesystem root path to scan.
DBFILE defaults to ~/.filesage/fscan.db
 -H enables content hashing of regular files
"""


BATCH_SIZE = 1000

# semantic version for the fscan tool
VERSION = "0.52"

# directories to skip during traversal (absolute normalized paths)
SKIP_DIRS = {os.path.normpath(os.path.abspath(p)) for p in ("/swap.img", "/tmp", "/dev", "/proc", "/sys", "/snap", "/run", "/mnt")}
#SKIP_DIRS = {os.path.normpath(os.path.abspath(p)) for p in ("/swap.img", "/tmp", "/dev", "/proc", "/sys", "/snap", "/media", "/run", "/mnt")}

# relative paths to skip under the scan root. Edit this set to add patterns
# Example: ".cache/mozilla/firefox" will skip any subtree whose path
# relative to the scan root starts with that string.
RELATIVE_SKIP_DIRS = {".cache/mozilla/firefox", ".cache/google-chrome", ".cache/opera", "flatpak/runtime" }

# Global flag set by signal handler to request scan stop
STOP_REQUESTED = False

# Helper to allow the signal handler to be cooperative
def _signal_handler(signum, frame):
  global STOP_REQUESTED
  if STOP_REQUESTED:
    # second CTRL-C -> force exit
    print("Second interrupt received, exiting immediately.", file=sys.stderr)
    sys.exit(1)
  print("Interrupt received, will stop after current file and exit gracefully...", file=sys.stderr)
  STOP_REQUESTED = True

def is_skipped_path(path,allow_mnt,allow_media):
  """Return True if path is equal to or under any configured skip directory."""
  path = os.path.normpath(os.path.abspath(path))
  for s in SKIP_DIRS:
    if (not allow_mnt or s=="/mnt") and (not allow_media or s=="/media"):
      continue
    try:
      if os.path.commonpath([path, s]) == s:
        return True
    except ValueError:
      # different mount points or other odd cases; treat as not under s
      continue
  return False


def is_rel_skipped(path, root):
  """Return True if the path (absolute) is under a relative-skip pattern.

  We compute the path relative to `root` and check whether it equals or is
  underneath any entry in RELATIVE_SKIP_DIRS.
  """
  try:
    rel = os.path.relpath(path, root)
  except Exception:
    return False

  # normalize to posix-like separators for pattern matching
  rel_norm = rel.replace(os.path.sep, '/')
  if rel_norm == '.' or rel_norm == './':
    rel_norm = ''

  for pat in RELATIVE_SKIP_DIRS:
    if not pat:
      continue
    pat_norm = pat.strip('/').replace(os.path.sep, '/')
    if pat_norm == '':
      continue
    # match when the normalized pattern appears anywhere in the relative
    # path as a complete component sequence. Use surrounding slashes so a
    # pattern like "cache/mozilla" doesn't match "unrelatedcache/mozillax".
    rel_slash = '/' + rel_norm.strip('/') + '/'
    pat_slash = '/' + pat_norm.strip('/') + '/'
    if pat_slash in rel_slash:
      return True
  return False


SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS files (
  dev_major INTEGER NOT NULL,
  dev_minor INTEGER NOT NULL,
  ino INTEGER NOT NULL,
  dirpath TEXT NOT NULL,
  name TEXT NOT NULL,
  suffix TEXT,
  mode INTEGER,
  uid INTEGER,
  gid INTEGER,
  size INTEGER,
  atime REAL,
  mtime REAL,
  ctime REAL,
  is_dir INTEGER,
  is_file INTEGER,
  is_symlink INTEGER,
  link_target TEXT,
  transfer_id INTEGER,
  content_hash_id INTEGER,
  scan_run_id INTEGER,
  drive_serial_id INTEGER,
  PRIMARY KEY (dirpath, name, scan_run_id)
);

CREATE INDEX IF NOT EXISTS idx_dirpath_name ON files(dirpath, name);

CREATE INDEX IF NOT EXISTS idx_ctime ON files(ctime);

CREATE INDEX IF NOT EXISTS idx_suffix ON files(suffix);

CREATE TABLE IF NOT EXISTS content_hashes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  content_hash TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS disk_by_id (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  disk_id TEXT,
  name_prefix TEXT,
  link_target TEXT,
  block_device TEXT,
  UNIQUE(disk_id, name_prefix, link_target, block_device)
);

CREATE TABLE IF NOT EXISTS scan_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at REAL NOT NULL,
  finished_at REAL,
  root TEXT NOT NULL,
  name TEXT,
  database_path TEXT,
  db_version INTEGER DEFAULT 1,
  hardware_id TEXT,
  os_id TEXT,
  hostname TEXT,
  passwd_ctime REAL,
  command_line TEXT,
  current_dir TEXT,
  comment TEXT,
  scan_args TEXT,
  log TEXT,
  skip_resume INTEGER DEFAULT 0
);
 
CREATE TABLE IF NOT EXISTS scan_run_disk_by_id (
  scan_run_id INTEGER NOT NULL,
  disk_by_id_id INTEGER NOT NULL,
  PRIMARY KEY(scan_run_id, disk_by_id_id),
  FOREIGN KEY(scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
  FOREIGN KEY(disk_by_id_id) REFERENCES disk_by_id(id) ON DELETE SET NULL
);


CREATE TABLE IF NOT EXISTS drive_serials (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  serial TEXT UNIQUE
);
 
CREATE TABLE IF NOT EXISTS scan_run_state (
  scan_run_id INTEGER PRIMARY KEY,
  queue TEXT,
  processed INTEGER,
  last_path TEXT,
  saved_at REAL,
  FOREIGN KEY(scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE
);
 
"""

INSERT_UPSERT = """
INSERT INTO files
(dev_major, dev_minor, ino, dirpath, name, suffix, mode, uid, gid, size, atime, mtime, ctime,
 is_dir, is_file, is_symlink, link_target, transfer_id, content_hash_id, scan_run_id, drive_serial_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(dirpath, name, scan_run_id) DO UPDATE SET
  dirpath = excluded.dirpath,
  name = excluded.name,
  suffix = excluded.suffix,
  mode = excluded.mode,
  uid = excluded.uid,
  gid = excluded.gid,
  size = excluded.size,
  atime = excluded.atime,
  mtime = excluded.mtime,
  ctime = excluded.ctime,
  is_dir = excluded.is_dir,
  is_file = excluded.is_file,
  is_symlink = excluded.is_symlink,
  link_target = excluded.link_target,
  transfer_id = excluded.transfer_id,
  content_hash_id = excluded.content_hash_id,
  scan_run_id = excluded.scan_run_id,
  drive_serial_id = excluded.drive_serial_id;
"""


def init_db(args):

  db_path = os.path.abspath(os.path.expanduser(args.db))  

  if not (args.print_log or args.resume or args.skip_resume):
  # Ensure parent directory exists for the DB file (skip special names like ':memory:')
    try:
      if db_path and db_path != ':memory:':
        #db_path_expanded = os.path.expanduser(db_path)
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
          try:
            os.makedirs(db_dir, exist_ok=True)
          except Exception:
            # non-fatal; let sqlite raise if the path is invalid
            pass
    except Exception:
      # defensive: ignore problems creating parent directories
      pass

  try:
    conn = sqlite3.connect(db_path, timeout=30)
  except Exception as e:
    print(f"Failed to open database {db_path}: {e}", file=sys.stderr)
    sys.exit(1)

  conn.execute("PRAGMA foreign_keys = ON;")
  conn.executescript(SCHEMA)
  # Perform lightweight migrations for older DBs: add any newly-introduced columns
  try:
    cur = conn.cursor()
    # scan_runs: ensure new columns exist
    try:
      cur.execute("PRAGMA table_info(scan_runs)")
      cols_sr = [r[1] for r in cur.fetchall()]
      if 'log' not in cols_sr:
        try:
          cur.execute("ALTER TABLE scan_runs ADD COLUMN log TEXT")
          conn.commit()
        except Exception:
          pass
      if 'skip_resume' not in cols_sr:
        try:
          cur.execute("ALTER TABLE scan_runs ADD COLUMN skip_resume INTEGER DEFAULT 0")
          conn.commit()
        except Exception:
          pass
      if 'database_path' not in cols_sr:
        try:
          cur.execute("ALTER TABLE scan_runs ADD COLUMN database_path TEXT")
          conn.commit()
        except Exception:
          pass
      if 'name' not in cols_sr:
        try:
          cur.execute("ALTER TABLE scan_runs ADD COLUMN name TEXT")
          conn.commit()
        except Exception:
          pass
    except Exception:
      # non-fatal: continue
      pass

    # files: ensure transfer_id exists
    try:
      cur.execute("PRAGMA table_info(files)")
      cols_files = [r[1] for r in cur.fetchall()]
      if 'transfer_id' not in cols_files:
        try:
          cur.execute("ALTER TABLE files ADD COLUMN transfer_id INTEGER")
          conn.commit()
        except Exception:
          pass
    except Exception:
      pass
  except Exception:
    # non-fatal
    pass

  return conn


def append_run_log(conn, run_id, message):
  """Append a human-readable timestamped log entry to scan_runs.log for run_id.

  This is defensive: failures to write are swallowed so logging never aborts a scan.
  """
  try:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    entry = f"[{ts}] {message}\n"
    cur = conn.cursor()
    # concatenate to existing log (or start with the new entry)
    cur.execute("UPDATE scan_runs SET log = COALESCE(log, '') || ? WHERE id = ?", (entry, run_id))
    conn.commit()
  except Exception:
    # non-fatal: do not raise errors that would stop the scan
    pass


def print_scan_logs(conn, scan_run_id=None):
  """Print logs for a single scan_run (if scan_run_id provided) or all runs."""
  try:
    cur = conn.cursor()
    if scan_run_id is None:
      cur.execute("SELECT id, started_at, root, log FROM scan_runs ORDER BY id")
      rows = cur.fetchall()
    else:
      cur.execute("SELECT id, started_at, root, log FROM scan_runs WHERE id = ?", (scan_run_id,))
      rows = cur.fetchall()
    if not rows:
      print("No scan_runs found.")
      return
    for idx, row in enumerate(rows):
      sid, started_at, rootpath, logtext = row
      try:
        started_s = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(started_at)) if started_at else 'N/A'
      except Exception:
        started_s = str(started_at)
      print(f"=== run id={sid} started_at={started_s} root={rootpath} ===")
      if logtext:
        # print stored log as-is
        print(logtext, end='')
      else:
        print("(no log entries)")
      if idx != len(rows) - 1:
        print()
  except Exception as e:
    print(f"Failed to read logs: {e}", file=sys.stderr)


def make_parser():
  """Build and return the argparse.ArgumentParser for this script.

  Exposed for unit testing CLI parsing without invoking main().
  """
  parser = argparse.ArgumentParser(description="Scan filesystem and store metadata in SQLite.")
  # add -? as an alternate help alias in addition to -h/--help
  parser.add_argument('-?', action='help', help='show this help message and exit')
  parser.add_argument("--database", dest='db', help="SQLite DB file (default: ~/.filesage/fscan.db)", default="~/.filesage/fscan.db")
  parser.add_argument("-s", "--silent", action="store_true", help="suppress progress and start/finish messages")
  parser.add_argument("-H", "--hash", dest='compute_hash', action="store_true", help="compute and store sha256 hash of regular file contents")
  parser.add_argument("-c", "--comment", dest='comment', help="comment to store with this scan run", default=None)
  parser.add_argument("--name", dest='name', help="friendly name for this scan run", default=None)
  parser.add_argument("--version", dest='version', action='store_true', help="Show program version and exit")
  parser.add_argument("--resume", dest='resume', type=int, help="resume an unfinished scan by scan_run id", default=None)
  parser.add_argument("--restart", dest='restart', type=int, help="alias for --resume (resume an unfinished scan by scan_run id)", default=None)
  parser.add_argument("--print-log", dest='print_log', nargs='?', const='ALL', help="Print scan run log(s). Optional scan_run_id. If omitted, prints all runs.", default=None)
  parser.add_argument("--skip-resume", dest='skip_resume', type=int, help="Mark a scan_run id so it will be ignored for resume/listing (sets skip_resume=1)", default=None)
  parser.add_argument("root", nargs='?', default=None, help="Root path to scan (required unless using --license/--version/--resume/--restart/--print-log/--skip-resume)")
  parser.add_argument("--license", dest='license', action='store_true', help='Print license text and exit')
  return parser


def parse_args(argv=None):

  if len(sys.argv) == 1:
    if "ANNOUNCE_TEXT" in globals():
      print(ANNOUNCE_TEXT)
          
  """Parse CLI arguments and return the Namespace. argv is a list (for tests) or None to parse sys.argv."""
  parser = make_parser()
  ns = parser.parse_args(argv)
  # keep backwards compatibility: if --restart was used, map it to resume for main
  try:
    if getattr(ns, 'restart', None) is not None and getattr(ns, 'resume', None) is None:
      ns.resume = ns.restart
  except Exception:
    pass
  return ns

def record_run_start(conn, root, comment=None, scan_args=None):
  cur = conn.cursor()
  started = time.time()
  # collect identifiers
  hardware_id = get_hardware_id()
  hostname = socket.gethostname()
  # compute /etc/passwd ctime for the filesystem rooted at `root`
  passwd_ctime = None
  try:
    passwd_path = os.path.join(root, "etc", "passwd")
    st_passwd = os.stat(passwd_path)
    passwd_ctime = st_passwd.st_ctime
  except Exception:
    # missing file, permission error, or other issues; leave as None
    passwd_ctime = None

  # format passwd_ctime as an ISO8601 UTC string for os_id, if available
  os_id = None
  if passwd_ctime is not None:
    try:
      # use UTC
      os_id = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(passwd_ctime))
    except Exception:
      os_id = None

  # determine database_path from provided scan_args (if any)
  database_path = None
  try:
    if isinstance(scan_args, dict):
      dbval = scan_args.get('db')
      if dbval:
        database_path = os.path.abspath(os.path.expanduser(dbval))
  except Exception:
    database_path = None

  try:
    cur.execute(
      "INSERT INTO scan_runs(started_at, root, database_path, hardware_id, os_id, hostname, passwd_ctime, command_line, current_dir, comment, scan_args) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      (started, root, database_path, hardware_id, os_id, hostname, passwd_ctime, ' '.join(sys.argv), os.getcwd(), comment, json.dumps(scan_args) if scan_args is not None else None),
    )
  except Exception:
    # fallback to insert without scan_args if serialization fails
    try:
      cur.execute(
        "INSERT INTO scan_runs(started_at, root, database_path, hardware_id, os_id, hostname, passwd_ctime, command_line, current_dir, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (started, root, database_path, hardware_id, os_id, hostname, passwd_ctime, ' '.join(sys.argv), os.getcwd(), comment),
      )
    except Exception:
      # final fallback: try without database_path if the alter didn't run yet (very old DB)
      cur.execute(
        "INSERT INTO scan_runs(started_at, root, hardware_id, os_id, hostname, passwd_ctime, command_line, current_dir, comment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (started, root, hardware_id, os_id, hostname, passwd_ctime, ' '.join(sys.argv), os.getcwd(), comment),
      )
  conn.commit()
  # populate disk_by_id entries for this run from /dev/disk/by-id
  try:
    scan_id = cur.lastrowid
    # determine and store run name: prefer provided name in scan_args, otherwise default to "<id> <timestamp> <root>"
    try:
      provided_name = None
      if isinstance(scan_args, dict):
        provided_name = scan_args.get('name')
      if provided_name:
        try:
          conn.execute("UPDATE scan_runs SET name = ? WHERE id = ?", (provided_name, scan_id))
          conn.commit()
        except Exception:
          pass
      else:
        try:
          ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
          default_name = f"{scan_id} {ts} {root}"
          conn.execute("UPDATE scan_runs SET name = ? WHERE id = ?", (default_name, scan_id))
          conn.commit()
        except Exception:
          pass
    except Exception:
      pass
    # populate deduplicated disk_by_id rows and get a representative id
    rep_id = _populate_disk_by_id(conn, scan_id)
    if rep_id:
      try:
        conn.execute("UPDATE scan_runs SET disk_by_id_id = ? WHERE id = ?", (rep_id, scan_id))
        conn.commit()
      except Exception:
        pass
  except Exception:
    # don't fail the run creation if disk by-id population fails
    pass
  # write an initial log entry for this run (human-readable time included)
  try:
    append_run_log(conn, cur.lastrowid, f"Started scan root={root}")
  except Exception:
    pass
  return cur.lastrowid


def get_hardware_id():
  """Attempt to get a hardware-unique id for this machine.

  Strategy:
  - Try reading /etc/machine-id (some distros provide a stable machine id for the host)
  - Fall back to uuid.getnode() (MAC based) as a less-stable but hardware-tied identifier
  - If all else fails, return None
  """
  # prefer /etc/machine-id if available
  try:
    with open("/etc/machine-id", "r") as f:
      val = f.read().strip()
      if val:
        return val
  except Exception:
    pass

  # fallback: MAC address based id
  try:
    node = uuid.getnode()
    # getnode() returns a 48-bit integer; format as hex
    return f"mac-{node:012x}"
  except Exception:
    return None


def get_block_device_name(major, minor):
  """Return block device name (e.g., 'sda') for given major:minor via /sys/dev/block lookup."""
  try:
    sys_path = f"/sys/dev/block/{major}:{minor}"
    real = os.path.realpath(sys_path)
    parts = real.split(os.path.sep)
    if 'block' in parts:
      bi = parts.index('block')
      if bi + 1 < len(parts):
        return parts[bi + 1]
    else:
      return parts[-2]
  except Exception:
    return None


def is_usb(conn, block_dev):
  """Return True if a disk_by_id linked to the most recent scan_run has block_device == block_dev

  and its name_prefix contains 'usb' (case-insensitive). Returns False otherwise.

  Parameters:
  - conn: sqlite3.Connection
  - block_dev: basename of the block device (e.g., 'sdb')
  """
  if not block_dev:
    return False
  try:
    cur = conn.cursor()
    # determine the most recent scan_run_id that has entries in the association table
    cur.execute("SELECT MAX(scan_run_id) FROM scan_run_disk_by_id")
    r = cur.fetchone()
    if not r or r[0] is None:
      return False
    scan_run_id = r[0]
    cur.execute(
      """
      SELECT db.name_prefix
      FROM disk_by_id db
      JOIN scan_run_disk_by_id srd ON db.id = srd.disk_by_id_id
      WHERE srd.scan_run_id = ? AND db.block_device = ?
      LIMIT 1
      """,
      (scan_run_id, block_dev),
    )
    row = cur.fetchone()
    if not row:
      return False
    name_prefix = row[0] or ''
    return 'usb' in name_prefix.lower()
  except Exception:
    return False

def extract_value(key1,key2,out):
  for line in out.splitlines():
    if key1 in line and key2 in line:
      parts = [p.strip() for p in line.split(' ')]
      for p in parts:
        if p.startswith(key2):
          serial = p.split('=', 1)[1].strip()
          if serial:
            return serial
  return ''


def probe_drive_serial(conn, block_dev):
  """Probe drive serial for block device name using hdparm, sysfs, then lsblk.

  Prefers `hdparm -i /dev/<dev>` (via sudo -n to avoid interactive password prompt).
  Falls back to sysfs entries and then to `lsblk` if needed. Returns serial string or None.
  """
  if not block_dev:
    return None

  if is_usb(conn, block_dev):

    # preferred lsblk SERIAL
    try:
      out = subprocess.check_output(["lsblk", "-no", "SERIAL", f"/dev/{block_dev}"], stderr=subprocess.DEVNULL, text=True, timeout=3)
      s = out.strip()
      if s:
        return s
    except Exception:
      pass

  # try blkid UUID
  try:
    blkid_out = subprocess.check_output(["blkid"], stderr=subprocess.DEVNULL, text=True, timeout=3)
    if blkid_out:
      uuid = extract_value(block_dev,"UUID",blkid_out)
      return uuid.split('"')[1]
  except Exception:
    pass
  # fallback to blkid ID_SERIAL if available
  try:
    idser = subprocess.check_output(["blkid", "-s", "ID_SERIAL", "-o", "value", f"/dev/{block_dev}"], stderr=subprocess.DEVNULL, text=True, timeout=3).strip()
    if idser:
      return idser
  except Exception:
    pass


  # prefer hdparm (may require sudo); run non-interactively
  try:
    out = subprocess.check_output(["sudo", "-n", "hdparm", "-i", f"/dev/{block_dev}"], stderr=subprocess.STDOUT, text=True, timeout=5)
    for line in out.splitlines():
      if 'Model=' in line and 'SerialNo=' in line:
        parts = [p.strip() for p in line.split(',')]
        for p in parts:
          if p.startswith('SerialNo='):
            serial = p.split('=', 1)[1].strip()
            if serial:
              return serial
    for line in out.splitlines():
      if 'SerialNo=' in line:
        idx = line.find('SerialNo=')
        serial = line[idx + len('SerialNo='):].split()[0].strip().strip(',')
        if serial:
          return serial
  except subprocess.CalledProcessError:
    # hdparm failed (non-zero exit), continue to sysfs/lsblk
    pass
  except subprocess.TimeoutExpired:
    pass
  except Exception:
    pass

  # try sysfs paths
  try:
    p1 = f"/sys/block/{block_dev}/device/serial"
    if os.path.exists(p1):
      try:
        with open(p1, 'r') as f:
          s = f.read().strip()
          if s:
            return s
      except Exception:
        pass
    p2 = f"/sys/block/{block_dev}/device/wwid"
    if os.path.exists(p2):
      try:
        with open(p2, 'r') as f:
          s = f.read().strip()
          if s:
            return s
      except Exception:
        pass
  except Exception:
    pass

  return None


def get_drive_serial_for_dev(conn, major, minor, cache):
  """Return drive_serials.id for device major/minor; insert if necessary. Uses cache dict."""

  key = (major, minor)
  if key in cache:
    return cache[key]
  
  block = get_block_device_name(major, minor)


  serial = probe_drive_serial(conn, block)



  if serial is None:
    cache[key] = None
    return None
  cur = conn.cursor()
  try:
    cur.execute("SELECT id FROM drive_serials WHERE serial = ?", (serial,))
    r = cur.fetchone()
    if r:
      cache[key] = r[0]
      return r[0]
    cur.execute("INSERT OR IGNORE INTO drive_serials(serial) VALUES (?)", (serial,))
    conn.commit()
    cur.execute("SELECT id FROM drive_serials WHERE serial = ?", (serial,))
    r = cur.fetchone()
    if r:
      cache[key] = r[0]
      return r[0]
  except Exception:
    pass
  cache[key] = None
  return None


def _populate_disk_by_id(conn, scan_run_id):
  """Read /dev/disk/by-id and insert deduplicated rows into disk_by_id.

  Returns the id of the first inserted/found disk_by_id row (or None).

  For each entry we store:
  - disk_id: the filename under /dev/disk/by-id
  - name_prefix: text up to the first dash (or entire name if no dash)
  - link_target: resolved absolute path the symlink points to
  - block_device: basename of the resolved path
  The function inserts using INSERT OR IGNORE and then SELECTs the id for each
  entry. It returns the first id encountered so the caller can reference a
  representative disk_by_id row.
  """
  byid_dir = "/dev/disk/by-id"
  first_id = None
  try:
    if not os.path.isdir(byid_dir):
      return None
    cur = conn.cursor()
    entries = sorted(os.listdir(byid_dir))
    for name in entries:
      try:
        link_path = os.path.join(byid_dir, name)
        # ensure it's a symlink or file
        if not os.path.exists(link_path):
          continue
        # resolve the symlink target to an absolute path
        try:
          resolved = os.path.realpath(link_path)
        except Exception:
          resolved = None
        block_device = os.path.basename(resolved) if resolved else None
        # prefix up to first dash
        if '-' in name:
          prefix = name.split('-', 1)[0]
        else:
          prefix = name
        # insert deduplicated row
        try:
          cur.execute(
            "INSERT OR IGNORE INTO disk_by_id(disk_id, name_prefix, link_target, block_device) VALUES (?, ?, ?, ?)",
            (name, prefix, resolved, block_device),
          )
          conn.commit()
        except Exception:
          # ignore individual insert failures
          pass
        # select the id for this entry
        try:
          cur.execute(
            "SELECT id FROM disk_by_id WHERE disk_id = ? AND name_prefix = ? AND link_target = ? AND block_device = ?",
            (name, prefix, resolved, block_device),
          )
          r = cur.fetchone()
          if r:
            dbid = r[0]
            # record association to this scan_run
            try:
              cur.execute(
                "INSERT OR IGNORE INTO scan_run_disk_by_id(scan_run_id, disk_by_id_id) VALUES (?, ?)",
                (scan_run_id, dbid),
              )
              conn.commit()
            except Exception:
              pass
            if first_id is None:
              first_id = dbid
        except Exception:
          # ignore select failures
          pass
      except Exception:
        # skip problematic entries
        continue
    return first_id
  except Exception:
    # non-fatal: do not raise errors that would stop the scan
    return None


def save_scan_state(conn, scan_run_id, queue, processed, last_path):
  """Persist the current scan queue and progress for a given scan_run_id.

  queue: a deque or list of remaining paths (will be JSON-encoded as a list)
  processed: integer count of processed entries so far
  last_path: most recently processed path (string or None)
  """
  try:
    qlist = list(queue) if queue is not None else []
    cur = conn.cursor()
    cur.execute(
      "INSERT OR REPLACE INTO scan_run_state(scan_run_id, queue, processed, last_path, saved_at) VALUES (?, ?, ?, ?, ?)",
      (scan_run_id, json.dumps(qlist), processed, last_path, time.time()),
    )
    conn.commit()
  except Exception:
    # non-fatal: do not raise errors that would stop the shutdown
    pass


def load_scan_state(conn, scan_run_id):
  """Load saved state for a scan_run_id. Returns (queue_list, processed, last_path) or None."""
  try:
    cur = conn.cursor()
    cur.execute("SELECT queue, processed, last_path FROM scan_run_state WHERE scan_run_id = ?", (scan_run_id,))
    r = cur.fetchone()
    if not r:
      return None
    qjson, processed, last_path = r
    qlist = json.loads(qjson) if qjson else []
    return (qlist, processed or 0, last_path)
  except Exception:
    return None


def delete_scan_state(conn, scan_run_id):
  try:
    conn.execute("DELETE FROM scan_run_state WHERE scan_run_id = ?", (scan_run_id,))
    conn.commit()
  except Exception:
    pass




def record_run_end(conn, run_id):
  # append a finished log entry then record finished_at timestamp
  try:
    append_run_log(conn, run_id, "Finished")
  except Exception:
    pass
  finished = time.time()
  conn.execute("UPDATE scan_runs SET finished_at = ? WHERE id = ?", (finished, run_id))
  conn.commit()

def scan(root, conn, batch_size=BATCH_SIZE, silent=False, compute_hash=False, comment=None, scan_args=None, resume_run_id=None, resume_queue=None, resume_processed=0, resume_last_path=None):

  root = os.path.abspath(root)

  allow_mnt = False
  if root.startswith("/mnt"):
    allow_mnt = True

  allow_media = False
  if root.startswith("/media"):
    allow_media = True

  if not os.path.exists(root):
    print(f"Root path does not exist: {root}", file=sys.stderr)
    sys.exit(2)

  cur       = conn.cursor()
  drive_serial_id_cache = {}
  batch     = []
  # determine whether we're resuming an existing run or starting a new one
  # track whether we've already processed the first resumed directory
  resumed_first_dir = False

  # determine whether we're resuming an existing run or starting a new one
  if resume_run_id is not None:
    run_id = resume_run_id
    queue = deque(resume_queue) if resume_queue is not None and len(resume_queue) > 0 else deque([root])
    processed = int(resume_processed or 0)
    last_path = resume_last_path
  else:
    run_id    = record_run_start(conn, root, comment, scan_args)
    queue     = deque([root])
    processed = 0
    last_path = None

  while queue:
    if STOP_REQUESTED:
      break
    current = queue.popleft()
    # skip any directory that is in or under a configured absolute skip dir
    # or if it matches a relative skip pattern under the scan root
    if is_skipped_path(current, allow_mnt, allow_media) or is_rel_skipped(current, root):
      continue
    try:
      with os.scandir(current) as it:
        # decide whether this directory is the first resumed directory
        is_first_dir = False
        if resume_run_id is not None and not resumed_first_dir:
          is_first_dir = True

        for entry in it:
          if STOP_REQUESTED:
            break

          full = entry.path

          # skip entries that are in or under a configured skip dir
          # or that match a relative-skip pattern under the scan root
          if is_skipped_path(full, allow_mnt, allow_media) or is_rel_skipped(full, root):
            continue
          try:
            st = entry.stat()
          except (FileNotFoundError, PermissionError):
            # skip items that disappear or are inaccessible
            continue

          # get block device majors early (used for first-dir lookup)
          try:
            maj_probe = os.major(st.st_dev)
            minr_probe = os.minor(st.st_dev)
          except Exception:
            maj_probe = None
            minr_probe = None

          is_dir      = 1 if entry.is_dir(follow_symlinks=False) else 0
          is_file     = 1 if entry.is_file(follow_symlinks=False) else 0
          is_symlink  = 1 if entry.is_symlink() else 0
          link_target = None

          if is_symlink:
            continue
            #try:
            #  link_target = os.readlink(full)
            #except OSError:
            #  link_target = None

          dirpath = os.path.dirname(full)
          name    = os.path.basename(full)

          # compute suffix: text after last '.' if present and not a leading-dot filename
          suffix = None

          try:
            parts = name.rsplit('.', 1)
            if len(parts) == 2 and parts[0] != '' and parts[1] != '':
              suffix = parts[1]
          except Exception:
            suffix = None
          # optionally compute content hash (sha256) for regular files
          content_hash = None
          content_hash_id = None
          skip_hash = False
          if compute_hash and is_file:
            # If resuming and this is the first directory being reprocessed,
            # check whether this file is already present for this run and reuse
            # its content_hash_id to avoid re-reading large files.
            if is_first_dir and maj_probe is not None and minr_probe is not None and resume_run_id is not None:
              try:
                cur.execute(
                  "SELECT content_hash_id FROM files WHERE dev_major = ? AND dev_minor = ? AND ino = ? AND scan_run_id = ?",
                  (maj_probe, minr_probe, st.st_ino, resume_run_id),
                )
                rr = cur.fetchone()
                if rr and rr[0] is not None:
                  content_hash_id = rr[0]
                  skip_hash = True
              except Exception:
                skip_hash = False

            if not skip_hash:
              try:
                h = hashlib.sha256()
                # read in chunks
                with open(full, 'rb') as fh:
                  while True:
                    chunk = fh.read(8192)
                    if not chunk:
                      break
                    h.update(chunk)
                content_hash = h.hexdigest()
              except Exception as e:
                print(f"An error occurred: {e}")
                content_hash = None

          # if we computed a content hash, normalize it into the content_hashes table
          if content_hash is not None:
            try:
              # insert-or-ignore then select the id
              cur.execute("INSERT OR IGNORE INTO content_hashes(content_hash) VALUES (?)", (content_hash,))
              cur.execute("SELECT id FROM content_hashes WHERE content_hash = ?", (content_hash,))
              r = cur.fetchone()
              if r:
                content_hash_id = r[0]
            except Exception:
              content_hash_id = None

          # determine drive_serial_id for this file's device (cache per-scan)
          drive_serial_id = None
          try:
            maj   = os.major(st.st_dev)
            minr  = os.minor(st.st_dev)
            drive_serial_id = get_drive_serial_for_dev(conn, maj, minr, drive_serial_id_cache)
          except Exception as e:
            #print(f"An error occurred: {e}")
            drive_serial_id = None
          # placeholder for transfer_id - currently unused, set to None
          transfer_id = None

          row = (
            os.major(st.st_dev),
            os.minor(st.st_dev),
            st.st_ino,
            dirpath,
            name,
            suffix,
            st.st_mode,
            st.st_uid if hasattr(st, "st_uid") else None,
            st.st_gid if hasattr(st, "st_gid") else None,
            st.st_size,
            st.st_atime,
            st.st_mtime,
            st.st_ctime,
            is_dir,
            is_file,
            is_symlink,
            link_target,
            transfer_id,
            content_hash_id,
            run_id,
            drive_serial_id,
          )
          batch.append(row)
          processed += 1
          # track most recent file path
          last_path = full

          # progress output every 1000 files
          if not silent and processed % 1000 == 0:
            try:
              print(f"Scanned {processed} files. Recent: {last_path}")
            except Exception:
              # don't let printing interrupt scanning
              pass

          if is_dir and not is_symlink:
            # queue subdirectory for traversal (but only if not under a skip dir
            # or relative skip pattern)
            if not (is_skipped_path(full, allow_mnt, allow_media) or is_rel_skipped(full, root)):
              queue.append(full)

          if len(batch) >= batch_size:
            cur.executemany(INSERT_UPSERT, batch)
            conn.commit()
            batch.clear()
        # end for
        if STOP_REQUESTED:
          break
        # finished processing this directory; mark that we've handled the resumed-first directory
        if is_first_dir:
          resumed_first_dir = True
    except PermissionError:
      # skip directories we can't enter
      continue
    except FileNotFoundError:
      # directory vanished
      continue
    except OSError:
      # other OS errors skip
      continue

  if batch:
    cur.executemany(INSERT_UPSERT, batch)
    conn.commit()

  # If the scan was interrupted, save remaining queue/state and DO NOT mark finished
  if STOP_REQUESTED:
    try:
      # ensure the directory we were working on is preserved at the front
      try:
        if 'current' in locals() and current is not None:
          # put the current directory back on the left of the queue so resume
          # will re-enter it and continue processing any remaining entries
          if not (len(queue) > 0 and queue[0] == current):
            queue.appendleft(current)
      except Exception:
        pass
      # save current batch progress and queue
      try:
        append_run_log(conn, run_id, f"Interrupted: last_path={last_path}")
      except Exception:
        pass
      save_scan_state(conn, run_id, queue, processed, last_path)
    except Exception:
      pass
    # inform the user which scan_run id was saved so they can resume later
    try:
      print(f"Scan paused and saved as run id={run_id}. Run with --resume {run_id} to resume.", file=sys.stderr)
    except Exception:
      pass
    # leave scan_runs.finished_at as NULL so it appears in unfinished listing
    return processed

  # normal completion: remove any persisted state and mark finished
  try:
    delete_scan_state(conn, run_id)
  except Exception:
    pass
  record_run_end(conn, run_id)
  return processed





def main():
  #print("\n")
  #print("\n")
  #print(sys.argv[1:])
  #print("\n")
  #print("\n")

  args = parse_args()
  # Allow root to be omitted for early-only actions
  try:
    early_flags = (
      getattr(args, 'license', False),
      getattr(args, 'version', False),
      getattr(args, 'resume', None) is not None,
      getattr(args, 'restart', None) is not None,
      getattr(args, 'print_log', None) is not None,
      getattr(args, 'skip_resume', None) is not None,
    )
    if args.root is None and not any(early_flags):
      print("Missing required ROOT argument. Use --help for usage.", file=sys.stderr)
      sys.exit(2)
  except Exception:
    pass

  # --license prints bundled LICENSE_TEXT and exits early
  try:
    if getattr(args, 'license', False):
      try:
        print(LICENSE_TEXT)
      except Exception:
        print("License text not available.")
      sys.exit(0)
  except Exception:
    pass

  # --version should print the tool version and exit early
  try:
    if getattr(args, 'version', False):
      print(VERSION)
      sys.exit(0)
  except Exception:
    # defensive: if VERSION or args.version missing, continue normally
    pass

  conn = init_db(args)
  # If user requested to mark a run as skip-resume, perform the update and exit
  if getattr(args, 'skip_resume', None) is not None:
    # When --skip-resume is used, the only other allowed option is --database
    tokens = sys.argv[1:]
    i = 0
    invalid = False
    while i < len(tokens):
      t = tokens[i]
      if t == '--skip-resume':
        i += 1
        # optional value accepted as separate token
        if i < len(tokens) and not tokens[i].startswith('-'):
          i += 1
        continue
      if t.startswith('--skip-resume='):
        i += 1
        continue
      if t == '--database':
        i += 1
        if i < len(tokens) and not tokens[i].startswith('-'):
          i += 1
        continue
      if t.startswith('--database='):
        i += 1
        continue
      invalid = True
      break
    if invalid:
      print("When using --skip-resume, the only other allowed option is --database", file=sys.stderr)
      conn.close()
      sys.exit(2)

    try:
      cur = conn.cursor()
      cur.execute("UPDATE scan_runs SET skip_resume = 1 WHERE id = ?", (args.skip_resume,))
      if cur.rowcount == 0:
        print(f"No scan_run found with id {args.skip_resume}", file=sys.stderr)
        conn.close()
        sys.exit(2)
      conn.commit()
      try:
        append_run_log(conn, args.skip_resume, "Marked skip_resume via CLI")
      except Exception:
        pass
      print(f"Marked scan_run id={args.skip_resume} skip_resume=1")
      conn.close()
      sys.exit(0)
    except Exception as e:
      print(f"Failed to mark skip_resume: {e}", file=sys.stderr)
      conn.close()
      sys.exit(2)
  # If user requested to print logs, do that and exit early
  if args.print_log is not None:
    # When --print-log is used, the only other allowed option is --database
    tokens = sys.argv[1:]
    i = 0
    invalid = False
    while i < len(tokens):
      t = tokens[i]
      if t == '--print-log':
        i += 1
        # optional value accepted if present and not another flag
        if i < len(tokens) and not tokens[i].startswith('-'):
          i += 1
        continue
      if t.startswith('--print-log='):
        i += 1
        continue
      if t == '--database':
        i += 1
        # expect a value if present and not another flag
        if i < len(tokens) and not tokens[i].startswith('-'):
          i += 1
        continue
      if t.startswith('--database='):
        i += 1
        continue
      # allow the script name token that may be present (shouldn't be in tokens)
      # everything else is invalid when --print-log is present
      invalid = True
      break
    if invalid:
      print("When using --print-log, the only other allowed option is --database", file=sys.stderr)
      conn.close()
      sys.exit(2)
    # args.print_log == 'ALL' when the flag is present without a value
    if args.print_log == 'ALL':
      print_scan_logs(conn, None)
    else:
      try:
        rid = int(args.print_log)
      except Exception:
        print(f"Invalid scan_run id: {args.print_log}", file=sys.stderr)
        conn.close()
        sys.exit(2)
      print_scan_logs(conn, rid)
    conn.close()
    sys.exit(0)
  # If there are unfinished runs, and the user did not request --resume,
  # list them and exit so the user can explicitly choose one to resume.
  try:
    cur = conn.cursor()
    # ignore runs that were explicitly marked skip_resume
    cur.execute("SELECT id, started_at, root FROM scan_runs WHERE finished_at IS NULL AND COALESCE(skip_resume,0) = 0 ORDER BY id")
    unfinished = cur.fetchall()
  except Exception:
    unfinished = []

  if unfinished and args.resume is None:
    print("Found unfinished scan runs:")
    for r in unfinished:
      sid, started_at, rootpath = r
      try:
        started_s = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(started_at)) if started_at else 'N/A'
      except Exception:
        started_s = str(started_at)
      print(f"  id={sid} started_at={started_s} root={rootpath}")
    print("Run with --resume <id> to resume a specific run.")
    conn.close()
    sys.exit(0)

  # if the user requested a resume, load the saved state for that run
  resume_run_id = None
  resume_queue = None
  resume_processed = 0
  resume_last_path = None
  if args.resume is not None:
    resume_run_id = args.resume
    # Ensure the target run is not marked skip_resume
    try:
      cur_check = conn.cursor()
      cur_check.execute("SELECT COALESCE(skip_resume,0) FROM scan_runs WHERE id = ?", (resume_run_id,))
      rr_check = cur_check.fetchone()
      if rr_check and rr_check[0]:
        print(f"scan_run id {resume_run_id} is marked skip_resume and cannot be resumed", file=sys.stderr)
        conn.close()
        sys.exit(2)
    except Exception:
      # fall through to normal load which will fail if run doesn't exist
      pass
    state = load_scan_state(conn, resume_run_id)
    if state is None:
      print(f"No saved state found for scan_run id {resume_run_id}", file=sys.stderr)
      conn.close()
      sys.exit(2)
    resume_queue, resume_processed, resume_last_path = state
    # enforce that only --resume and optionally --database are provided on the command line
    # Allow forms: --resume 42, --resume=42, --database <file>, --database=<file>
    tokens = sys.argv[1:]
    i = 0
    extra_tokens = []
    while i < len(tokens):
      t = tokens[i]
      # resume forms
      if t == '--resume':
        i += 1
        # skip the numeric value if provided as separate token
        if i < len(tokens) and not tokens[i].startswith('-'):
          i += 1
        continue
      if t.startswith('--resume='):
        i += 1
        continue
      # allow database forms
      if t == '--database':
        i += 1
        # skip the value token if present
        if i < len(tokens) and not tokens[i].startswith('-'):
          i += 1
        continue
      if t.startswith('--database='):
        i += 1
        continue
      # everything else is unexpected when resuming
      extra_tokens.append(t)
      i += 1
    if extra_tokens:
      print("When using --resume, only --database may be provided in addition to --resume.", file=sys.stderr)
      print(f"Unexpected tokens: {extra_tokens}", file=sys.stderr)
      conn.close()
      sys.exit(2)
    # load saved scan arguments from the original run so we can reuse them
    try:
      cur = conn.cursor()
      cur.execute("SELECT scan_args FROM scan_runs WHERE id = ?", (resume_run_id,))
      r = cur.fetchone()
      if not r or not r[0]:
        print(f"No saved scan arguments found for run id {resume_run_id}", file=sys.stderr)
        conn.close()
        sys.exit(2)
      saved_args = json.loads(r[0])
    except Exception:
      print(f"Failed to load saved scan arguments for run id {resume_run_id}", file=sys.stderr)
      conn.close()
      sys.exit(2)
    # override runtime options with saved args
    args_root = saved_args.get('root') if isinstance(saved_args, dict) else None
    args_silent = saved_args.get('silent') if isinstance(saved_args, dict) else None
    args_compute_hash = saved_args.get('compute_hash') if isinstance(saved_args, dict) else None
    args_comment = saved_args.get('comment') if isinstance(saved_args, dict) else None
    args_name = saved_args.get('name') if isinstance(saved_args, dict) else None
    # use these when calling scan
    resume_root = args_root or args.root
    resume_silent = bool(args_silent) if args_silent is not None else args.silent
    resume_compute_hash = bool(args_compute_hash) if args_compute_hash is not None else getattr(args, 'compute_hash', False)
    resume_comment = args_comment if args_comment is not None else getattr(args, 'comment', None)
  else:
    # not resumeing: prepare saved scan args to persist
    saved_args = {
      'root': args.root,
      'db': args.db,
      'silent': args.silent,
      'compute_hash': getattr(args, 'compute_hash', False),
      'comment': getattr(args, 'comment', None),
      'name': getattr(args, 'name', None),
    }
  # install signal handlers for graceful shutdown
  signal.signal(signal.SIGINT, _signal_handler)
  try:
    signal.signal(signal.SIGTERM, _signal_handler)
  except Exception:
    # some platforms may not support SIGTERM handling in the same way
    pass
  start = time.time()
  try:
    if not args.silent:
      if resume_run_id is not None:
        qcount = len(resume_queue) if resume_queue is not None else 0
        try:
          print(f"Resuming scan run id={resume_run_id} (processed={resume_processed}, queued={qcount}, last={resume_last_path})")
        except Exception:
          print(f"Resuming scan run id={resume_run_id}")
      else:
        print("Filescan starting.")
    if resume_run_id is not None:
      # record a resume log entry for this run
      try:
        qcount = len(resume_queue) if resume_queue is not None else 0
        append_run_log(conn, resume_run_id, f"Resumeed: processed={resume_processed}, queued={qcount}, last={resume_last_path}")
      except Exception:
        pass
      total = scan(
        resume_root,
        conn,
        silent=resume_silent,
        compute_hash=resume_compute_hash,
        comment=resume_comment,
        resume_run_id=resume_run_id,
        resume_queue=resume_queue,
        resume_processed=resume_processed,
        resume_last_path=resume_last_path,
      )
    else:
      total = scan(
        args.root,
        conn,
        silent=args.silent,
        compute_hash=getattr(args, 'compute_hash', False),
        comment=getattr(args, 'comment', None),
        scan_args=saved_args,
      )
  finally:
    conn.close()
  elapsed = time.time() - start
  if not args.silent:
    print("Finished.")
  print(f"Scanned {total} items under {args.root} in {elapsed:.1f}s. DB: {os.path.abspath(args.db)}")

if __name__ == "__main__":
  main()
