#!/usr/bin/env python3
"""
fsgui.py - simple Qt GUI to compare two fscan SQLite databases

Features:
- Select one or two SQLite DB files created by `fscan.py` (scan DB schema included in user's project).
- Choose a scan_run id from each DB (populated from the `scan_runs` table).
- Compare the two selected runs: report counts of total files, files present in both paths, same content-hash matches, and differing files.
- Show files that are present in the first scan but not in the second in a tree view by path.

Dependencies: PyQt5 or PySide6 (the code will attempt to import PyQt5 first and fall back to PySide6).

Run: python fsgui.py
"""
import os
import sys
import sqlite3
import time
import threading
import json
import random
import fcntl
import signal
import shutil
import stat
import errno

from lib.LICENSE_fsgui import LICENSE_TEXT

# semantic version for the GUI tool
VERSION = "0.39"
DB_VERSION = "1.1"

VERSION_NOTE = """DB Only Hardlink Dry Run"""



try:
    # Prefer PyQt5 and import QtNetwork for local sockets/server
    from PyQt5 import QtWidgets, QtGui, QtCore, QtNetwork
    QLocalServer = QtNetwork.QLocalServer
    QLocalSocket = QtNetwork.QLocalSocket
except Exception:
    try:
        # Fallback to PySide6
        from PySide6 import QtWidgets, QtGui, QtCore, QtNetwork
        QLocalServer = QtNetwork.QLocalServer
        QLocalSocket = QtNetwork.QLocalSocket
    except Exception:
        print("This tool requires PyQt5 or PySide6. Install with: pip install PyQt5 or pip install PySide6", file=sys.stderr)
        sys.exit(1)

# Compatibility for Signal name between PyQt5 (pyqtSignal) and PySide6 (Signal)
Signal = getattr(QtCore, 'pyqtSignal', getattr(QtCore, 'Signal', None))


class TransferWorker(QtCore.QThread):
    """Worker thread to transfer checked files from source DB to target DB.

    Emits:
      progress(int) - percentage complete
      finished(int transfer_id, int transferred) - when done
      error(str) - on fatal error
    """
    progress = Signal(int)
    file_progress = Signal(str)
    finished = Signal(int, int)
    cancelled = Signal(int)
    error = Signal(str)

    def __init__(self, db_src, db_tgt, run_src, run_tgt, checked_files, src_root_override=None, tgt_root_override=None):
        super().__init__()
        self.db_src = db_src
        self.db_tgt = db_tgt
        self.run_src = run_src
        self.run_tgt = run_tgt
        self.checked_files = list(checked_files)
        # optional override for the source run root (resolved mountpoint)
        self.src_root_override = src_root_override
        # optional override for the target run root (resolved mountpoint)
        self.tgt_root_override = tgt_root_override
        self._cancelled = False
        # synchronization for handling first-error user decision (abort/continue)
        try:
            self._error_action = None  # 'abort' or 'continue'
            self._error_action_event = threading.Event()
        except Exception:
            self._error_action = None
            self._error_action_event = None

    def _handle_error_and_wait(self, msg: str):
        """Emit error message and wait for GUI to set an action.

        Returns the action string set by GUI ('abort' or 'continue'), or
        None if canceled or timeout.
        """
        try:
            # emit the message to GUI
            try:
                self.error.emit(msg)
            except Exception:
                pass
            # reset prior action and event
            try:
                if self._error_action_event is not None:
                    self._error_action = None
                    self._error_action_event.clear()
            except Exception:
                pass
            # wait until GUI sets the decision or we are cancelled
            while not getattr(self, '_cancelled', False):
                try:
                    if self._error_action_event is None:
                        break
                    # wait up to 0.1s and loop to check _cancelled
                    if self._error_action_event.wait(0.1):
                        break
                except Exception:
                    break
            return getattr(self, '_error_action', None)
        except Exception:
            return None

    def cancel(self):
        """Request cancellation from the GUI thread. The worker will check the flag between files."""
        try:
            self._cancelled = True
        except Exception:
            pass

    def run(self):
        try:
            src_conn = sqlite3.connect(self.db_src)
            tgt_conn = sqlite3.connect(self.db_tgt)
        except Exception as e:
            self.error.emit(f"Failed to open DBs: {e}")
            return

        try:
            src_cur = src_conn.cursor()
            tgt_cur = tgt_conn.cursor()

            # ensure transfers table exists in target
            try:
                tgt_cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS transfers (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      name TEXT,
                      source_db_path TEXT,
                      source_scan_run_id INTEGER,
                      target_db_path TEXT,
                      target_scan_run_id INTEGER,
                      started_at REAL,
                      finished_at REAL,
                      hostname TEXT,
                      os_id TEXT,
                      hardware_id TEXT,
                      passwd_ctime REAL,
                                            current_dir TEXT,
                                            db_version TEXT
                    )
                    """
                )
                tgt_conn.commit()
            except Exception:
                # non-fatal
                pass

            # ensure transfers.name and transfers.db_version columns exist (for older transfer tables)
            try:
                tgt_cur.execute("PRAGMA table_info(transfers)")
                tr_cols = [r[1] for r in tgt_cur.fetchall()]
                if 'name' not in tr_cols:
                    try:
                        tgt_cur.execute("ALTER TABLE transfers ADD COLUMN name TEXT")
                        tgt_conn.commit()
                    except Exception:
                        pass
                if 'db_version' not in tr_cols:
                    try:
                        tgt_cur.execute("ALTER TABLE transfers ADD COLUMN db_version TEXT")
                        tgt_conn.commit()
                    except Exception:
                        pass
            except Exception:
                pass

            # ensure transfer_id column present in target files table
            try:
                tgt_cur.execute("PRAGMA table_info(files)")
                tgt_cols = [r[1] for r in tgt_cur.fetchall()]
                if 'transfer_id' not in tgt_cols:
                    try:
                        tgt_cur.execute("ALTER TABLE files ADD COLUMN transfer_id INTEGER")
                        tgt_conn.commit()
                    except Exception:
                        pass
            except Exception:
                pass

            # fetch metadata from source scan_run to populate transfer row
            try:
                src_cur.execute("SELECT hostname, os_id, hardware_id, passwd_ctime, current_dir FROM scan_runs WHERE id = ?", (self.run_src,))
                meta = src_cur.fetchone() or (None, None, None, None, None)
                hostname, os_id, hardware_id, passwd_ctime, current_dir = meta
            except Exception:
                hostname = os_id = hardware_id = passwd_ctime = current_dir = None

            # fetch source and target run roots so we can map filesystem paths when copying
            try:
                # always fetch the original src root recorded in the DB; we'll use it
                # to compute relative paths (and to translate DB paths to an overridden
                # mountpoint if the user provided one).
                src_cur.execute("SELECT root FROM scan_runs WHERE id = ?", (self.run_src,))
                rr = src_cur.fetchone()
                orig_src_root = rr[0] if rr and rr[0] else None
            except Exception:
                orig_src_root = None
            try:
                # effective source root used for some checks/cases: prefer GUI-provided override
                src_root = self.src_root_override if getattr(self, 'src_root_override', None) else orig_src_root
            except Exception:
                src_root = orig_src_root
            try:
                tgt_cur.execute("SELECT root FROM scan_runs WHERE id = ?", (self.run_tgt,))
                rr2 = tgt_cur.fetchone()
                orig_tgt_root = rr2[0] if rr2 and rr2[0] else None
            except Exception:
                orig_tgt_root = None
            try:
                tgt_root = self.tgt_root_override if getattr(self, 'tgt_root_override', None) else orig_tgt_root
            except Exception:
                tgt_root = orig_tgt_root

            started_at = time.time()
            try:
                tgt_cur.execute(
                    "INSERT INTO transfers(source_db_path, source_scan_run_id, target_db_path, target_scan_run_id, started_at, finished_at, hostname, os_id, hardware_id, passwd_ctime, current_dir, db_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (self.db_src, self.run_src, self.db_tgt, self.run_tgt, started_at, None, hostname, os_id, hardware_id, passwd_ctime, current_dir, DB_VERSION),
                )
                tgt_conn.commit()
                transfer_id = tgt_cur.lastrowid
            except Exception as e:
                self.error.emit(f"Failed to create transfer record: {e}")
                return

            total = len(self.checked_files)
            transferred = 0

            def map_content_hash(src_ch_id):
                if not src_ch_id:
                    return None
                try:
                    src_cur.execute("SELECT content_hash FROM content_hashes WHERE id = ?", (src_ch_id,))
                    r = src_cur.fetchone()
                    if not r or not r[0]:
                        return None
                    ch = r[0]
                    tgt_cur.execute("INSERT OR IGNORE INTO content_hashes(content_hash) VALUES (?)", (ch,))
                    tgt_conn.commit()
                    tgt_cur.execute("SELECT id FROM content_hashes WHERE content_hash = ?", (ch,))
                    rr = tgt_cur.fetchone()
                    return rr[0] if rr else None
                except Exception:
                    return None

            def map_drive_serial(src_ds_id):
                if not src_ds_id:
                    return None
                try:
                    src_cur.execute("SELECT serial FROM drive_serials WHERE id = ?", (src_ds_id,))
                    r = src_cur.fetchone()
                    if not r or not r[0]:
                        return None
                    serial = r[0]
                    tgt_cur.execute("INSERT OR IGNORE INTO drive_serials(serial) VALUES (?)", (serial,))
                    tgt_conn.commit()
                    tgt_cur.execute("SELECT id FROM drive_serials WHERE serial = ?", (serial,))
                    rr = tgt_cur.fetchone()
                    return rr[0] if rr else None
                except Exception:
                    return None

            for idx, (dirpath, name) in enumerate(self.checked_files):
                # check cancellation flag between items
                if getattr(self, '_cancelled', False):
                    break
                try:
                    src_cur.execute(
                        "SELECT dev_major, dev_minor, ino, dirpath, name, suffix, mode, uid, gid, size, atime, mtime, ctime, is_dir, is_file, is_symlink, link_target, content_hash_id, drive_serial_id FROM files WHERE scan_run_id = ? AND dirpath = ? AND name = ? LIMIT 1",
                        (self.run_src, dirpath, name),
                    )
                    r = src_cur.fetchone()
                    if not r:
                        # skip missing
                        pass
                    else:
                        (dev_major, dev_minor, ino, dirpath_s, name_s, suffix, mode, uid, gid, size, atime_v, mtime_v, ctime_v, is_dir_v, is_file_v, is_symlink_v, link_target, src_ch_id, src_ds_id) = r
                        tgt_ch_id = map_content_hash(src_ch_id)
                        tgt_ds_id = map_drive_serial(src_ds_id)

                        # Emit current filename being processed for UI
                        try:
                            full = os.path.join(dirpath_s, name_s)
                            self.file_progress.emit(full)
                        except Exception:
                            pass

                        # Attempt to copy the actual file/directory/symlink to the target filesystem
                        # Construct a sensible target path by mapping source run root -> target run root
                        def _copy_item():
                            # Construct the actual source filesystem path. If the user supplied
                            # a resolved-source override and the DB recorded an original root,
                            # translate the DB-stored path into the override mountpoint so we
                            # attempt to read the file from the correct device.
                            src_path_db = os.path.join(dirpath_s, name_s)
                            src_path = src_path_db
                            try:
                                if getattr(self, 'src_root_override', None) and orig_src_root:
                                    abs_db_root = os.path.abspath(orig_src_root)
                                    abs_db_path = os.path.abspath(src_path_db)
                                    # if the DB path is under the recorded root, compute relative
                                    # path and join with override
                                    if os.path.commonpath([abs_db_path, abs_db_root]) == abs_db_root:
                                        rel_src = os.path.relpath(abs_db_path, abs_db_root)
                                        src_path = os.path.normpath(os.path.join(self.src_root_override, rel_src))
                                    else:
                                        # fallback: join override with DB path stripped of leading /
                                        src_path = os.path.normpath(os.path.join(self.src_root_override, src_path_db.lstrip(os.path.sep)))
                            except Exception:
                                # on any error fall back to DB path
                                src_path = src_path_db
                            # Determine target full path
                            try:
                                if tgt_root:
                                    # If src_root is known and src_path is under it, preserve relative layout
                                    try:
                                        # Prefer computing the relative path based on the original
                                        # DB-recorded root so the directory layout in the target
                                        # mirrors the scan's original layout. If that's not
                                        # available, fall back to using the effective src_root.
                                        abs_src_db_root = os.path.abspath(orig_src_root) if orig_src_root else None
                                        abs_src_path_db = os.path.abspath(os.path.join(dirpath_s, name_s))
                                        if abs_src_db_root and os.path.commonpath([abs_src_path_db, abs_src_db_root]) == abs_src_db_root:
                                            rel = os.path.relpath(abs_src_path_db, abs_src_db_root)
                                            tgt_path = os.path.normpath(os.path.join(tgt_root, rel))
                                        elif src_root and os.path.commonpath([os.path.abspath(src_path), os.path.abspath(src_root)]) == os.path.abspath(src_root):
                                            rel = os.path.relpath(src_path, src_root)
                                            tgt_path = os.path.normpath(os.path.join(tgt_root, rel))
                                        else:
                                            # Otherwise, place under target root mirroring absolute path
                                            rel = src_path.lstrip(os.path.sep)
                                            tgt_path = os.path.normpath(os.path.join(tgt_root, rel))
                                    except Exception:
                                        rel = src_path.lstrip(os.path.sep)
                                        tgt_path = os.path.normpath(os.path.join(tgt_root, rel))
                                else:
                                    # No target root known: attempt to copy to same absolute path
                                    tgt_path = src_path
                            except Exception:
                                tgt_path = src_path

                            # create parent directory
                            try:
                                tgt_dir = os.path.dirname(tgt_path)
                                if tgt_dir and not os.path.exists(tgt_dir):
                                    os.makedirs(tgt_dir, exist_ok=True)
                            except PermissionError as e:
                                # clearly handle permission errors on target
                                try:
                                    action = self._handle_error_and_wait(f"Permission denied creating target directory {tgt_dir}: {e}")
                                except Exception:
                                    action = None
                                if action == 'continue':
                                    return False
                                try:
                                    self._cancelled = True
                                except Exception:
                                    pass
                                return False
                            except OSError as e:
                                try:
                                    action = self._handle_error_and_wait(f"Failed to create target directory {tgt_dir}: {e}")
                                except Exception:
                                    action = None
                                if action == 'continue':
                                    return False
                                try:
                                    self._cancelled = True
                                except Exception:
                                    pass
                                return False

                            # handle symlink
                            if is_symlink_v:
                                try:
                                    # remove existing target if any
                                    try:
                                        if os.path.lexists(tgt_path):
                                            os.remove(tgt_path)
                                    except Exception:
                                        pass
                                    os.symlink(link_target, tgt_path)
                                    # try to preserve ownership of the symlink itself if possible
                                    try:
                                        if hasattr(os, 'lchown') and uid is not None and gid is not None:
                                            os.lchown(tgt_path, uid, gid)
                                    except Exception:
                                        pass
                                    return True
                                except PermissionError as e:
                                    try:
                                        action = self._handle_error_and_wait(f"Permission denied creating symlink {tgt_path}: {e}")
                                    except Exception:
                                        action = None
                                    if action == 'continue':
                                        return False
                                    try:
                                        self._cancelled = True
                                    except Exception:
                                        pass
                                    return False
                                except OSError as e:
                                    try:
                                        action = self._handle_error_and_wait(f"Failed to create symlink {tgt_path}: {e}")
                                    except Exception:
                                        action = None
                                    if action == 'continue':
                                        return False
                                    try:
                                        self._cancelled = True
                                    except Exception:
                                        pass
                                    return False

                            # handle directory
                            if is_dir_v:
                                try:
                                    if not os.path.exists(tgt_path):
                                        os.makedirs(tgt_path, exist_ok=True)
                                    try:
                                        os.chmod(tgt_path, mode or 0o755)
                                    except Exception:
                                        pass
                                    try:
                                        if uid is not None and gid is not None:
                                            os.chown(tgt_path, uid, gid)
                                    except Exception:
                                        pass
                                    return True
                                except PermissionError as e:
                                    try:
                                        action = self._handle_error_and_wait(f"Permission denied creating directory {tgt_path}: {e}")
                                    except Exception:
                                        action = None
                                    if action == 'continue':
                                        return False
                                    try:
                                        self._cancelled = True
                                    except Exception:
                                        pass
                                    return False
                                except OSError as e:
                                    try:
                                        action = self._handle_error_and_wait(f"Failed to create directory {tgt_path}: {e}")
                                    except Exception:
                                        action = None
                                    if action == 'continue':
                                        return False
                                    try:
                                        self._cancelled = True
                                    except Exception:
                                        pass
                                    return False

                            # handle regular file copy
                            if is_file_v:
                                base = tgt_path
                                tmp = base + f".tmp-transfer-{os.getpid()}-{int(time.time() * 1000)}"
                                # ensure no stale tmp
                                try:
                                    if os.path.exists(tmp):
                                        try:
                                            os.remove(tmp)
                                        except Exception:
                                            pass
                                except Exception:
                                    pass

                                # perform copy of file data and basic metadata
                                try:
                                    try:
                                        shutil.copyfile(src_path, tmp)
                                    except PermissionError as e:
                                        try:
                                            action = self._handle_error_and_wait(f"Permission denied copying {src_path} -> {tmp}: {e}")
                                        except Exception:
                                            action = None
                                        try:
                                            if os.path.exists(tmp):
                                                os.remove(tmp)
                                        except Exception:
                                            pass
                                        if action == 'continue':
                                            return False
                                        try:
                                            self._cancelled = True
                                        except Exception:
                                            pass
                                        return False
                                    except OSError:
                                        # fallback to copy2 which also tries to copy metadata
                                        try:
                                            shutil.copy2(src_path, tmp)
                                        except PermissionError as e2:
                                            try:
                                                action = self._handle_error_and_wait(f"Permission denied copying {src_path} -> {tmp}: {e2}")
                                            except Exception:
                                                action = None
                                            try:
                                                if os.path.exists(tmp):
                                                    os.remove(tmp)
                                            except Exception:
                                                pass
                                            if action == 'continue':
                                                return False
                                            try:
                                                self._cancelled = True
                                            except Exception:
                                                pass
                                            return False
                                        except Exception as e2:
                                            try:
                                                action = self._handle_error_and_wait(f"Failed to copy file {src_path} -> {tmp}: {e2}")
                                            except Exception:
                                                action = None
                                            try:
                                                if os.path.exists(tmp):
                                                    os.remove(tmp)
                                            except Exception:
                                                pass
                                            if action == 'continue':
                                                return False
                                            try:
                                                self._cancelled = True
                                            except Exception:
                                                pass
                                            return False

                                    # copy permission bits and timestamps
                                    try:
                                        shutil.copystat(src_path, tmp, follow_symlinks=True)
                                    except Exception:
                                        pass

                                    # try to set ownership if possible
                                    try:
                                        if uid is not None and gid is not None:
                                            os.chown(tmp, uid, gid)
                                    except PermissionError:
                                        # non-fatal if not permitted
                                        pass
                                    except Exception:
                                        pass

                                    # atomic replace into final location
                                    try:
                                        os.replace(tmp, base)
                                    except PermissionError as e:
                                        # likely permission denied on target dir
                                        try:
                                            action = self._handle_error_and_wait(f"Permission denied moving {tmp} -> {base}: {e}")
                                        except Exception:
                                            action = None
                                        try:
                                            if os.path.exists(tmp):
                                                os.remove(tmp)
                                        except Exception:
                                            pass
                                        if action == 'continue':
                                            return False
                                        try:
                                            self._cancelled = True
                                        except Exception:
                                            pass
                                        return False
                                    except OSError:
                                        # final fallback: try shutil.move
                                        try:
                                            shutil.move(tmp, base)
                                        except PermissionError as e2:
                                            try:
                                                action = self._handle_error_and_wait(f"Permission denied moving {tmp} -> {base}: {e2}")
                                            except Exception:
                                                action = None
                                            try:
                                                if os.path.exists(tmp):
                                                    os.remove(tmp)
                                            except Exception:
                                                pass
                                            if action == 'continue':
                                                return False
                                            try:
                                                self._cancelled = True
                                            except Exception:
                                                pass
                                            return False
                                        except Exception as e2:
                                            try:
                                                action = self._handle_error_and_wait(f"Failed to move copied file into place {tmp} -> {base}: {e2}")
                                            except Exception:
                                                action = None
                                            try:
                                                if os.path.exists(tmp):
                                                    os.remove(tmp)
                                            except Exception:
                                                pass
                                            if action == 'continue':
                                                return False
                                            try:
                                                self._cancelled = True
                                            except Exception:
                                                pass
                                            return False

                                    return True
                                except Exception as e:
                                    # Catch-all for unexpected exceptions; report and cleanup
                                    try:
                                        if os.path.exists(tmp):
                                            os.remove(tmp)
                                    except Exception:
                                        pass
                                    try:
                                        action = self._handle_error_and_wait(f"Failed to copy file {src_path} -> {tgt_path}: {e}")
                                    except Exception:
                                        action = None
                                    if action == 'continue':
                                        return False
                                    try:
                                        self._cancelled = True
                                    except Exception:
                                        pass
                                    return False
                            # unknown type: skip
                            return False

                        # perform copy and skip DB update if copy fails
                        try:
                            copied_ok = _copy_item()
                        except Exception:
                            copied_ok = False
                        if not copied_ok:
                            # On failure, emit progress once. The copy helper has
                            # already emitted an error and possibly set
                            # self._cancelled depending on the user's decision.
                            try:
                                pct = int((idx + 1) * 100 / max(1, total))
                                self.progress.emit(pct)
                            except Exception:
                                pass
                            # if the user chose to abort, stop the transfer
                            if getattr(self, '_cancelled', False):
                                break
                            # otherwise continue to next item
                            continue

                        # Check for existing target row by the new primary key (dirpath, name, scan_run_id)
                        existing = None
                        try:
                            tgt_cur.execute(
                                "SELECT dev_major, dev_minor, ino, dirpath, name, suffix, mode, uid, gid, size, atime, mtime, ctime, is_dir, is_file, is_symlink, link_target, transfer_id, content_hash_id, scan_run_id, drive_serial_id FROM files WHERE dirpath = ? AND name = ? AND scan_run_id = ? LIMIT 1",
                                (dirpath_s, name_s, self.run_tgt),
                            )
                            existing = tgt_cur.fetchone()
                        except Exception:
                            existing = None

                        # If an existing row is present, archive it into files_history before replacing
                        if existing:
                            try:
                                tgt_cur.execute(
                                    """
                                    CREATE TABLE IF NOT EXISTS files_history (
                                      dev_major INTEGER,
                                      dev_minor INTEGER,
                                      ino INTEGER,
                                      dirpath TEXT,
                                      name TEXT,
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
                                      drive_serial_id INTEGER
                                    )
                                    """
                                )
                                try:
                                    tgt_cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS files_history_unique_dirname_transfer ON files_history(dirpath, name, transfer_id)")
                                except Exception:
                                    pass
                                try:
                                    tgt_cur.execute("CREATE INDEX IF NOT EXISTS idx_history_dirpath_name ON files_history(dirpath, name)")
                                except Exception:
                                    pass
                                tgt_conn.commit()
                            except Exception:
                                pass

                            try:
                                # insert the existing row into files_history but override transfer_id with current transfer_id
                                tgt_cur.execute(
                                    "INSERT OR IGNORE INTO files_history(dev_major, dev_minor, ino, dirpath, name, suffix, mode, uid, gid, size, atime, mtime, ctime, is_dir, is_file, is_symlink, link_target, transfer_id, content_hash_id, scan_run_id, drive_serial_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        existing[0], existing[1], existing[2], existing[3], existing[4], existing[5], existing[6], existing[7], existing[8], existing[9], existing[10], existing[11], existing[12], existing[13], existing[14], existing[15], existing[16], transfer_id, existing[18], existing[19], existing[20]
                                    ),
                                )
                                tgt_conn.commit()
                            except Exception:
                                pass

                        # Now insert/replace the new row into files for the target run
                        try:
                            tgt_cur.execute(
                                "INSERT OR REPLACE INTO files(dev_major, dev_minor, ino, dirpath, name, suffix, mode, uid, gid, size, atime, mtime, ctime, is_dir, is_file, is_symlink, link_target, transfer_id, content_hash_id, scan_run_id, drive_serial_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (dev_major, dev_minor, ino, dirpath_s, name_s, suffix, mode, uid, gid, size, atime_v, mtime_v, ctime_v, is_dir_v, is_file_v, is_symlink_v, link_target, transfer_id, tgt_ch_id, self.run_tgt, tgt_ds_id),
                            )
                            tgt_conn.commit()
                            transferred += 1
                        except Exception:
                            try:
                                # fallback without drive_serial_id if target schema lacks it
                                tgt_cur.execute(
                                    "INSERT OR REPLACE INTO files(dev_major, dev_minor, ino, dirpath, name, suffix, mode, uid, gid, size, atime, mtime, ctime, is_dir, is_file, is_symlink, link_target, transfer_id, content_hash_id, scan_run_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (
                                        dev_major, dev_minor, ino, dirpath_s, name_s, suffix, mode, uid, gid, size, atime_v, mtime_v, ctime_v, is_dir_v, is_file_v, is_symlink_v, link_target, transfer_id, tgt_ch_id, self.run_tgt,
                                    ),
                                )
                                tgt_conn.commit()
                                transferred += 1
                            except Exception:
                                pass
                except Exception:
                    pass

                # emit progress
                try:
                    pct = int((idx + 1) * 100 / max(1, total))
                    self.progress.emit(pct)
                except Exception:
                    pass

            # if cancelled, mark finished and emit cancelled
            if getattr(self, '_cancelled', False):
                try:
                    finished_at = time.time()
                    tgt_cur.execute("UPDATE transfers SET finished_at = ? WHERE id = ?", (finished_at, transfer_id))
                    tgt_conn.commit()
                except Exception:
                    pass
                # emit cancelled with how many transferred so far
                self.cancelled.emit(transferred)
                return

            # mark transfer finished
            try:
                finished_at = time.time()
                tgt_cur.execute("UPDATE transfers SET finished_at = ? WHERE id = ?", (finished_at, transfer_id))
                tgt_conn.commit()
            except Exception:
                pass

            self.finished.emit(transfer_id, transferred)

        finally:
            try:
                src_conn.close()
            except Exception:
                pass
            try:
                tgt_conn.close()
            except Exception:
                pass


class CompareWorker(QtCore.QThread):
    """Background worker to compare two scan_runs without blocking the UI.

    Emits:
      progress(int) - percentage complete
      finished(object) - result dict when done
      error(str) - on fatal error
    """
    progress = Signal(int)
    finished = Signal(object)
    error = Signal(str)

    def __init__(self, db1, db2, run1, run2):
        super().__init__()
        self.db1 = db1
        self.db2 = db2
        self.run1 = run1
        self.run2 = run2

    def run(self):
        try:
            c1 = sqlite3.connect(self.db1)
            c2 = sqlite3.connect(self.db2)
        except Exception as e:
            self.error.emit(f"Failed to open DBs: {e}")
            return

        try:
            cur1 = c1.cursor()
            cur2 = c2.cursor()

            # totals
            try:
                cur1.execute("SELECT COUNT(*) FROM files WHERE scan_run_id = ?", (self.run1,))
                total1 = cur1.fetchone()[0] or 0
            except Exception:
                total1 = 0
            try:
                cur2.execute("SELECT COUNT(*) FROM files WHERE scan_run_id = ?", (self.run2,))
                total2 = cur2.fetchone()[0] or 0
            except Exception:
                total2 = 0
            # quick progress
            try:
                self.progress.emit(10)
            except Exception:
                pass

            # create temp table in DB1 to hold hashes from DB2
            try:
                c1.execute("CREATE TEMP TABLE IF NOT EXISTS temp_hashes(hash TEXT PRIMARY KEY)")
                c1.commit()
            except Exception:
                # not fatal; continue
                pass
            try:
                self.progress.emit(25)
            except Exception:
                pass

            # gather distinct hashes from DB2 for run2
            rows2 = []
            try:
                cur2.execute(
                    "SELECT DISTINCT ch.content_hash FROM files f JOIN content_hashes ch ON f.content_hash_id = ch.id WHERE f.scan_run_id = ? AND ch.content_hash IS NOT NULL",
                    (self.run2,)
                )
                rows2 = [r[0] for r in cur2.fetchall() if r and r[0]]
            except Exception:
                rows2 = []

            # insert into temp table in DB1
            try:
                if rows2:
                    insert_rows = [(h,) for h in rows2]
                    c1.executemany("INSERT OR IGNORE INTO temp_hashes(hash) VALUES (?)", insert_rows)
                    c1.commit()
            except Exception:
                pass
            try:
                self.progress.emit(60)
            except Exception:
                pass

            # Now query DB1 for files in run1 whose content_hash is NOT present in temp_hashes
            missing_rows = []
            try:
                cur1.execute(
                    "SELECT ch.content_hash, f.dirpath, f.name FROM files f JOIN content_hashes ch ON f.content_hash_id = ch.id WHERE f.scan_run_id = ? AND ch.content_hash IS NOT NULL AND ch.content_hash NOT IN (SELECT hash FROM temp_hashes)",
                    (self.run1,)
                )
                missing_rows = cur1.fetchall()
            except Exception:
                missing_rows = []

            # compute counts
            try:
                cur1.execute(
                    "SELECT DISTINCT ch.content_hash FROM files f JOIN content_hashes ch ON f.content_hash_id = ch.id WHERE f.scan_run_id = ? AND ch.content_hash IS NOT NULL",
                    (self.run1,)
                )
                rows1_hashes = [r[0] for r in cur1.fetchall() if r and r[0]]
            except Exception:
                rows1_hashes = []
            set1 = set(rows1_hashes)
            set2 = set(rows2)
            common_hashes = set1 & set2
            try:
                cur1.execute("SELECT COUNT(*) FROM files f WHERE f.scan_run_id = ? AND f.content_hash_id IS NOT NULL", (self.run1,))
                files_with_hash_run1 = cur1.fetchone()[0] or 0
            except Exception:
                files_with_hash_run1 = 0
            diff_hash_files = len(missing_rows)
            same_hash_files = files_with_hash_run1 - diff_hash_files

            # cleanup temp table
            try:
                c1.execute("DROP TABLE IF EXISTS temp_hashes")
                c1.commit()
            except Exception:
                pass

            # final progress
            try:
                self.progress.emit(100)
            except Exception:
                pass

            result = {
                'total1': total1,
                'total2': total2,
                'set1_count': len(set1),
                'set2_count': len(set2),
                'common_hashes': len(common_hashes),
                'files_with_hash_run1': files_with_hash_run1,
                'same_hash_files': same_hash_files,
                'diff_hash_files': diff_hash_files,
                'missing_rows': missing_rows,
            }
            self.finished.emit(result)

        except Exception as e:
            self.error.emit(f"Error comparing runs: {e}")
        finally:
            try:
                c1.close()
            except Exception:
                pass
            try:
                c2.close()
            except Exception:
                pass



class FSCompareGUI(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("fileSage")
        self.resize(900, 600)

        # central widget and layout
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)

        # timer to hit python event loop periodically
        self.timer = QtCore.QTimer()
        self.timer.timeout.connect(lambda: None)
        self.timer.start(200)

        # Help / About menu
        try:
            menubar = self.menuBar()
            help_menu = menubar.addMenu("Help")
            about_action = QtWidgets.QAction("About", self)
            about_action.triggered.connect(self.show_about)
            help_menu.addAction(about_action)
        except Exception:
            # non-fatal if menu operations fail in some Qt bindings
            pass

        # try to load BACKGROUND.JPG from the same directory as this script
        self.bg_pixmap = None
        #try:
        #    script_dir = os.path.dirname(os.path.abspath(__file__))
        #    #bg_path = os.path.join(script_dir, 'images1/BACKGROUND.JPG')
        #    bg_path = resource_path(os.path.join("images1", "BACKGROUND.JPG"))
        #    if os.path.exists(bg_path):
        #        pm = QtGui.QPixmap(bg_path)
        #        if not pm.isNull():
        #            self.bg_pixmap = pm
        #            central.setAutoFillBackground(True)
        #except Exception:
        #    # non-fatal: continue without background
        #    self.bg_pixmap = None

        # attempt to load a random JPG from an `images1` subdirectory next to this script
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            images1_dir = os.path.join(script_dir, 'images1')
            if os.path.isdir(images1_dir):
                candidates = [os.path.join(images1_dir, fn) for fn in os.listdir(images1_dir)
                              if fn.lower().endswith('.jpg') or fn.lower().endswith('.jpeg')]
                if candidates:
                    chosen = random.choice(candidates)
                    pm = QtGui.QPixmap(chosen)
                    if not pm.isNull():
                        self.bg_pixmap = pm
                        self.centralWidget().setAutoFillBackground(True)
        except Exception:
            pass

        # top: DB selectors
        db_select_layout = QtWidgets.QHBoxLayout()

        # DB1
        left_group = QtWidgets.QGroupBox("  Source")
        left_group.setObjectName('db1Group')
        left_layout = QtWidgets.QVBoxLayout(left_group)
        try:
            left_layout.setContentsMargins(8, 20, 8, 8)
            left_layout.setSpacing(6)
            left_group.setStyleSheet(
                "QGroupBox#db1Group { background-color: rgba(255,255,255,230); border: 1px solid rgba(0,0,0,40); border-radius: 6px; padding: 10px 6px 6px 6px; font-size: 11pt; }"
                "QGroupBox#db1Group QLabel { font-weight: bold; }"
            )
        except Exception:
            pass
        db1_h = QtWidgets.QHBoxLayout()
        self.db1_path = QtWidgets.QLineEdit()
        self.db1_browse = QtWidgets.QPushButton("Browse...")
        db1_h.addWidget(self.db1_path)
        db1_h.addWidget(self.db1_browse)
        left_layout.addLayout(db1_h)
        self.db1_runs = QtWidgets.QComboBox()
        left_layout.addWidget(QtWidgets.QLabel("Select scan_run:"))
        left_layout.addWidget(self.db1_runs)
        # allow user to override/choose the resolved source root (folder)
        left_layout.addWidget(QtWidgets.QLabel("Resolved source root (override):"))
        db1_root_h = QtWidgets.QHBoxLayout()
        self.db1_root_override = QtWidgets.QLineEdit()
        self.db1_root_browse = QtWidgets.QPushButton("Browse...")
        db1_root_h.addWidget(self.db1_root_override)
        db1_root_h.addWidget(self.db1_root_browse)
        left_layout.addLayout(db1_root_h)

        # DB2
        right_group = QtWidgets.QGroupBox("  Target")
        right_group.setObjectName('db2Group')
        right_layout = QtWidgets.QVBoxLayout(right_group)
        try:
            right_layout.setContentsMargins(8, 20, 8, 8)
            right_layout.setSpacing(6)
            right_group.setStyleSheet(
                "QGroupBox#db2Group { background-color: rgba(255,255,255,230); border: 1px solid rgba(0,0,0,40); border-radius: 6px; padding: 10px 6px 6px 6px; font-size: 11pt; }"
                "QGroupBox#db2Group QLabel { font-weight: bold; }"
            )
        except Exception:
            pass
        db2_h = QtWidgets.QHBoxLayout()
        self.db2_path = QtWidgets.QLineEdit()
        self.db2_browse = QtWidgets.QPushButton("Browse...")
        db2_h.addWidget(self.db2_path)
        db2_h.addWidget(self.db2_browse)
        right_layout.addLayout(db2_h)
        self.db2_runs = QtWidgets.QComboBox()
        right_layout.addWidget(QtWidgets.QLabel("Select scan_run:"))
        right_layout.addWidget(self.db2_runs)
        # allow user to override/choose the resolved target root (folder)
        right_layout.addWidget(QtWidgets.QLabel("Resolved target root (override):"))
        db2_root_h = QtWidgets.QHBoxLayout()
        self.db2_root_override = QtWidgets.QLineEdit()
        self.db2_root_browse = QtWidgets.QPushButton("Browse...")
        db2_root_h.addWidget(self.db2_root_override)
        db2_root_h.addWidget(self.db2_root_browse)
        right_layout.addLayout(db2_root_h)

        db_select_layout.addWidget(left_group)
        db_select_layout.addWidget(right_group)
        layout.addLayout(db_select_layout)

        # actions
        actions = QtWidgets.QHBoxLayout()
        self.compare_btn = QtWidgets.QPushButton("Compare")
        # small spacer so spinner can appear between the buttons
        self.show_missing_btn = QtWidgets.QPushButton("Show files only in DB1")
        self.show_missing_btn.setEnabled(False)
        self.transfer_btn = QtWidgets.QPushButton("Selected files to Target==>")
        self.transfer_btn.setEnabled(False)
        self.cancel_btn = QtWidgets.QPushButton("Cancel")
        self.cancel_btn.setVisible(False)
        self.cancel_btn.setEnabled(False)
        # indeterminate progress indicator (appears as a busy bar)
        self.spinner = QtWidgets.QProgressBar()
        self.spinner.setRange(0, 0)  # makes it an indeterminate / busy indicator
        self.spinner.setFixedWidth(140)
        self.spinner.setVisible(False)
        actions.addWidget(self.compare_btn)
        actions.addSpacing(12)
        actions.addWidget(self.spinner)
        actions.addSpacing(12)
        actions.addWidget(self.show_missing_btn)
        actions.addSpacing(12)
        actions.addWidget(self.transfer_btn)
        actions.addSpacing(6)
        actions.addWidget(self.cancel_btn)
        actions.addStretch()
        layout.addLayout(actions)

        # results summary
        self.results_label = QtWidgets.QLabel("")
        self.results_label.setWordWrap(True)
        layout.addWidget(self.results_label)

        # tree view for missing files
        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderLabels(["Name"])
        layout.addWidget(self.tree, 1)

        # connections
        self.db1_browse.clicked.connect(lambda: self.choose_db(1))
        self.db2_browse.clicked.connect(lambda: self.choose_db(2))
        try:
            self.db1_root_browse.clicked.connect(lambda: self._choose_db1_root_override())
        except Exception:
            pass
        try:
            self.db2_root_browse.clicked.connect(lambda: self._choose_db2_root_override())
        except Exception:
            pass
        self.db1_path.editingFinished.connect(lambda: self.load_runs_for_field(1))
        self.db2_path.editingFinished.connect(lambda: self.load_runs_for_field(2))
        # disable show_missing when run/db selection changes
        self.db1_runs.currentIndexChanged.connect(lambda _: self.on_selection_changed())
        self.db2_runs.currentIndexChanged.connect(lambda _: self.on_selection_changed())
        self.db1_path.textChanged.connect(lambda _: self.on_selection_changed())
        self.db2_path.textChanged.connect(lambda _: self.on_selection_changed())
        self.compare_btn.clicked.connect(self.on_compare)
        self.show_missing_btn.clicked.connect(self.on_show_missing)
        self.transfer_btn.clicked.connect(self.on_transfer_selected)
        self.cancel_btn.clicked.connect(self.on_cancel_transfer)
        # update transfer button enable state when any item's check state changes
        try:
            self.tree.itemChanged.connect(lambda *_: self._update_transfer_button_state())
        except Exception:
            pass

        # internal
        self.db1_conn = None
        self.db2_conn = None
        self.last_missing_rows = []
        # remember last-loaded DB paths to avoid reloading runs on spurious editingFinished events
        self._last_loaded_paths = {1: None, 2: None}

        # load saved UI state (paths and selected run ids) if available
        try:
            self._load_saved_state()
        except Exception:
            # non-fatal
            pass

        # Organize UI into tabbed pages: TRANSFER (current UI), SCAN, HARDLINK
        try:
            # keep a reference to the transfer page (the current central widget)
            self.transfer_page = central
            try:
                # style transfer page children: translucent groupboxes and bolder labels
                self.transfer_page.setObjectName('transferPanel')
                self.transfer_page.setStyleSheet(
                    "QWidget#transferPanel QGroupBox { background-color: rgba(255,255,255,220); border: 1px solid rgba(0,0,0,40); border-radius: 6px; padding: 6px; }"
                    "QWidget#transferPanel QLabel { font-weight: bold; }"
                )
            except Exception:
                pass
            self.tabs = QtWidgets.QTabWidget()
            self.tabs.addTab(self.transfer_page, "TRANSFER")

            # SCAN page: controls to run fscan.py and a live output pane
            scan_page = QtWidgets.QWidget()
            scan_layout = QtWidgets.QHBoxLayout(scan_page)

            # Left: controls
            controls = QtWidgets.QGroupBox("Scan Options")
            controls.setObjectName('scanControls')
            controls_layout = QtWidgets.QFormLayout(controls)
            # provide extra top margin so enlarged title doesn't overlap the controls
            try:
                controls_layout.setContentsMargins(8, 20, 8, 8)
                controls_layout.setSpacing(6)
            except Exception:
                pass
            # style: slightly translucent light background and bold labels
            try:
                controls.setStyleSheet(
                    "QGroupBox#scanControls { background-color: rgba(255,255,255,230); border: 1px solid rgba(0,0,0,40); border-radius: 6px; padding: 10px 6px 6px 6px; font-size: 11pt; }"
                    "QGroupBox#scanControls QLabel { font-weight: bold; }"
                )
            except Exception:
                pass

            # Database selector
            self.scan_db_path = QtWidgets.QLineEdit()
            scan_db_browse = QtWidgets.QPushButton("Browse...")
            db_h = QtWidgets.QHBoxLayout()
            db_h.addWidget(self.scan_db_path)
            db_h.addWidget(scan_db_browse)
            controls_layout.addRow("Database:", db_h)

            # Root selector
            self.scan_root = QtWidgets.QLineEdit()
            scan_root_browse = QtWidgets.QPushButton("Browse...")
            root_h = QtWidgets.QHBoxLayout()
            root_h.addWidget(self.scan_root)
            root_h.addWidget(scan_root_browse)
            controls_layout.addRow("Root:", root_h)

            # checkboxes and options
            self.scan_silent = QtWidgets.QCheckBox("Silent (no progress)")
            self.scan_hash = QtWidgets.QCheckBox("Compute content hash (-H)")
            # Option to skip resume checks when starting a new scan
            self.scan_skip_resume = QtWidgets.QCheckBox("Skip resume checks")
            controls_layout.addRow(self.scan_silent)
            controls_layout.addRow(self.scan_hash)
            controls_layout.addRow(self.scan_skip_resume)

            # name above comment
            self.scan_name = QtWidgets.QLineEdit()
            controls_layout.addRow("Name:", self.scan_name)
            # comment: multi-line text box (approx 3 lines)
            self.scan_comment = QtWidgets.QPlainTextEdit()
            try:
                fm = self.scan_comment.fontMetrics()
                h = fm.lineSpacing() * 3 + 8
                self.scan_comment.setMinimumHeight(h)
                self.scan_comment.setMaximumHeight(h * 2)
            except Exception:
                pass
            controls_layout.addRow("Comment:", self.scan_comment)

            # (resume UI removed) If you want resume support, use CLI --resume or add it later

            # Run / Cancel buttons
            btn_h = QtWidgets.QHBoxLayout()
            self.scan_run_btn = QtWidgets.QPushButton("RUN SCAN")
            self.scan_cancel_btn = QtWidgets.QPushButton("Cancel")
            self.scan_cancel_btn.setEnabled(False)
            btn_h.addWidget(self.scan_run_btn)
            btn_h.addWidget(self.scan_cancel_btn)
            controls_layout.addRow(btn_h)

            controls.setLayout(controls_layout)

            # Right: output pane
            output_group = QtWidgets.QGroupBox("Scan Output")
            # style the Scan Output title: larger, bold and white for visibility over backgrounds
            try:
                output_group.setObjectName('scanOutputGroup')
                # Make the title prominent and give the outer frame (the group box) a semi-opaque background
                # while keeping the inner QPlainTextEdit transparent so the frame itself appears as the picture frame.
                # Increase margin-top so the title does not overlap the inner text widget.
                output_group.setStyleSheet("""
QGroupBox#scanOutputGroup {
    font-size: 12pt;
    font-weight: bold;
    color: white;
    background-color: rgba(0,0,0,160); /* semi-opaque outer frame */
    border: 2px solid rgba(255,255,255,80);
    border-radius: 8px;
    padding: 6px;
    margin-top: 28px;
}
QGroupBox#scanOutputGroup QPlainTextEdit {
    background-color: transparent; /* let the group box show through as the frame */
    color: white;
    border: none;
    padding: 6px;
}
""")
            except Exception:
                pass
            output_layout = QtWidgets.QVBoxLayout(output_group)
            # add some extra top margin inside the layout so content sits below the title
            try:
                output_layout.setContentsMargins(6, 28, 6, 6)
            except Exception:
                pass
            self.scan_output = QtWidgets.QPlainTextEdit()
            self.scan_output.setReadOnly(True)
            output_layout.addWidget(self.scan_output)
            output_group.setLayout(output_layout)

            # Place controls and output side-by-side
            scan_layout.addWidget(controls, 0)
            scan_layout.addWidget(output_group, 1)

            # choose a random image from images2 for scan page background
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                images2_dir = os.path.join(script_dir, 'images2')
                pm2 = None
                if os.path.isdir(images2_dir):
                    candidates2 = [os.path.join(images2_dir, fn) for fn in os.listdir(images2_dir)
                                   if fn.lower().endswith(('.jpg', '.jpeg', '.png'))]
                    if candidates2:
                        chosen2 = random.choice(candidates2)
                        pm_temp = QtGui.QPixmap(chosen2)
                        if not pm_temp.isNull():
                            pm2 = pm_temp
                if pm2:
                    self.tab_bg_pixmaps = getattr(self, 'tab_bg_pixmaps', {})
                    self.tab_bg_pixmaps['SCAN'] = pm2
                    scan_page.setAutoFillBackground(True)
            except Exception:
                pass
            self.tabs.addTab(scan_page, "SCAN")

            # RESUME SCAN page: select a DB and an unfinished scan_run to resume
            resume_page = QtWidgets.QWidget()
            resume_layout = QtWidgets.QHBoxLayout(resume_page)

            resume_controls = QtWidgets.QGroupBox("Resume Scan")
            resume_controls.setObjectName('resumeControls')



            resume_layout_form = QtWidgets.QFormLayout(resume_controls)
            try:
                resume_layout_form.setContentsMargins(8, 20, 8, 8)
                resume_layout_form.setSpacing(6)
                resume_controls.setStyleSheet(
                    "QGroupBox#resumeControls { background-color: rgba(255,255,255,230); border: 1px solid rgba(0,0,0,40); border-radius: 6px; padding: 10px 6px 6px 6px; font-size: 11pt; }"
                    "QGroupBox#resumeControls QLabel { font-weight: bold; }"
                )
            except Exception:
                pass

            # Database selector for resume
            self.resume_db_path = QtWidgets.QLineEdit()
            resume_db_browse = QtWidgets.QPushButton("Browse...")
            rh = QtWidgets.QHBoxLayout()
            rh.addWidget(self.resume_db_path)
            rh.addWidget(resume_db_browse)
            resume_layout_form.addRow("Database:", rh)

            # scan_run selector populated with unfinished runs
            self.resume_runs = QtWidgets.QComboBox()
            resume_layout_form.addRow("Select unfinished run:", self.resume_runs)

            # run info box: show name, comment, command line and logs for selected run
            self.resume_info = QtWidgets.QPlainTextEdit()
            self.resume_info.setReadOnly(True)
            try:
                fm = self.resume_info.fontMetrics()
                h = fm.lineSpacing() * 6 + 8
                self.resume_info.setMinimumHeight(h)
            except Exception:
                pass
            resume_layout_form.addRow("Run info:", self.resume_info)

            # resume / cancel buttons
            btn_h2 = QtWidgets.QHBoxLayout()
            self.resume_run_btn = QtWidgets.QPushButton("RESUME")
            self.resume_cancel_btn = QtWidgets.QPushButton("Cancel")
            self.resume_cancel_btn.setEnabled(False)
            btn_h2.addWidget(self.resume_run_btn)
            btn_h2.addWidget(self.resume_cancel_btn)
            resume_layout_form.addRow(btn_h2)

            resume_controls.setLayout(resume_layout_form)

            # Right: output pane for resume
            resume_output_group = QtWidgets.QGroupBox("Resume Output")
            resume_output_layout = QtWidgets.QVBoxLayout(resume_output_group)
            self.resume_output = QtWidgets.QPlainTextEdit()
            self.resume_output.setReadOnly(True)
            resume_output_layout.addWidget(self.resume_output)
            resume_output_group.setLayout(resume_output_layout)

            try:
                resume_output_group.setObjectName('resumeOutputGroup')
                resume_output_group.setStyleSheet("""
QGroupBox#resumeOutputGroup {
    font-size: 12pt;
    font-weight: bold;
    color: white;
    background-color: rgba(0,0,0,80); /* semi-opaque outer frame */
    border: 0px solid rgba(255,255,255,80);
    border-radius: 8px;
    padding: 6px;
    margin-top: 28px;
}
""")
            except Exception:
                pass

            resume_layout.addWidget(resume_controls, 0)
            resume_layout.addWidget(resume_output_group, 1)

            # choose a random image from images4 for resume page background
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                images4_dir = os.path.join(script_dir, 'images4')
                pm4 = None
                if os.path.isdir(images4_dir):
                    candidates4 = [os.path.join(images4_dir, fn) for fn in os.listdir(images4_dir)
                                   if fn.lower().endswith(('.jpg', '.jpeg', '.png'))]
                    if candidates4:
                        chosen4 = random.choice(candidates4)
                        pm_temp4 = QtGui.QPixmap(chosen4)
                        if not pm_temp4.isNull():
                            pm4 = pm_temp4
                if pm4:
                    self.tab_bg_pixmaps = getattr(self, 'tab_bg_pixmaps', {})
                    # tab text is "RESUME SCAN" so use that as the key so _on_tab_changed finds it
                    self.tab_bg_pixmaps['RESUME SCAN'] = pm4
                    resume_page.setAutoFillBackground(True)
            except Exception:
                pass
            self.tabs.addTab(resume_page, "RESUME SCAN")

            # wire up resume controls
            try:
                resume_db_browse.clicked.connect(lambda: self._choose_resume_db())
                self.resume_db_path.editingFinished.connect(lambda: self._load_resume_runs())
                # update run info when selection changes
                try:
                    self.resume_runs.currentIndexChanged.connect(lambda _: self._on_resume_selection_changed())
                except Exception:
                    pass
                self.resume_run_btn.clicked.connect(lambda: self._on_run_resume())
                self.resume_cancel_btn.clicked.connect(lambda: self._on_cancel_resume())
            except Exception:
                pass

            # wire up scan controls
            try:
                scan_db_browse.clicked.connect(lambda: self._choose_scan_db())
                scan_root_browse.clicked.connect(lambda: self._choose_scan_root())
                self.scan_run_btn.clicked.connect(lambda: self._on_run_scan())
                self.scan_cancel_btn.clicked.connect(lambda: self._on_cancel_scan())
            except Exception:
                pass

            # HARDLINK page: duplicate of the RESUME SCAN UI but for hardlink operations
            hardlink_page = QtWidgets.QWidget()
            hardlink_page_layout = QtWidgets.QHBoxLayout(hardlink_page)

            hardlink_controls = QtWidgets.QGroupBox("Hardlink")
            hardlink_controls.setObjectName('hardlinkControls')
            hardlink_form = QtWidgets.QFormLayout(hardlink_controls)
            try:
                hardlink_form.setContentsMargins(8, 20, 8, 8)
                hardlink_form.setSpacing(6)
                hardlink_controls.setStyleSheet(
                    "QGroupBox#hardlinkControls { background-color: rgba(255,255,255,230); border: 1px solid rgba(0,0,0,40); border-radius: 6px; padding: 10px 6px 6px 6px; font-size: 11pt; }"
                    "QGroupBox#hardlinkControls QLabel { font-weight: bold; }"
                )
            except Exception:
                pass

            # Database selector for hardlink
            self.hardlink_db_path = QtWidgets.QLineEdit()
            hardlink_db_browse = QtWidgets.QPushButton("Browse...")
            hh = QtWidgets.QHBoxLayout()
            hh.addWidget(self.hardlink_db_path)
            hh.addWidget(hardlink_db_browse)
            hardlink_form.addRow("Database:", hh)

            # scan_run selector populated with unfinished runs
            self.hardlink_runs = QtWidgets.QComboBox()
            hardlink_form.addRow("Select scan:", self.hardlink_runs)

            # run info box: show name, comment, command line and logs for selected run
            self.hardlink_info = QtWidgets.QPlainTextEdit()
            self.hardlink_info.setReadOnly(True)
            try:
                fm = self.hardlink_info.fontMetrics()
                h = fm.lineSpacing() * 6 + 8
                self.hardlink_info.setMinimumHeight(h)
            except Exception:
                pass
            hardlink_form.addRow("Run info:", self.hardlink_info)

            # Minimum size filter: checkbox + numeric field (default checked, 4097)
            try:
                self.hardlink_min_size_chk = QtWidgets.QCheckBox("Minimum size")
                self.hardlink_min_size_chk.setChecked(True)
                self.hardlink_min_size = QtWidgets.QSpinBox()
                # allow large values but stay within practical int range
                try:
                    self.hardlink_min_size.setRange(0, 2**31 - 1)
                except Exception:
                    try:
                        self.hardlink_min_size.setRange(0, 10**9)
                    except Exception:
                        pass
                try:
                    self.hardlink_min_size.setValue(4097)
                except Exception:
                    pass
                hmin_h = QtWidgets.QHBoxLayout()
                hmin_h.addWidget(self.hardlink_min_size_chk)
                hmin_h.addWidget(self.hardlink_min_size)
                hardlink_form.addRow(hmin_h)
            except Exception:
                pass

            # hardlink / cancel buttons
            btn_h3 = QtWidgets.QHBoxLayout()
            self.hardlink_run_btn = QtWidgets.QPushButton("HARDLINK")
            self.hardlink_dryrun_btn = QtWidgets.QPushButton("DRY RUN")
            self.hardlink_cancel_btn = QtWidgets.QPushButton("Cancel")
            self.hardlink_cancel_btn.setEnabled(False)
            btn_h3.addWidget(self.hardlink_run_btn)
            btn_h3.addWidget(self.hardlink_dryrun_btn)
            btn_h3.addWidget(self.hardlink_cancel_btn)
            hardlink_form.addRow(btn_h3)

            hardlink_controls.setLayout(hardlink_form)

            # Right: output pane for hardlink
            hardlink_output_group = QtWidgets.QGroupBox("Hardlink Output")
            hardlink_output_layout = QtWidgets.QVBoxLayout(hardlink_output_group)
            self.hardlink_output = QtWidgets.QPlainTextEdit()
            self.hardlink_output.setReadOnly(True)
            hardlink_output_layout.addWidget(self.hardlink_output)
            hardlink_output_group.setLayout(hardlink_output_layout)

            try:
                hardlink_output_group.setObjectName('hardlinkOutputGroup')
                hardlink_output_group.setStyleSheet("""
QGroupBox#hardlinkOutputGroup {
    font-size: 12pt;
    font-weight: bold;
    color: white;
    background-color: rgba(0,0,0,80); /* semi-opaque outer frame */
    border: 0px solid rgba(255,255,255,80);
    border-radius: 8px;
    padding: 3px;
    margin-top: 28px;
}
""")
            except Exception:
                pass            

            hardlink_page_layout.addWidget(hardlink_controls, 0)
            hardlink_page_layout.addWidget(hardlink_output_group, 1)

            # choose a random image from images3 for hardlink page background
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                images3_dir = os.path.join(script_dir, 'images3')
                pm3 = None
                if os.path.isdir(images3_dir):
                    candidates3 = [os.path.join(images3_dir, fn) for fn in os.listdir(images3_dir)
                                   if fn.lower().endswith(('.jpg', '.jpeg', '.png'))]
                    if candidates3:
                        chosen3 = random.choice(candidates3)
                        pm_temp3 = QtGui.QPixmap(chosen3)
                        if not pm_temp3.isNull():
                            pm3 = pm_temp3
                if pm3:
                    self.tab_bg_pixmaps = getattr(self, 'tab_bg_pixmaps', {})
                    self.tab_bg_pixmaps['HARDLINK'] = pm3
                    hardlink_page.setAutoFillBackground(True)
            except Exception:
                pass

            self.tabs.addTab(hardlink_page, "HARDLINK")

            # wire up hardlink controls
            try:
                hardlink_db_browse.clicked.connect(lambda: self._choose_hardlink_db())
                self.hardlink_db_path.editingFinished.connect(lambda: self._load_hardlink_runs())
                try:
                    self.hardlink_runs.currentIndexChanged.connect(lambda _: self._on_hardlink_selection_changed())
                except Exception:
                    pass
                self.hardlink_run_btn.clicked.connect(lambda: self._on_run_hardlink())
                try:
                    self.hardlink_dryrun_btn.clicked.connect(lambda: self._on_hardlink_dryrun())
                except Exception:
                    pass
                self.hardlink_cancel_btn.clicked.connect(lambda: self._on_cancel_hardlink())
            except Exception:
                pass

            # Replace the central widget with a wrapper that holds the tabs
            wrapper = QtWidgets.QWidget()
            wrapper_layout = QtWidgets.QVBoxLayout(wrapper)
            wrapper_layout.setContentsMargins(0, 0, 0, 0)
            wrapper_layout.addWidget(self.tabs)
            self.setCentralWidget(wrapper)
            # refresh background when tabs change so backgrounds are rendered
            try:
                self.tabs.currentChanged.connect(self._on_tab_changed)
                # schedule a refresh after the event loop starts so widget sizes are valid
                try:
                    QtCore.QTimer.singleShot(0, lambda: self._on_tab_changed(self.tabs.currentIndex()))
                except Exception:
                    # best-effort immediate call as fallback
                    try:
                        self._on_tab_changed(self.tabs.currentIndex())
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            # If anything goes wrong, leave the original central widget in place
            try:
                self.setCentralWidget(central)
            except Exception:
                pass

    def choose_db(self, which):
        # Start file browser in last-used directory for this DB field (if available),
        # otherwise default to user's home.
        if which == 1:
            start_dir = getattr(self, '_last_db1_dir', None) or os.path.expanduser("~")
        else:
            start_dir = getattr(self, '_last_db2_dir', None) or os.path.expanduser("~")
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Select SQLite DB", start_dir, "SQLite DB (*.db *.sqlite *.sqlite3);;All Files (*)")
        if not path:
            return
        if which == 1:
            self.db1_path.setText(path)
            # remember directory for next time
            try:
                self._last_db1_dir = os.path.dirname(path) or os.path.expanduser("~")
            except Exception:
                self._last_db1_dir = os.path.expanduser("~")
            self.load_runs_for_field(1)
        else:
            self.db2_path.setText(path)
            try:
                self._last_db2_dir = os.path.dirname(path) or os.path.expanduser("~")
            except Exception:
                self._last_db2_dir = os.path.expanduser("~")
            self.load_runs_for_field(2)

    def open_conn(self, path):
        if not path:
            return None
        try:
            conn = sqlite3.connect(path)
            return conn
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "DB open error", f"Failed to open {path}: {e}")
            return None

    def load_runs_for_field(self, which):
        path = self.db1_path.text().strip() if which == 1 else self.db2_path.text().strip()
        # avoid reloading if the path hasn't changed since last successful load
        try:
            if path and path == self._last_loaded_paths.get(which):
                return
        except Exception:
            pass
        combo = self.db1_runs if which == 1 else self.db2_runs
        # close any previous connection
        if which == 1 and self.db1_conn:
            try:
                self.db1_conn.close()
            except Exception:
                pass
            self.db1_conn = None
        if which == 2 and self.db2_conn:
            try:
                self.db2_conn.close()
            except Exception:
                pass
            self.db2_conn = None

        combo.clear()
        if not path:
            return
        conn = self.open_conn(path)
        if not conn:
            return
        # fetch runs
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, started_at, root FROM scan_runs ORDER BY id")
            rows = cur.fetchall()
        except Exception as e:
            QtWidgets.QMessageBox.warning(self, "DB read error", f"Failed to read scan_runs from {path}: {e}")
            conn.close()
            return
        if not rows:
            combo.addItem("(no runs found)")
        else:
            for r in rows:
                rid, started_at, root = r
                try:
                    started_s = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(started_at)) if started_at else 'N/A'
                except Exception:
                    started_s = str(started_at)
                label = f"id={rid} root={root} started={started_s}"
                combo.addItem(label, rid)
        # keep conn
        if which == 1:
            self.db1_conn = conn
            self._last_loaded_paths[1] = path
        else:
            self.db2_conn = conn
            self._last_loaded_paths[2] = path
        # any change to loaded runs invalidates previous compare results
        try:
            self.on_selection_changed()
        except Exception:
            pass

    def get_selected_run_id(self, which):
        combo = self.db1_runs if which == 1 else self.db2_runs
        idx = combo.currentIndex()
        if idx < 0:
            return None
        data = combo.itemData(idx)
        if isinstance(data, int):
            return data
        # if no runs found item present return None
        try:
            # try to parse id from label as fallback
            text = combo.currentText()
            if text.startswith('id='):
                parts = text.split()
                first = parts[0]
                return int(first.split('=', 1)[1])
        except Exception:
            pass
        return None

    def on_selection_changed(self):
        """Called when database path or run selection changes: invalidate previous compare results."""
        try:
            self.show_missing_btn.setEnabled(False)
        except Exception:
            pass
        self.last_missing_rows = []
        try:
            self.tree.clear()
        except Exception:
            pass
        try:
            self.results_label.setText("")
        except Exception:
            pass
        # Clear any resolved-source override whenever the source DB or run selection changes.
        try:
            if getattr(self, 'db1_root_override', None) is not None:
                self.db1_root_override.setText("")
        except Exception:
            pass

    def _state_file_path(self):
        home = os.path.expanduser('~')
        dh = os.path.join(home, '.filesage')
        return dh, os.path.join(dh, 'fsave.state')

    def _load_saved_state(self):
        """Load saved DB paths and selected run ids from ~/.filesage/fsave.state if present."""
        dh, path = self._state_file_path()
        if not os.path.exists(path):
            # If there's no saved state, default both source and target DB fields
            # to ~/.filesage/fscan.db so users have a sensible starting point.
            try:
                default_db = os.path.join(os.path.expanduser('~'), '.filesage', 'fscan.db')
                self.db1_path.setText(default_db)
                self.db2_path.setText(default_db)
                # attempt to load runs for each field (will silently fail if DB missing)
                try:
                    self.load_runs_for_field(1)
                except Exception:
                    pass
                try:
                    self.load_runs_for_field(2)
                except Exception:
                    pass
            except Exception:
                pass
            return
        try:
            with open(path, 'r') as f:
                data = json.load(f)
        except Exception:
            return
        # expected keys: db1, db2, run1, run2
        db1 = data.get('db1')
        db2 = data.get('db2')
        run1 = data.get('run1')
        run2 = data.get('run2')
        if db1:
            try:
                self.db1_path.setText(db1)
                # this will load runs into the combo
                self.load_runs_for_field(1)
                if run1 is not None:
                    # try to select the run id in the combo
                    for i in range(self.db1_runs.count()):
                        if self.db1_runs.itemData(i) == run1:
                            self.db1_runs.setCurrentIndex(i)
                            break
                # Do NOT restore a previously-saved resolved source override here.
                # Clear any existing override so the user makes an explicit choice
                # for the current DB/run. This prevents stale overrides being used.
                try:
                    if getattr(self, 'db1_root_override', None) is not None:
                        self.db1_root_override.setText("")
                except Exception:
                    pass
            except Exception:
                pass
        if db2:
            try:
                self.db2_path.setText(db2)
                self.load_runs_for_field(2)
                if run2 is not None:
                    for i in range(self.db2_runs.count()):
                        if self.db2_runs.itemData(i) == run2:
                            self.db2_runs.setCurrentIndex(i)
                            break
            except Exception:
                pass

    def _save_state(self):
        """Persist currently selected DB paths and scan_run ids to ~/.filesage/fsave.state."""
        dh, path = self._state_file_path()
        try:
            os.makedirs(dh, exist_ok=True)
        except Exception:
            # if we can't create directory, bail silently
            return
        try:
            db1 = self.db1_path.text().strip() if self.db1_path is not None else None
            db2 = self.db2_path.text().strip() if self.db2_path is not None else None
            run1 = self.get_selected_run_id(1)
            run2 = self.get_selected_run_id(2)
            data = {'db1': db1, 'db2': db2, 'run1': run1, 'run2': run2}
            with open(path, 'w') as f:
                json.dump(data, f)
        except Exception:
            # swallow errors
            pass

    def closeEvent(self, event):
        # save state then accept close
        try:
            self._save_state()
        except Exception:
            pass

        # mark that we're shutting down so handlers can avoid showing modal dialogs
        try:
            self._shutting_down = True
        except Exception:
            pass

        # Attempt to gracefully stop any background workers before exit. This
        # helps avoid native crashes where a QThread may still emit signals
        # while Qt objects are being torn down.
        try:
            # Transfer worker
            tw = getattr(self, '_transfer_worker', None)
            if tw is not None:
                try:
                    # request cancellation and wait briefly for the thread to finish
                    try:
                        tw.cancel()
                    except Exception:
                        pass
                    # wait up to 5 seconds for the thread to exit
                    try:
                        tw.wait(5000)
                    except Exception:
                        pass
                except Exception:
                    pass

            # Compare worker
            cw = getattr(self, '_compare_worker', None)
            if cw is not None:
                try:
                    try:
                        # if the worker exposes a cancel/requestInterruption API, call it
                        if hasattr(cw, 'cancel'):
                            try:
                                cw.cancel()
                            except Exception:
                                pass
                        elif hasattr(cw, 'requestInterruption'):
                            try:
                                cw.requestInterruption()
                            except Exception:
                                pass
                    except Exception:
                        pass
                    try:
                        cw.wait(3000)
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass

        return super().closeEvent(event)

    def on_compare(self):
        # ensure both DBs and runs selected, then start background compare worker
        db1 = self.db1_path.text().strip()
        db2 = self.db2_path.text().strip()
        if not db1 or not db2:
            QtWidgets.QMessageBox.warning(self, "Missing DB", "Please select both database files before comparing.")
            return
        run1 = self.get_selected_run_id(1)
        run2 = self.get_selected_run_id(2)
        if run1 is None or run2 is None:
            QtWidgets.QMessageBox.warning(self, "Missing run", "Please select a scan_run from each database.")
            return

        # disable UI and clear previous results
        try:
            self.show_missing_btn.setEnabled(False)
        except Exception:
            pass
        self.last_missing_rows = []
        try:
            self.tree.clear()
        except Exception:
            pass
        try:
            self.results_label.setText("Comparing...")
        except Exception:
            pass

        # prepare spinner as determinate progress bar
        try:
            self._set_ui_enabled(False)
            self.spinner.setRange(0, 100)
            self.spinner.setValue(0)
            self.spinner.setVisible(True)
            QtWidgets.QApplication.processEvents()
        except Exception:
            pass

        # create worker and start compare in background
        try:
            self._compare_worker = CompareWorker(db1, db2, run1, run2)
            self._compare_worker.progress.connect(self._on_compare_progress)
            self._compare_worker.finished.connect(self._on_compare_finished)
            self._compare_worker.error.connect(self._on_compare_error)
            self._compare_worker.start()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Compare error", f"Failed to start compare: {e}")
            try:
                self.spinner.setVisible(False)
            except Exception:
                pass
            self._set_ui_enabled(True)
            return

    def _on_compare_progress(self, pct: int):
        try:
            if self.spinner.maximum() == 0:
                self.spinner.setRange(0, 100)
            self.spinner.setValue(int(pct))
            self.results_label.setText(f"Comparing... {pct}%")
            QtWidgets.QApplication.processEvents()
        except Exception:
            pass

    def _on_compare_error(self, msg: str):
        try:
            self.spinner.setVisible(False)
        except Exception:
            pass
        try:
            QtWidgets.QMessageBox.critical(self, "Compare error", msg)
        except Exception:
            pass
        try:
            self._set_ui_enabled(True)
        except Exception:
            pass

    def _on_compare_finished(self, result: object):
        try:
            # cleanup worker
            try:
                self._compare_worker.progress.disconnect(self._on_compare_progress)
                self._compare_worker.finished.disconnect(self._on_compare_finished)
                self._compare_worker.error.disconnect(self._on_compare_error)
            except Exception:
                pass
            self._compare_worker = None
        except Exception:
            pass

        try:
            self.spinner.setVisible(False)
        except Exception:
            pass
        try:
            # restore UI
            self._set_ui_enabled(True)
        except Exception:
            pass

        try:
            # unpack result dict (keys defined by CompareWorker)
            total1 = result.get('total1', 0)
            total2 = result.get('total2', 0)
            set1_count = result.get('set1_count', 0)
            set2_count = result.get('set2_count', 0)
            common_hashes = result.get('common_hashes', 0)
            files_with_hash_run1 = result.get('files_with_hash_run1', 0)
            same_hash_files = result.get('same_hash_files', 0)
            diff_hash_files = result.get('diff_hash_files', 0)
            missing_rows = result.get('missing_rows', [])

            # store last_missing_rows for tree display: include dirpath,name,hash
            try:
                self.last_missing_rows = [(d, n, h) for (h, d, n) in missing_rows]
            except Exception:
                self.last_missing_rows = []

            # update results label
            try:
                self.results_label.setText(
                    f"DB1 files={total1}, DB2 files={total2}.\n"
                    f"Unique hashes DB1={set1_count}, DB2={set2_count}, common_hashes={common_hashes}.\n"
                    f"Files in DB1 whose hash exists in DB2={same_hash_files}. Files in DB1 with hash missing from DB2={diff_hash_files}."
                )
            except Exception:
                pass

            try:
                self.show_missing_btn.setEnabled(len(self.last_missing_rows) > 0)
            except Exception:
                pass
        except Exception:
            pass

    def on_show_missing(self):
        rows = self.last_missing_rows
        self.tree.clear()
        if not rows:
            return

        # Build hierarchical tree from dirpath/name
        root_nodes = {}

        for dirpath, name, h in rows:
            # normalize and split
            dirpath_norm = os.path.normpath(dirpath)
            if dirpath_norm == os.path.sep:
                parts = [os.path.sep]
            else:
                # remove leading empty part if path starts with '/'
                parts = [p for p in dirpath_norm.split(os.sep) if p]

            parent = None
            path_acc = []
            node_map = root_nodes
            # create/lookup nodes for each component
            for comp in parts:
                path_acc.append(comp)
                key = "/".join(path_acc)
                if key not in node_map:
                    item = QtWidgets.QTreeWidgetItem([comp])
                    # make directory nodes checkable and tristate so checking a folder
                    # can affect children (UI convenience)
                    try:
                        item.setFlags(item.flags() | QtCore.Qt.ItemIsUserCheckable | QtCore.Qt.ItemIsTristate)
                        item.setCheckState(0, QtCore.Qt.Unchecked)
                    except Exception:
                        # fallback: if flags or checkstate fail, ignore
                        pass
                    if parent is None:
                        self.tree.addTopLevelItem(item)
                    else:
                        parent.addChild(item)
                    node_map[key] = item
                parent = node_map[key]
            # finally add the file as a child of parent (or top-level if no dir parts)
            # display only the file name (hide the hash in the tree)
            file_item = QtWidgets.QTreeWidgetItem([name])
            # make file items user-checkable
            try:
                file_item.setFlags(file_item.flags() | QtCore.Qt.ItemIsUserCheckable)
                file_item.setCheckState(0, QtCore.Qt.Unchecked)
            except Exception:
                pass
            if parent is None:
                self.tree.addTopLevelItem(file_item)
            else:
                parent.addChild(file_item)

        self.tree.expandAll()
        # enable transfer button only if at least one file checkbox is checked
        try:
            self._update_transfer_button_state()
        except Exception:
            pass

    def _update_transfer_button_state(self):
        """Enable transfer button only when tree populated and at least one file checked."""
        # require that the tree has items and at least one checked leaf exists
        has_items = self.tree.topLevelItemCount() > 0
        if not has_items:
            self.transfer_btn.setEnabled(False)
            return
        checked_found = False
        def check_item(item):
            nonlocal checked_found
            # if leaf and checked
            if item.childCount() == 0:
                try:
                    if item.checkState(0) == QtCore.Qt.Checked:
                        checked_found = True
                        return True
                except Exception:
                    pass
            else:
                for i in range(item.childCount()):
                    if check_item(item.child(i)):
                        return True
            return False
        for i in range(self.tree.topLevelItemCount()):
            if check_item(self.tree.topLevelItem(i)):
                break
        self.transfer_btn.setEnabled(checked_found)

    def _gather_checked_files(self):
        """Return list of (dirpath, name) tuples for checked leaf items in the tree.

        Reconstructs dirpath by walking parent nodes; handles root '/' specially.
        """
        results = []
        def rec(item, path_parts):
            text = item.text(0)
            # if this node is the special root marker
            if text == os.path.sep:
                new_parts = [os.path.sep]
            else:
                new_parts = path_parts + [text]
            if item.childCount() == 0:
                try:
                    if item.checkState(0) == QtCore.Qt.Checked:
                        # determine dirpath from new_parts excluding the filename
                        if len(path_parts) == 0:
                            # file at top-level (unlikely) => dirpath '/'
                            dirpath = os.path.sep
                        else:
                            # when the parent is root marker
                            if path_parts == [os.path.sep]:
                                dirpath = os.path.sep
                            else:
                                dirpath = os.path.sep + os.path.join(*path_parts)
                        results.append((dirpath, text))
                except Exception:
                    pass
            else:
                # iterate children
                for i in range(item.childCount()):
                    rec(item.child(i), new_parts if text != os.path.sep else [os.path.sep])
        for i in range(self.tree.topLevelItemCount()):
            rec(self.tree.topLevelItem(i), [])
        return results

    def on_transfer_selected(self):
        """Create transfer record in target DB and insert selected files into target files table.

        The button is enabled only when at least one file is checked.
        """
        db_src = self.db1_path.text().strip()
        db_tgt = self.db2_path.text().strip()
        if not db_src or not db_tgt:
            QtWidgets.QMessageBox.warning(self, "Missing DB", "Please select both database files before transferring.")
            return
        run_src = self.get_selected_run_id(1)
        run_tgt = self.get_selected_run_id(2)
        if run_src is None or run_tgt is None:
            QtWidgets.QMessageBox.warning(self, "Missing run", "Please select a scan_run from each database.")
            return

        checked = self._gather_checked_files()
        if not checked:
            QtWidgets.QMessageBox.information(self, "No files selected", "No checked files to transfer.")
            return

        # start background transfer worker
        try:
            self._set_ui_enabled(False)
            # prepare spinner as determinate progress bar
            self.spinner.setRange(0, 100)
            self.spinner.setValue(0)
            self.spinner.setVisible(True)
            QtWidgets.QApplication.processEvents()

            # create and start worker
            # If the user provided an explicit resolved source root override, pass it
            src_override = None
            try:
                so = self.db1_root_override.text().strip() if getattr(self, 'db1_root_override', None) is not None else ''
                if so:
                    src_override = so
            except Exception:
                src_override = None
            tgt_override = None
            try:
                to = self.db2_root_override.text().strip() if getattr(self, 'db2_root_override', None) is not None else ''
                if to:
                    tgt_override = to
            except Exception:
                tgt_override = None
            self._transfer_worker = TransferWorker(db_src, db_tgt, run_src, run_tgt, checked, src_root_override=src_override, tgt_root_override=tgt_override)
            # initialize/clear the per-transfer error buffer so we don't show
            # many modal dialogs for individual file errors
            try:
                self._transfer_errors = []
            except Exception:
                pass
            self._transfer_worker.progress.connect(self._on_transfer_progress)
            self._transfer_worker.file_progress.connect(self._on_transfer_file_progress)
            self._transfer_worker.finished.connect(self._on_transfer_finished)
            self._transfer_worker.cancelled.connect(self._on_transfer_cancelled)
            self._transfer_worker.error.connect(self._on_transfer_error)
            self._transfer_worker.start()
            # show cancel button while transfer runs
            try:
                self.cancel_btn.setVisible(True)
                self.cancel_btn.setEnabled(True)
            except Exception:
                pass
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Transfer error", f"Failed to start transfer: {e}")
            self._set_ui_enabled(True)
            try:
                self.spinner.setVisible(False)
            except Exception:
                pass

    def _set_ui_enabled(self, enabled: bool):
        """Enable/disable UI controls during background transfer."""
        try:
            self.compare_btn.setEnabled(enabled)
            self.show_missing_btn.setEnabled(enabled and len(self.last_missing_rows) > 0)
            self.transfer_btn.setEnabled(enabled)
            self.db1_browse.setEnabled(enabled)
            self.db2_browse.setEnabled(enabled)
            self.db1_path.setEnabled(enabled)
            self.db2_path.setEnabled(enabled)
            self.db1_runs.setEnabled(enabled)
            self.db2_runs.setEnabled(enabled)
            try:
                if getattr(self, 'db1_root_override', None) is not None:
                    self.db1_root_override.setEnabled(enabled)
            except Exception:
                pass
            try:
                if getattr(self, 'db1_root_browse', None) is not None:
                    self.db1_root_browse.setEnabled(enabled)
            except Exception:
                pass
            try:
                if getattr(self, 'db2_root_override', None) is not None:
                    self.db2_root_override.setEnabled(enabled)
            except Exception:
                pass
            try:
                if getattr(self, 'db2_root_browse', None) is not None:
                    self.db2_root_browse.setEnabled(enabled)
            except Exception:
                pass
            self.tree.setEnabled(enabled)
        except Exception:
            pass

    def _on_transfer_progress(self, pct: int):
        try:
            # ensure determinate mode
            if self.spinner.maximum() == 0:
                self.spinner.setRange(0, 100)
            self.spinner.setValue(int(pct))
            # keep percentage and optionally current filename updated by file_progress
            cur_text = self.results_label.text()
            # if the label already contains a filename line, keep it and update percent on first line
            if '\n' in cur_text:
                parts = cur_text.split('\n', 1)
                first = f"Transferring... {pct}%"
                rest = parts[1]
                self.results_label.setText(first + "\n" + rest)
            else:
                self.results_label.setText(f"Transferring... {pct}%")
            QtWidgets.QApplication.processEvents()
        except Exception:
            pass

    def _on_transfer_file_progress(self, filepath: str):
        try:
            # show current filename beneath the percentage
            try:
                short = filepath
                # optionally shorten long paths
                if len(short) > 120:
                    short = '...' + short[-117:]
                cur = self.results_label.text()
                if '\n' in cur:
                    first = cur.split('\n', 1)[0]
                    self.results_label.setText(first + "\n" + short)
                else:
                    self.results_label.setText(f"Transferring... 0%\n{short}")
            except Exception:
                # fallback: set simple text
                self.results_label.setText(f"Transferring: {filepath}")
            QtWidgets.QApplication.processEvents()
        except Exception:
            pass

    def _on_transfer_finished(self, transfer_id: int, transferred: int):
        try:
            self.spinner.setVisible(False)
        except Exception:
            pass
        try:
            self._set_ui_enabled(True)
            # cleanup worker
            try:
                self._transfer_worker.progress.disconnect(self._on_transfer_progress)
                self._transfer_worker.finished.disconnect(self._on_transfer_finished)
                try:
                    self._transfer_worker.cancelled.disconnect(self._on_transfer_cancelled)
                except Exception:
                    pass
                try:
                    self._transfer_worker.file_progress.disconnect(self._on_transfer_file_progress)
                except Exception:
                    pass
                self._transfer_worker.error.disconnect(self._on_transfer_error)
            except Exception:
                pass
            self._transfer_worker = None
        except Exception:
            pass
        try:
            self.cancel_btn.setVisible(False)
            self.cancel_btn.setEnabled(False)
        except Exception:
            pass
        # If there were errors during transfer, aggregate and show a single
        # summary dialog (unless we're shutting down, in which case we avoid
        # modal dialogs and just update the results label). This prevents many
        # stacked modal dialogs from appearing.
        try:
            errs = getattr(self, '_transfer_errors', []) or []
            if errs:
                # build a short summary (limit to first 10 messages)
                summary_lines = [f"Transferred {transferred} files to target DB. Transfer id={transfer_id}."]
                summary_lines.append(f"{len(errs)} errors occurred during transfer. First messages:")
                for e in errs[:10]:
                    # keep lines short
                    line = str(e)
                    if len(line) > 300:
                        line = line[:297] + '...'
                    summary_lines.append(line)
                summary = "\n\n".join(summary_lines)
                if not getattr(self, '_shutting_down', False):
                    try:
                        QtWidgets.QMessageBox.warning(self, "Transfer completed with errors", summary)
                    except Exception:
                        # fallback: update label
                        try:
                            self.results_label.setText(f"Transferred {transferred} files; {len(errs)} errors (see log).")
                        except Exception:
                            pass
                else:
                    try:
                        self.results_label.setText(f"Transferred {transferred} files; {len(errs)} errors (shutting down).")
                    except Exception:
                        pass
            else:
                try:
                    if not getattr(self, '_shutting_down', False):
                        QtWidgets.QMessageBox.information(self, "Transfer complete", f"Transferred {transferred} files to target DB. Transfer id={transfer_id}")
                    else:
                        try:
                            self.results_label.setText(f"Transferred {transferred} files. Transfer id={transfer_id}")
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    def _on_transfer_error(self, msg: str):
        # Record the error and update the results label non-modally. We avoid
        # showing a modal dialog per error to prevent many stacked dialogs and
        # possible race conditions during shutdown.
        try:
            # determine if this is the first error for this transfer
            try:
                first_error = not bool(getattr(self, '_transfer_errors', None))
            except Exception:
                first_error = True
            try:
                if not hasattr(self, '_transfer_errors'):
                    self._transfer_errors = []
                self._transfer_errors.append(str(msg))
            except Exception:
                try:
                    self._transfer_errors = [str(msg)]
                except Exception:
                    pass

            # update UI minimally
            try:
                self.spinner.setVisible(False)
            except Exception:
                pass
            try:
                self._set_ui_enabled(True)
            except Exception:
                pass
            try:
                self.cancel_btn.setVisible(False)
                self.cancel_btn.setEnabled(False)
            except Exception:
                pass

            # On the first error, present an Abort/Continue dialog so the user
            # can decide whether to stop or skip remaining files.
            try:
                transferred_count = int(getattr(self, '_transfer_success_count', 0) or 0)
            except Exception:
                transferred_count = 0
            if first_error and not getattr(self, '_shutting_down', False):
                try:
                    mb = QtWidgets.QMessageBox(self)
                    mb.setIcon(QtWidgets.QMessageBox.Critical)
                    mb.setWindowTitle("Transfer error")
                    mb.setText(f"Error after {transferred_count} transferred files:\n\n{msg}")
                    abort_btn = mb.addButton("Abort", QtWidgets.QMessageBox.RejectRole)
                    cont_btn = mb.addButton("Continue", QtWidgets.QMessageBox.AcceptRole)
                    mb.exec_()
                    clicked = mb.clickedButton()
                    if clicked is cont_btn:
                        action = 'continue'
                    else:
                        action = 'abort'
                    # inform worker of the user's decision
                    if getattr(self, '_transfer_worker', None) is not None:
                        try:
                            self._transfer_worker._error_action = action
                        except Exception:
                            pass
                        try:
                            # if user chose to continue, ensure worker.cancelled is False
                            if action == 'continue':
                                try:
                                    self._transfer_worker._cancelled = False
                                except Exception:
                                    pass
                            elif action == 'abort':
                                try:
                                    self._transfer_worker._cancelled = True
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        try:
                            if getattr(self._transfer_worker, '_error_action_event', None) is not None:
                                self._transfer_worker._error_action_event.set()
                        except Exception:
                            pass
                except Exception:
                    # if dialog failed, fall back to non-modal logging
                    try:
                        cur = self.results_label.text() or ''
                        line = f"Error: {msg}"
                        new = (cur + "\n" + line).strip()
                        if len(new) > 5000:
                            new = new[-5000:]
                        self.results_label.setText(new)
                    except Exception:
                        pass
            else:
                # Subsequent errors: log to results label
                try:
                    cur = self.results_label.text() or ''
                    line = f"Error: {msg}"
                    new = (cur + "\n" + line).strip()
                    if len(new) > 5000:
                        new = new[-5000:]
                    self.results_label.setText(new)
                except Exception:
                    pass
                # wake worker (non-blocking) and indicate continue so it doesn't wait
                try:
                    if getattr(self, '_transfer_worker', None) is not None:
                        try:
                            self._transfer_worker._error_action = 'continue'
                        except Exception:
                            pass
                        try:
                            if getattr(self._transfer_worker, '_error_action_event', None) is not None:
                                self._transfer_worker._error_action_event.set()
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    def on_cancel_transfer(self):
        """User requested transfer cancellation: signal the worker and update UI."""
        try:
            if getattr(self, '_transfer_worker', None) is None:
                return
            try:
                self._transfer_worker.cancel()
            except Exception:
                # best-effort
                pass
            try:
                self.cancel_btn.setEnabled(False)
            except Exception:
                pass
            try:
                self.results_label.setText("Cancelling transfer...")
            except Exception:
                pass
        except Exception:
            pass

    def _on_transfer_cancelled(self, transferred: int):
        try:
            self.spinner.setVisible(False)
        except Exception:
            pass
        try:
            self._set_ui_enabled(True)
            # cleanup worker
            try:
                self._transfer_worker.progress.disconnect(self._on_transfer_progress)
                try:
                    self._transfer_worker.cancelled.disconnect(self._on_transfer_cancelled)
                except Exception:
                    pass
                self._transfer_worker.finished.disconnect(self._on_transfer_finished)
                try:
                    self._transfer_worker.file_progress.disconnect(self._on_transfer_file_progress)
                except Exception:
                    pass
                self._transfer_worker.error.disconnect(self._on_transfer_error)
            except Exception:
                pass
            self._transfer_worker = None
        except Exception:
            pass
        try:
            self.cancel_btn.setVisible(False)
            self.cancel_btn.setEnabled(False)
        except Exception:
            pass
        try:
            if not getattr(self, '_shutting_down', False):
                QtWidgets.QMessageBox.information(self, "Transfer cancelled", f"Transfer cancelled after {transferred} files were transferred.")
            else:
                try:
                    self.results_label.setText(f"Transfer cancelled after {transferred} files.")
                except Exception:
                    pass
        except Exception:
            pass

    def show_about(self):
        """Show an About dialog with program name, author and version."""
        try:
            prog = "fileSage"
            author = "Brett J. Nelson"
            text = f"{prog}\nAuthor: {author}\nVersion: {VERSION}"
            # Use a resizable QDialog so the user can expand to read the license.
            try:
                dlg = QtWidgets.QDialog(self)
                dlg.setWindowTitle(f"About {prog}")
                dlg.setModal(True)
                dlg.setMinimumSize(360, 180)
                # allow the user to resize via a size grip
                try:
                    dlg.setSizeGripEnabled(True)
                except Exception:
                    pass

                v = QtWidgets.QVBoxLayout(dlg)
                lbl = QtWidgets.QLabel(text)
                # allow selecting/copying the short text
                try:
                    lbl.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
                except Exception:
                    pass
                v.addWidget(lbl)

                # show the license in a read-only, expandable text area
                try:
                    license_widget = QtWidgets.QPlainTextEdit()
                    license_widget.setReadOnly(True)
                    license_widget.setPlainText(LICENSE_TEXT)
                    license_widget.setMinimumHeight(120)
                    license_widget.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
                    v.addWidget(license_widget, 1)
                except Exception:
                    # fallback: attach as detailed text on a message box if editor not available
                    try:
                        msg = QtWidgets.QMessageBox(self)
                        msg.setWindowTitle(f"About {prog}")
                        msg.setText(text)
                        msg.setIcon(QtWidgets.QMessageBox.Information)
                        try:
                            msg.setDetailedText(LICENSE_TEXT)
                        except Exception:
                            pass
                        msg.setMinimumSize(360, 180)
                        msg.setStandardButtons(QtWidgets.QMessageBox.Ok)
                        msg.exec_()
                        return
                    except Exception:
                        pass

                btns = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok)
                btns.accepted.connect(dlg.accept)
                v.addWidget(btns)
                dlg.exec_()
            except Exception:
                # ultimate fallback to simple information dialog
                try:
                    QtWidgets.QMessageBox.information(self, f"About {prog}", text)
                except Exception:
                    pass
        except Exception:
            pass

    def resizeEvent(self, event):
        # Scale and center-crop background pixmap to fill the current tab/page widget
        try:
            # determine the widget to apply background to: current tab widget if tabs exist
            target_widget = None
            pix = None
            tab_name = None
            if getattr(self, 'tabs', None) is not None:
                target_widget = self.tabs.currentWidget()
                try:
                    idx = self.tabs.currentIndex()
                    if idx >= 0:
                        tab_name = self.tabs.tabText(idx)
                except Exception:
                    tab_name = None
            if tab_name == 'TRANSFER':
                pix = getattr(self, 'bg_pixmap', None)
                # prefer using the original transfer page as the target if available
                if getattr(self, 'transfer_page', None) is not None:
                    target_widget = self.transfer_page
            else:
                pix = getattr(self, 'tab_bg_pixmaps', {}).get(tab_name) if tab_name else None
            if target_widget is None:
                target_widget = self.centralWidget()

            if pix is not None and not pix.isNull() and target_widget is not None:
                size = target_widget.size()
                if size.width() > 0 and size.height() > 0:
                    # scale to cover the widget (may be larger) then center-crop into a target pixmap
                    scaled = pix.scaled(size, QtCore.Qt.KeepAspectRatioByExpanding, QtCore.Qt.SmoothTransformation)
                    target = QtGui.QPixmap(size)
                    target.fill(QtCore.Qt.transparent)
                    painter = QtGui.QPainter(target)
                    x = (size.width() - scaled.width()) // 2
                    y = (size.height() - scaled.height()) // 2
                    painter.drawPixmap(x, y, scaled)
                    painter.end()
                    pal = target_widget.palette()
                    pal.setBrush(target_widget.backgroundRole(), QtGui.QBrush(target))
                    target_widget.setPalette(pal)
        except Exception:
            # resize should not crash the app
            pass
        return super().resizeEvent(event)

    def _apply_background_to_widget(self, widget, pix):
        """Apply a centered, scaled background pixmap to a specific widget."""
        try:
            if widget is None or pix is None or pix.isNull():
                return
            size = widget.size()
            if size.width() <= 0 or size.height() <= 0:
                return
            scaled = pix.scaled(size, QtCore.Qt.KeepAspectRatioByExpanding, QtCore.Qt.SmoothTransformation)
            target = QtGui.QPixmap(size)
            target.fill(QtCore.Qt.transparent)
            painter = QtGui.QPainter(target)
            x = (size.width() - scaled.width()) // 2
            y = (size.height() - scaled.height()) // 2
            painter.drawPixmap(x, y, scaled)
            painter.end()
            pal = widget.palette()
            pal.setBrush(widget.backgroundRole(), QtGui.QBrush(target))
            widget.setPalette(pal)
            widget.setAutoFillBackground(True)
            try:
                widget.update()
            except Exception:
                pass
        except Exception:
            pass

    def _on_tab_changed(self, idx):
        """Slot called when the current tab changes; refresh the tab's background immediately."""
        try:
            if getattr(self, 'tabs', None) is None:
                return
            if idx < 0:
                return
            try:
                tab_name = self.tabs.tabText(idx)
            except Exception:
                tab_name = None
            if tab_name == 'TRANSFER':
                pix = getattr(self, 'bg_pixmap', None)
                widget = getattr(self, 'transfer_page', None) or self.tabs.widget(idx)
            else:
                pix = getattr(self, 'tab_bg_pixmaps', {}).get(tab_name) if tab_name else None
                widget = self.tabs.widget(idx)
            if pix and widget is not None:
                # Apply immediately instead of waiting for a resize event
                self._apply_background_to_widget(widget, pix)
        except Exception:
            pass

    # ---- SCAN tab helpers ----
    def _choose_scan_db(self):
        # Start file browser in last-used scan DB directory (if available),
        # otherwise default to the user's home directory.
        start_dir = getattr(self, '_last_scan_db_dir', None) or os.path.expanduser('~')
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, 'Select SQLite DB', start_dir, 'SQLite DB (*.db *.sqlite *.sqlite3);;All Files (*)')
        if path:
            try:
                self.scan_db_path.setText(path)
                # remember directory for next time
                try:
                    self._last_scan_db_dir = os.path.dirname(path) or os.path.expanduser('~')
                except Exception:
                    self._last_scan_db_dir = os.path.expanduser('~')
            except Exception:
                pass

    def _choose_resume_db(self):
        # start in the last-used resume directory if available, otherwise fall back to home
        start_dir = getattr(self, '_last_resume_dir', None) or os.path.expanduser("~")
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, 'Select SQLite DB', start_dir, 'SQLite DB (*.db *.sqlite *.sqlite3);;All Files (*)')
        if path:
            try:
                self.resume_db_path.setText(path)
                # populate unfinished runs
                # remember the directory for next time
                try:
                    self._last_resume_dir = os.path.dirname(path) or os.path.expanduser('~')
                except Exception:
                    self._last_resume_dir = os.path.expanduser('~')
                self._load_resume_runs()
            except Exception:
                pass

    def _load_resume_runs(self):
        """Load unfinished scan_runs (finished_at IS NULL) into resume_runs combo box.
        Mark skip_resume items in red.
        """
        path = self.resume_db_path.text().strip() if getattr(self, 'resume_db_path', None) is not None else None
        self.resume_runs.clear()
        if not path:
            return
        try:
            conn = sqlite3.connect(path)
        except Exception:
            return
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, started_at, root, COALESCE(skip_resume,0) FROM scan_runs WHERE finished_at IS NULL ORDER BY id")
            rows = cur.fetchall()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return
        if not rows:
            self.resume_runs.addItem("(no unfinished runs)")
        else:
            for r in rows:
                rid, started_at, root = r[0], r[1], r[2]
                skip_flag = r[3] if len(r) > 3 else 0
                try:
                    started_s = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(started_at)) if started_at else 'N/A'
                except Exception:
                    started_s = str(started_at)
                label = f"id={rid} root={root} started={started_s}"
                idx = self.resume_runs.count()
                # store run id as user data (Qt.UserRole) via addItem second arg
                self.resume_runs.addItem(label, rid)
                # also store skip flag under a secondary role so we can act on it later
                try:
                    self.resume_runs.setItemData(idx, int(bool(skip_flag)), QtCore.Qt.UserRole + 1)
                    # if skip_resume is set, color this item red so user knows
                    if skip_flag:
                        brush = QtGui.QBrush(QtGui.QColor('red'))
                        self.resume_runs.setItemData(idx, brush, QtCore.Qt.ForegroundRole)
                except Exception:
                    pass
        try:
            conn.close()
        except Exception:
            pass

    def _on_resume_selection_changed(self):
        """Populate the Run info box for the selected unfinished run."""
        try:
            idx = self.resume_runs.currentIndex()
            if idx < 0:
                try:
                    self.resume_info.clear()
                except Exception:
                    pass
                return
            data = self.resume_runs.itemData(idx)
            try:
                run_id = int(data)
            except Exception:
                try:
                    self.resume_info.setPlainText("")
                except Exception:
                    pass
                return
            dbpath = self.resume_db_path.text().strip() if getattr(self, 'resume_db_path', None) is not None else None
            if not dbpath:
                try:
                    self.resume_info.setPlainText('No database selected')
                except Exception:
                    pass
                return
            try:
                conn = sqlite3.connect(dbpath)
            except Exception:
                try:
                    self.resume_info.setPlainText('Failed to open database')
                except Exception:
                    pass
                return
            try:
                cur = conn.cursor()
                cur.execute("SELECT name, comment, command_line, log FROM scan_runs WHERE id = ?", (run_id,))
                r = cur.fetchone()
                if not r:
                    self.resume_info.setPlainText('(no details found)')
                else:
                    name, comment, cmdline, logtext = r
                    parts = []
                    parts.append(f"Name: {name if name else ''}")
                    parts.append(f"Comment: {comment if comment else ''}")
                    parts.append(f"Command line: {cmdline if cmdline else ''}")
                    parts.append("")
                    parts.append("Log:")
                    parts.append(logtext if logtext else '(no log entries)')
                    out = '\n'.join(parts)
                    try:
                        self.resume_info.setPlainText(out)
                    except Exception:
                        pass
            except Exception:
                try:
                    self.resume_info.setPlainText('Failed to read run details')
                except Exception:
                    pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass

    def _choose_scan_root(self):
        start_dir = os.path.expanduser('~')
        path = QtWidgets.QFileDialog.getExistingDirectory(self, 'Select scan root', start_dir)
        if path:
            try:
                self.scan_root.setText(path)
            except Exception:
                pass

    def _choose_db1_root_override(self):
        """Prompt user to choose a resolved source root (directory) for DB1."""
        try:
            start_dir = os.path.expanduser('~')
            path = QtWidgets.QFileDialog.getExistingDirectory(self, 'Select source root', start_dir)
            if path:
                try:
                    self.db1_root_override.setText(path)
                except Exception:
                    pass
        except Exception:
            pass

    def _choose_db2_root_override(self):
        """Prompt user to choose a resolved target root (directory) for DB2."""
        try:
            start_dir = os.path.expanduser('~')
            path = QtWidgets.QFileDialog.getExistingDirectory(self, 'Select target root', start_dir)
            if path:
                try:
                    self.db2_root_override.setText(path)
                except Exception:
                    pass
        except Exception:
            pass

    # ---- HARDLINK tab helpers (mirror of resume UI but placeholder actions) ----
    def _choose_hardlink_db(self):
        start_dir = getattr(self, '_last_hardlink_dir', None) or os.path.expanduser('~')
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, 'Select SQLite DB', start_dir, 'SQLite DB (*.db *.sqlite *.sqlite3);;All Files (*)')
        if path:
            try:
                self.hardlink_db_path.setText(path)
                try:
                    self._last_hardlink_dir = os.path.dirname(path) or os.path.expanduser('~')
                except Exception:
                    self._last_hardlink_dir = os.path.expanduser('~')
                self._load_hardlink_runs()
            except Exception:
                pass

    def _load_hardlink_runs(self):
        """Load finished scan_runs into hardlink combo (finished_at IS NOT NULL)."""
        path = self.hardlink_db_path.text().strip() if getattr(self, 'hardlink_db_path', None) is not None else None
        self.hardlink_runs.clear()
        if not path:
            return
        try:
            conn = sqlite3.connect(path)
        except Exception:
            return
        try:
            cur = conn.cursor()
            cur.execute("SELECT id, started_at, root, COALESCE(skip_resume,0) FROM scan_runs WHERE finished_at IS NOT NULL ORDER BY id")
            rows = cur.fetchall()
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return
        if not rows:
            self.hardlink_runs.addItem("(no finished runs)")
        else:
            for r in rows:
                rid, started_at, root = r[0], r[1], r[2]
                skip_flag = r[3] if len(r) > 3 else 0
                try:
                    started_s = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(started_at)) if started_at else 'N/A'
                except Exception:
                    started_s = str(started_at)
                label = f"id={rid} root={root} started={started_s}"
                idx = self.hardlink_runs.count()
                self.hardlink_runs.addItem(label, rid)
                try:
                    self.hardlink_runs.setItemData(idx, int(bool(skip_flag)), QtCore.Qt.UserRole + 1)
                    if skip_flag:
                        brush = QtGui.QBrush(QtGui.QColor('red'))
                        self.hardlink_runs.setItemData(idx, brush, QtCore.Qt.ForegroundRole)
                except Exception:
                    pass
        try:
            conn.close()
        except Exception:
            pass

    def _on_hardlink_selection_changed(self):
        try:
            idx = self.hardlink_runs.currentIndex()
            if idx < 0:
                try:
                    self.hardlink_info.clear()
                except Exception:
                    pass
                return
            data = self.hardlink_runs.itemData(idx)
            try:
                run_id = int(data)
            except Exception:
                try:
                    self.hardlink_info.setPlainText("")
                except Exception:
                    pass
                return
            dbpath = self.hardlink_db_path.text().strip() if getattr(self, 'hardlink_db_path', None) is not None else None
            if not dbpath:
                try:
                    self.hardlink_info.setPlainText('No database selected')
                except Exception:
                    pass
                return
            try:
                conn = sqlite3.connect(dbpath)
            except Exception:
                try:
                    self.hardlink_info.setPlainText('Failed to open database')
                except Exception:
                    pass
                return
            try:
                cur = conn.cursor()
                cur.execute("SELECT name, comment, command_line, log FROM scan_runs WHERE id = ?", (run_id,))
                r = cur.fetchone()
                if not r:
                    self.hardlink_info.setPlainText('(no details found)')
                else:
                    name, comment, cmdline, logtext = r
                    parts = []
                    parts.append(f"Name: {name if name else ''}")
                    parts.append(f"Comment: {comment if comment else ''}")
                    parts.append(f"Command line: {cmdline if cmdline else ''}")
                    parts.append("")
                    parts.append("Log:")
                    parts.append(logtext if logtext else '(no log entries)')
                    out = '\n'.join(parts)
                    try:
                        self.hardlink_info.setPlainText(out)
                    except Exception:
                        pass
            except Exception:
                try:
                    self.hardlink_info.setPlainText('Failed to read run details')
                except Exception:
                    pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass

    def _append_hardlink_output(self, text):
        try:
            self.hardlink_output.moveCursor(QtGui.QTextCursor.End)
            self.hardlink_output.insertPlainText(text)
            self.hardlink_output.moveCursor(QtGui.QTextCursor.End)
        except Exception:
            try:
                cur = self.hardlink_output.toPlainText()
                self.hardlink_output.setPlainText(cur + text)
            except Exception:
                pass

    def _on_run_hardlink(self):
        # Confirm destructive operation then deduplicate by creating hardlinks.
        try:
            idx = self.hardlink_runs.currentIndex()
            if idx < 0:
                QtWidgets.QMessageBox.critical(self, 'Error', 'Please select a run to hardlink')
                return
            data = self.hardlink_runs.itemData(idx)
            try:
                run_id = int(data)
            except Exception:
                QtWidgets.QMessageBox.critical(self, 'Error', 'Invalid run id selected')
                return

            # show confirmation dialog
            msg = (
                "This operation will replace duplicate files with hardlinks to the first occurrence.\n"
                "This is destructive to duplicate file data (files will be replaced).\n\n"
                "Do you want to continue?"
            )
            resp = QtWidgets.QMessageBox.question(self, 'Confirm hardlink', msg, QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
            if resp != QtWidgets.QMessageBox.Yes:
                return

            # disable UI controls for duration
            # disable UI controls for duration (keep Cancel enabled)
            try:
                self._set_hardlink_controls_enabled(False)
                self.hardlink_cancel_btn.setEnabled(True)
            except Exception:
                pass

            self.hardlink_output.clear()
            self._append_hardlink_output(f"Starting hardlink dedupe for run id={run_id}\n")

            db = self.hardlink_db_path.text().strip() if getattr(self, 'hardlink_db_path', None) is not None else None
            if not db:
                QtWidgets.QMessageBox.critical(self, 'Error', 'No database selected for hardlink operation')
                return

            conn = sqlite3.connect(db)
            cur = conn.cursor()
            # get scan run root for informative messages
            try:
                cur.execute("SELECT root FROM scan_runs WHERE id = ?", (run_id,))
                rr = cur.fetchone()
                run_root = rr[0] if rr and rr[0] else None
            except Exception:
                run_root = None

            # find content_hash_id groups with more than one file
            try:
                cur.execute(
                    "SELECT content_hash_id, COUNT(*) FROM files WHERE scan_run_id = ? AND content_hash_id IS NOT NULL GROUP BY content_hash_id HAVING COUNT(*) > 1",
                    (run_id,)
                )
                groups = cur.fetchall()
            except Exception as e:
                self._append_hardlink_output(f"Failed to enumerate duplicate hashes: {e}\n")
                groups = []

            total_groups = len(groups)
            self._append_hardlink_output(f"Found {total_groups} duplicate-hash groups to process.\n")

            processed_groups = 0
            skipped_count = 0
            for (ch_id, cnt) in groups:
                processed_groups += 1
                self._append_hardlink_output(f"Processing hash id={ch_id} ({cnt} files) [{processed_groups}/{total_groups}]\n")
                QtWidgets.QApplication.processEvents()
                try:
                    cur.execute(
                        "SELECT dirpath, name FROM files WHERE scan_run_id = ? AND content_hash_id = ? ORDER BY dirpath, name",
                        (run_id, ch_id),
                    )
                    rows = cur.fetchall()
                except Exception as e:
                    self._append_hardlink_output(f"  Failed to list files for hash {ch_id}: {e}\n")
                    continue
                if not rows or len(rows) < 2:
                    continue
                # choose first as canonical
                first_dir, first_name = rows[0]
                first_path = os.path.join(first_dir, first_name)
                # verify first exists
                try:
                    if not os.path.exists(first_path):
                        self._append_hardlink_output(f"  Skipping group; canonical file missing: {first_path}\n")
                        continue
                except Exception:
                    self._append_hardlink_output(f"  Skipping group; error checking canonical file: {first_path}\n")
                    continue

                # iterate duplicates
                any_hardlinked = False
                for dup in rows[1:]:
                    dup_dir, dup_name = dup
                    dup_path = os.path.join(dup_dir, dup_name)
                    try:
                        if not os.path.exists(dup_path):
                            self._append_hardlink_output(f"    Skipping missing file: {dup_path}\n")
                            continue
                        # if already same inode, skip
                        try:
                            s_first = os.stat(first_path)
                            s_dup = os.stat(dup_path)
                            # Minimum size filter: if enabled, skip files smaller than threshold
                            if getattr(self, 'hardlink_min_size_chk', None) and self.hardlink_min_size_chk.isChecked():
                                try:
                                    min_sz = int(getattr(self, 'hardlink_min_size', None).value())
                                except Exception:
                                    min_sz = 0
                                try:
                                    if getattr(s_dup, 'st_size', None) is not None and s_dup.st_size < int(min_sz):
                                        self._append_hardlink_output(f"    Skipping small file (<{min_sz} bytes): {dup_path}\n")
                                        skipped_count += 1
                                        continue
                                except Exception:
                                    pass
                            if s_first.st_ino == s_dup.st_ino and s_first.st_dev == s_dup.st_dev:
                                self._append_hardlink_output(f"    Already hardlinked: {dup_path}\n")
                                continue
                        except Exception:
                            pass

                        # create hardlink at a temporary path then replace the duplicate atomically
                        tmp = dup_path + f".tmp_hl.{os.getpid()}"
                        try:
                            # remove any leftover tmp
                            if os.path.exists(tmp):
                                try:
                                    os.remove(tmp)
                                except Exception:
                                    pass
                            os.link(first_path, tmp)
                            os.replace(tmp, dup_path)
                        except OSError as oe:
                            # cross-device or permission errors
                            self._append_hardlink_output(f"    Failed to hardlink {dup_path}: {oe}\n")
                            try:
                                if os.path.exists(tmp):
                                    os.remove(tmp)
                            except Exception:
                                pass
                            continue
                        except Exception as e:
                            self._append_hardlink_output(f"    Error creating hardlink for {dup_path}: {e}\n")
                            try:
                                if os.path.exists(tmp):
                                    os.remove(tmp)
                            except Exception:
                                pass
                            continue

                        # update DB row for dup to reflect new inode/dev/size/times
                        try:
                            st = os.stat(dup_path)
                            dev_major = os.major(st.st_dev)
                            dev_minor = os.minor(st.st_dev)
                            ino = st.st_ino
                            size = st.st_size
                            atime = st.st_atime
                            mtime = st.st_mtime
                            ctime = st.st_ctime
                            mode = st.st_mode
                            uid = st.st_uid if hasattr(st, 'st_uid') else None
                            gid = st.st_gid if hasattr(st, 'st_gid') else None
                            cur.execute(
                                "UPDATE files SET dev_major = ?, dev_minor = ?, ino = ?, size = ?, atime = ?, mtime = ?, ctime = ?, mode = ?, uid = ?, gid = ? WHERE scan_run_id = ? AND dirpath = ? AND name = ?",
                                (dev_major, dev_minor, ino, size, atime, mtime, ctime, mode, uid, gid, run_id, dup_dir, dup_name),
                            )
                            conn.commit()
                            any_hardlinked = True
                            self._append_hardlink_output(f"    Replaced {dup_path} with hardlink to {first_path}\n")
                        except Exception as e:
                            self._append_hardlink_output(f"    Hardlink created but failed to update DB for {dup_path}: {e}\n")
                            continue
                    except Exception as e:
                        self._append_hardlink_output(f"    Unexpected error for {dup_path}: {e}\n")
                        continue

                # After processing all duplicates for this canonical file, if we created
                # any hardlinks, read the on-disk link count and update the DB rows
                # for this group's files to reflect the inode_ref_count and mark
                # them as hardlinked.
                try:
                    if any_hardlinked:
                        try:
                            st_first = os.stat(first_path)
                            nlink = getattr(st_first, 'st_nlink', None)
                            if nlink is None:
                                # fallback to 1 if unavailable
                                nlink = 1
                        except Exception as e:
                            nlink = 1
                        try:
                            cur.execute(
                                "UPDATE files SET inode_ref_count = ?, hardlinked = 1 WHERE scan_run_id = ? AND content_hash_id = ?",
                                (nlink, run_id, ch_id),
                            )
                            conn.commit()
                            self._append_hardlink_output(f"  Updated DB: set inode_ref_count={nlink} and hardlinked=1 for content_hash_id={ch_id}\n")
                        except Exception as e:
                            self._append_hardlink_output(f"  Warning: failed to update inode_ref_count/hardlinked in DB for hash {ch_id}: {e}\n")
                except Exception:
                    pass

            self._append_hardlink_output("Hardlink dedupe complete.\n")
            try:
                if skipped_count:
                    self._append_hardlink_output(f"{skipped_count} files were skipped due to minimum size filter.\n")
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            # restore UI state
            try:
                self._set_hardlink_controls_enabled(True)
                self.hardlink_cancel_btn.setEnabled(False)
            except Exception:
                pass
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, 'Error', f'Error preparing hardlink: {e}')

    def _on_hardlink_dryrun(self):
        """List the hardlink operations that would be performed without making changes."""
        try:
            idx = self.hardlink_runs.currentIndex()
            if idx < 0:
                QtWidgets.QMessageBox.critical(self, 'Error', 'Please select a run to dry-run')
                return
            data = self.hardlink_runs.itemData(idx)
            try:
                run_id = int(data)
            except Exception:
                QtWidgets.QMessageBox.critical(self, 'Error', 'Invalid run id selected')
                return

            # disable UI controls for duration (keep Cancel enabled)
            try:
                self._set_hardlink_controls_enabled(False)
                self.hardlink_cancel_btn.setEnabled(True)
            except Exception:
                pass

            self.hardlink_output.clear()
            self._append_hardlink_output(f"Dry run: listing hardlink candidates for run id={run_id}\n")

            db = self.hardlink_db_path.text().strip() if getattr(self, 'hardlink_db_path', None) is not None else None
            if not db:
                QtWidgets.QMessageBox.critical(self, 'Error', 'No database selected for hardlink dry-run')
                return

            try:
                conn = sqlite3.connect(db)
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, 'Error', f'Failed to open DB: {e}')
                return
            cur = conn.cursor()

            try:
                cur.execute(
                    "SELECT content_hash_id, COUNT(*) FROM files WHERE scan_run_id = ? AND content_hash_id IS NOT NULL GROUP BY content_hash_id HAVING COUNT(*) > 1",
                    (run_id,)
                )
                groups = cur.fetchall()
            except Exception as e:
                self._append_hardlink_output(f"Failed to enumerate duplicate hashes: {e}\n")
                groups = []

            if not groups:
                self._append_hardlink_output("No duplicate files found for hardlinking.\n")
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    self._set_hardlink_controls_enabled(True)
                    self.hardlink_cancel_btn.setEnabled(False)
                except Exception:
                    pass
                return

            total_groups = len(groups)
            processed = 0
            # count how many duplicate files would be replaced by hardlinks
            would_replace = 0
            would_replace_bytes = 0
            skipped_count = 0
            for (ch_id, cnt) in groups:
                processed += 1
                self._append_hardlink_output(f"Group {processed}/{total_groups}: hash id={ch_id} ({cnt} files)\n")
                QtWidgets.QApplication.processEvents()
                try:
                    # For dry-run avoid touching the filesystem: read required metadata from DB
                    cur.execute(
                        "SELECT dirpath, name, size, dev_major, dev_minor, ino FROM files WHERE scan_run_id = ? AND content_hash_id = ? ORDER BY dirpath, name",
                        (run_id, ch_id),
                    )
                    rows = cur.fetchall()
                except Exception as e:
                    self._append_hardlink_output(f"  Failed to list files for hash {ch_id}: {e}\n")
                    continue

                if not rows or len(rows) < 2:
                    continue

                # first row is canonical (we won't touch the disk; trust DB info)
                try:
                    first_dir, first_name, _first_size, first_dev_major, first_dev_minor, first_ino = rows[0]
                except Exception:
                    try:
                        first_dir, first_name = rows[0][:2]
                        _first_size = None
                        first_dev_major = first_dev_minor = first_ino = None
                    except Exception:
                        continue
                first_path = os.path.join(first_dir, first_name)
                self._append_hardlink_output(f"  Canonical: {first_path}\n")

                for dup in rows[1:]:
                    try:
                        # dup may include size and device/ino as later columns
                        try:
                            dup_dir, dup_name, dup_size, dup_dev_major, dup_dev_minor, dup_ino = dup
                        except Exception:
                            # fallback if some columns missing
                            dup_dir, dup_name = dup[0], dup[1]
                            dup_size = dup[2] if len(dup) > 2 else None
                            dup_dev_major = dup_dev_minor = dup_ino = None
                        dup_path = os.path.join(dup_dir, dup_name)
                        # Determine if DB records indicate these are already the same inode
                        try:
                            if (first_dev_major is not None and dup_dev_major is not None and
                                first_ino is not None and dup_ino is not None and
                                int(first_dev_major) == int(dup_dev_major) and int(first_dev_minor) == int(dup_dev_minor) and int(first_ino) == int(dup_ino)):
                                self._append_hardlink_output(f"    Already hardlinked (per DB): {dup_path}\n")
                                continue
                        except Exception:
                            pass

                        # Minimum size filter: use DB 'size' column rather than stat
                        try:
                            if getattr(self, 'hardlink_min_size_chk', None) and self.hardlink_min_size_chk.isChecked():
                                try:
                                    min_sz = int(getattr(self, 'hardlink_min_size', None).value())
                                except Exception:
                                    min_sz = 0
                                try:
                                    if dup_size is not None and int(dup_size) < int(min_sz):
                                        self._append_hardlink_output(f"    Skipping small file (<{min_sz} bytes): {dup_path}\n")
                                        skipped_count += 1
                                        continue
                                except Exception:
                                    pass
                        except Exception:
                            pass

                        self._append_hardlink_output(f"    Would replace {dup_path} with hardlink to {first_path}\n")
                        try:
                            would_replace += 1
                            if isinstance(dup_size, (int, float)) and dup_size > 0:
                                would_replace_bytes += int(dup_size)
                        except Exception:
                            pass
                    except Exception as e:
                        self._append_hardlink_output(f"    Error inspecting {dup_path}: {e}\n")
                        continue

                # Report summary of dry-run (include total bytes and skipped count)
            try:
                def _hr_bytes(n):
                    try:
                        if n is None:
                            return '0 B'
                        n = float(n)
                        units = ['B', 'KB', 'MB', 'GB', 'TB']
                        idx = 0
                        while n >= 1024.0 and idx < len(units) - 1:
                            n /= 1024.0
                            idx += 1
                        if units[idx] == 'B':
                            return f"{int(n)} {units[idx]}"
                        return f"{n:.2f} {units[idx]}"
                    except Exception:
                        return str(n)

                if would_replace:
                    hr = _hr_bytes(would_replace_bytes)
                    s = f"Dry run complete. No changes were made. {would_replace} files would be replaced by hardlinks (total size: {would_replace_bytes} bytes, {hr})."
                else:
                    s = "Dry run complete. No changes were made. No files would be replaced."
                try:
                    if skipped_count:
                        s = s + f" {skipped_count} files would be skipped due to minimum size filter."
                except Exception:
                    pass
                try:
                    self._append_hardlink_output(s + "\n")
                except Exception:
                    pass
            except Exception:
                try:
                    self._append_hardlink_output("Dry run complete. No changes were made.\n")
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass
            try:
                self._set_hardlink_controls_enabled(True)
                self.hardlink_cancel_btn.setEnabled(False)
            except Exception:
                pass
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, 'Error', f'Error preparing dry-run: {e}')

    def _on_cancel_hardlink(self):
        try:
            # placeholder cancel behavior: re-enable run button
            try:
                # restore full hardlink UI state
                self._set_hardlink_controls_enabled(True)
                self.hardlink_cancel_btn.setEnabled(False)
                self._append_hardlink_output('Hardlink operation cancelled (placeholder)\n')
            except Exception:
                pass
        except Exception:
            pass

    def _set_hardlink_controls_enabled(self, enabled: bool):
        """Enable/disable the primary hardlink controls (except Cancel).

        When an operation is running we want the Cancel button to remain enabled
        while other controls are disabled to avoid user changes mid-run.
        """
        try:
            # primary buttons
            try:
                self.hardlink_run_btn.setEnabled(enabled)
            except Exception:
                pass
            try:
                self.hardlink_dryrun_btn.setEnabled(enabled)
            except Exception:
                pass
            # inputs: DB path and runs selector and info pane
            try:
                self.hardlink_db_path.setEnabled(enabled)
            except Exception:
                pass
            try:
                self.hardlink_runs.setEnabled(enabled)
            except Exception:
                pass
            try:
                self.hardlink_info.setEnabled(enabled)
            except Exception:
                pass
        except Exception:
            pass

    # (resume controls removed) no-op placeholder kept for compatibility

    def _append_scan_output(self, text):
        try:
            # ensure UI updates happen in main thread
            self.scan_output.moveCursor(QtGui.QTextCursor.End)
            self.scan_output.insertPlainText(text)
            self.scan_output.moveCursor(QtGui.QTextCursor.End)
        except Exception:
            try:
                # fallback: append as simple text
                cur = self.scan_output.toPlainText()
                self.scan_output.setPlainText(cur + text)
            except Exception:
                pass

    def _append_resume_output(self, text):
        try:
            self.resume_output.moveCursor(QtGui.QTextCursor.End)
            self.resume_output.insertPlainText(text)
            self.resume_output.moveCursor(QtGui.QTextCursor.End)
        except Exception:
            try:
                cur = self.resume_output.toPlainText()
                self.resume_output.setPlainText(cur + text)
            except Exception:
                pass

    def _on_run_scan(self):
        try:
            # build command
            script_dir = os.path.dirname(os.path.abspath(__file__))
            fscan_py = os.path.join(script_dir, 'fscan.py')
            if not os.path.exists(fscan_py):
                QtWidgets.QMessageBox.critical(self, 'Error', f'Could not find fscan.py at {fscan_py}')
                return
            # prepare base args for fscan (without interpreter)
            base_args = []
            db = self.scan_db_path.text().strip()
            if db:
                base_args += ['--database', db]
            # If the selected DB exists and lacks the new columns, warn the user about a possible long ALTER
            try:
                if db and not self._confirm_db_will_be_altered(db):
                    return
            except Exception:
                pass
            root = self.scan_root.text().strip()
            if root:
                base_args.append(root)
            if self.scan_silent.isChecked():
                base_args.append('--silent')
            if self.scan_hash.isChecked():
                base_args.append('--hash')
            try:
                if getattr(self, 'scan_skip_resume', None) is not None and self.scan_skip_resume.isChecked():
                    base_args.append('--skip')
            except Exception:
                pass
            try:
                comment = self.scan_comment.toPlainText().strip()
            except Exception:
                try:
                    comment = self.scan_comment.text().strip()
                except Exception:
                    comment = ''
            if comment:
                base_args += ['--comment', comment]
            name = self.scan_name.text().strip()
            if name:
                base_args += ['--name', name]

            # choose whether to run the external 'fscan' executable (if present in script_dir)
            exe_path = os.path.join(script_dir, 'fscan')
            use_exe = os.path.isfile(exe_path) and os.access(exe_path, os.X_OK)
            if use_exe:
                prog = exe_path
                prog_args = base_args
                start_cmd = ' '.join([prog] + prog_args)
            else:
                # run the Python interpreter in unbuffered mode so stdout/stderr are forwarded line-by-line
                prog = sys.executable
                prog_args = ['-u', fscan_py] + base_args
                start_cmd = ' '.join([prog] + prog_args)
            # start QProcess
            try:
                # if db exists and needs ALTER, run that first in background and show a progress dialog
                run_direct = True
                try:
                    if db and os.path.exists(db):
                        # quick check
                        conn = sqlite3.connect(db)
                        try:
                            cur = conn.cursor()
                            cur.execute("PRAGMA table_info(files)")
                            cols = [r[1] for r in cur.fetchall()]
                        except Exception:
                            cols = []
                        try:
                            conn.close()
                        except Exception:
                            pass
                        if 'inode_ref_count' not in cols or 'hardlinked' not in cols:
                            run_direct = False
                except Exception:
                    run_direct = True

                # If DB needs ALTERs, we warn the user via _confirm_db_will_be_altered(),
                # but don't run ALTERs from the GUI. Instead, start fscan.py directly
                # and let it perform any necessary lightweight ALTERs in its init_db().
                self.scan_process = QtCore.QProcess(self)
                # merge channels so we capture stderr as well
                self.scan_process.setProcessChannelMode(QtCore.QProcess.MergedChannels)
                self.scan_process.readyReadStandardOutput.connect(self._on_scan_stdout)
                self.scan_process.finished.connect(self._on_scan_finished)
                # start
                self.scan_output.clear()
                self.scan_output.appendPlainText('Starting: ' + start_cmd + '\n')
                # QProcess expects program and args separately
                self.scan_process.start(prog, prog_args)
                started = self.scan_process.waitForStarted(500)
                if not started:
                    self.scan_output.appendPlainText('Failed to start scan process.\n')
                    return
                # update UI
                self.scan_run_btn.setEnabled(False)
                self.scan_cancel_btn.setEnabled(True)
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, 'Start error', f'Failed to start scan: {e}')
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, 'Error', f'Error preparing scan: {e}')

    def _on_run_resume(self):
        try:
            # find selected run id
            db = self.resume_db_path.text().strip() if getattr(self, 'resume_db_path', None) is not None else None
            if not db:
                QtWidgets.QMessageBox.critical(self, 'Error', 'Please select a database containing the unfinished run')
                return
            idx = self.resume_runs.currentIndex()
            if idx < 0:
                QtWidgets.QMessageBox.critical(self, 'Error', 'Please select an unfinished run to resume')
                return
            data = self.resume_runs.itemData(idx)
            try:
                run_id = int(data)
            except Exception:
                QtWidgets.QMessageBox.critical(self, 'Error', 'Invalid run id selected')
                return

            # If the selected run is marked skip_resume, clear that flag in the DB first
            try:
                skip_flag = self.resume_runs.itemData(idx, QtCore.Qt.UserRole + 1)
            except Exception:
                skip_flag = 0
            if skip_flag:
                try:
                    # open the DB and clear skip_resume for this run id
                    tmp_conn = sqlite3.connect(db)
                    try:
                        tmp_cur = tmp_conn.cursor()
                        tmp_cur.execute("UPDATE scan_runs SET skip_resume = 0 WHERE id = ?", (run_id,))
                        tmp_conn.commit()
                        # recolor the combo item to default (black)
                        try:
                            brush = QtGui.QBrush(QtGui.QColor('black'))
                            self.resume_runs.setItemData(idx, brush, QtCore.Qt.ForegroundRole)
                            # update stored flag
                            self.resume_runs.setItemData(idx, 0, QtCore.Qt.UserRole + 1)
                        except Exception:
                            pass
                        try:
                            self.resume_output.appendPlainText(f"Cleared skip_resume for run id={run_id}\n")
                        except Exception:
                            pass
                    finally:
                        try:
                            tmp_conn.close()
                        except Exception:
                            pass
                except Exception as e:
                    try:
                        QtWidgets.QMessageBox.warning(self, 'Warning', f'Failed to clear skip_resume flag: {e}')
                    except Exception:
                        pass

            # prepare command: enforce allowed args for resume: --resume <id> and optionally --database
            script_dir = os.path.dirname(os.path.abspath(__file__))
            fscan_py = os.path.join(script_dir, 'fscan.py')
            if not os.path.exists(fscan_py):
                QtWidgets.QMessageBox.critical(self, 'Error', f'Could not find fscan.py at {fscan_py}')
                return
            # prepare base args for resume invoke
            base_args = ['--resume', str(run_id)]
            if db:
                base_args += ['--database', db]

            # If the selected DB exists and lacks the new columns, warn the user about a possible long ALTER
            try:
                if db and not self._confirm_db_will_be_altered(db):
                    return
            except Exception:
                pass

            exe_path = os.path.join(script_dir, 'fscan')
            use_exe = os.path.isfile(exe_path) and os.access(exe_path, os.X_OK)
            if use_exe:
                prog = exe_path
                prog_args = base_args
                start_cmd = ' '.join([prog] + prog_args)
            else:
                prog = sys.executable
                prog_args = ['-u', fscan_py] + base_args
                start_cmd = ' '.join([prog] + prog_args)

            try:
                self.resume_process = QtCore.QProcess(self)
                self.resume_process.setProcessChannelMode(QtCore.QProcess.MergedChannels)
                self.resume_process.readyReadStandardOutput.connect(self._on_resume_stdout)
                self.resume_process.finished.connect(self._on_resume_finished)
                self.resume_output.clear()
                self.resume_output.appendPlainText('Starting: ' + start_cmd + '\n')
                self.resume_process.start(prog, prog_args)
                started = self.resume_process.waitForStarted(500)
                if not started:
                    self.resume_output.appendPlainText('Failed to start resume process.\n')
                    return
                self.resume_run_btn.setEnabled(False)
                self.resume_cancel_btn.setEnabled(True)
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, 'Start error', f'Failed to start resume: {e}')
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, 'Error', f'Error preparing resume: {e}')

    def _on_resume_stdout(self):
        try:
            if not getattr(self, 'resume_process', None):
                return
            ba = self.resume_process.readAllStandardOutput()
            try:
                s = bytes(ba).decode('utf-8', errors='replace')
            except Exception:
                s = str(ba)
            self._append_resume_output(s)
        except Exception:
            pass

    def _confirm_db_will_be_altered(self, db_path):
        """Check whether the `files` table in db_path lacks inode_ref_count or hardlinked.
        If so, prompt the user with a blocking dialog explaining the ALTER may take a long time.
        Returns True to continue, False to cancel.
        """
        try:
            if not db_path:
                return True
            # only warn for existing files (avoid creating new DBs)
            try:
                if not os.path.exists(db_path):
                    return True
            except Exception:
                return True

            try:
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                cur.execute("PRAGMA table_info(files)")
                cols = [r[1] for r in cur.fetchall()]
                try:
                    conn.close()
                except Exception:
                    pass
            except Exception:
                # if we can't read the DB, don't block the operation
                return True

            missing = []
            if 'inode_ref_count' not in cols:
                missing.append('inode_ref_count')
            if 'hardlinked' not in cols:
                missing.append('hardlinked')
            if not missing:
                return True

            msg = (
                f"The database at {db_path} is missing the following columns in table 'files': {', '.join(missing)}.\n\n"
                "The scanner will ALTER the files table to add these columns.\n"
                "On large databases this operation may rewrite the table and can take a long time.\n\n"
                "Do you want to continue?"
            )
            resp = QtWidgets.QMessageBox.question(self, 'Database update required', msg, QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
            return resp == QtWidgets.QMessageBox.Yes
        except Exception:
            # on any unexpected error, allow operation to proceed
            return True

    # NOTE: The background DB-alter worker was removed per user request. The GUI
    # will continue to warn the user via _confirm_db_will_be_altered(), but any
    # ALTER TABLE operations will be performed by the scanner process itself
    # (fscan.py) when it runs init_db().

    def _on_resume_finished(self, exitCode, exitStatus=QtCore.QProcess.NormalExit):
        try:
            self._on_resume_stdout()
        except Exception:
            pass
        try:
            self.resume_run_btn.setEnabled(True)
            self.resume_cancel_btn.setEnabled(False)
            self.resume_output.appendPlainText(f'Process finished with exit code {exitCode}\n')
        except Exception:
            pass

    def _on_cancel_resume(self):
        try:
            if getattr(self, 'resume_process', None) is None:
                return
            try:
                proc = self.resume_process
                pid = None
                try:
                    if callable(getattr(proc, 'processId', None)):
                        pid = proc.processId()
                    else:
                        pid = getattr(proc, 'processId', None) or getattr(proc, 'pid', None)
                except Exception:
                    pid = None
                try:
                    if pid:
                        os.kill(int(pid), signal.SIGINT)
                        proc.waitForFinished(5000)
                    else:
                        proc.terminate()
                        if not proc.waitForFinished(2000):
                            proc.kill()
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            pass

    def _on_scan_stdout(self):
        try:
            if not getattr(self, 'scan_process', None):
                return
            ba = self.scan_process.readAllStandardOutput()
            try:
                s = bytes(ba).decode('utf-8', errors='replace')
            except Exception:
                s = str(ba)
            # If there is an active DB-alter progress dialog, close it when output starts
            try:
                dlg = getattr(self, '_db_alter_dialog', None)
                if dlg and dlg.isVisible():
                    try:
                        dlg.close()
                    except Exception:
                        pass
                    try:
                        self._db_alter_dialog = None
                    except Exception:
                        pass
            except Exception:
                pass
            self._append_scan_output(s)
        except Exception:
            pass

    def _on_scan_finished(self, exitCode, exitStatus=QtCore.QProcess.NormalExit):
        try:
            self._on_scan_stdout()
        except Exception:
            pass
        try:
            self.scan_run_btn.setEnabled(True)
            self.scan_cancel_btn.setEnabled(False)
            self.scan_output.appendPlainText(f'Process finished with exit code {exitCode}\n')
        except Exception:
            pass

    def _on_cancel_scan(self):
        try:
            if getattr(self, 'scan_process', None) is None:
                return
            try:
                proc = self.scan_process
                pid = None
                try:
                    if callable(getattr(proc, 'processId', None)):
                        pid = proc.processId()
                    else:
                        pid = getattr(proc, 'processId', None) or getattr(proc, 'pid', None)
                except Exception:
                    pid = None
                try:
                    if pid:
                        os.kill(int(pid), signal.SIGINT)
                        # allow several seconds for fscan to save state
                        proc.waitForFinished(5000)
                    else:
                        proc.terminate()
                        if not proc.waitForFinished(2000):
                            proc.kill()
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            except Exception:
                pass
        except Exception:
            pass

def resource_path(relative_path):
    """Get the absolute path to resource, works for dev and for PyInstaller"""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def main():
    # support CLI --version for the GUI tool as well
    try:
        if '--version' in sys.argv:
            print(VERSION)
            return
    except Exception:
        pass

    # create QApplication first (needed for message boxes) then attempt to acquire
    # a single-instance lock so only one fsgui runs at a time.
    app = QtWidgets.QApplication(sys.argv)

    # Install signal handlers so Ctrl+C (SIGINT) or SIGTERM will quit the Qt event loop
    def _sig_handler(signum, frame):
        try:
            # polite message to caller
            print("Received signal, shutting down.")
        except Exception:
            pass
        try:
            # If there is a running scan or resume process, try to terminate it first so it can
            # perform its own graceful shutdown (fscan.py will save state on SIGINT/SIGTERM).
            try:
                # 'w' may not yet be defined; guard access
                if 'w' in globals() and w is not None:
                    try:
                        proc = getattr(w, 'scan_process', None)
                        if proc is None:
                            proc = getattr(w, 'resume_process', None)
                            if proc is not None:
                                try:
                                    # Try to send SIGINT to mimic Ctrl+C so the child can perform
                                    # its graceful shutdown and save state.
                                    pid = None
                                    try:
                                        # PyQt5/PySide6 provide processId() in newer versions
                                        if callable(getattr(proc, 'processId', None)):
                                            pid = proc.processId()
                                        else:
                                            pid = getattr(proc, 'processId', None) or getattr(proc, 'pid', None)
                                    except Exception:
                                        pid = None
                                    try:
                                        if pid:
                                            os.kill(int(pid), signal.SIGINT)
                                            # give the child several seconds to flush and save state
                                            proc.waitForFinished(5000)
                                        else:
                                            # fallback to QProcess.terminate()
                                            proc.terminate()
                                            proc.waitForFinished(2000)
                                    except Exception:
                                        try:
                                            proc.kill()
                                        except Exception:
                                            pass
                                except Exception:
                                    pass
                    except Exception:
                        pass
            except Exception:
                pass
            app.quit()
        except Exception:
            try:
                sys.exit(0)
            except Exception:
                pass

    try:
        signal.signal(signal.SIGINT, _sig_handler)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGTERM, _sig_handler)
    except Exception:
        pass

    def _acquire_instance_lock():
        try:
            dh = os.path.join(os.path.expanduser('~'), '.filesage')
            os.makedirs(dh, exist_ok=True)
            lock_path = os.path.join(dh, 'fsgui.lock')
            # open in append mode so file exists and we can write pid
            f = open(lock_path, 'a+')
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                try:
                    f.close()
                except Exception:
                    pass
                return None
            try:
                # write pid for diagnostics
                f.seek(0)
                f.truncate()
                f.write(str(os.getpid()))
                f.flush()
            except Exception:
                pass
            return f
        except Exception:
            return None

    _lock_file = _acquire_instance_lock()
    # Name for the local activation server; include UID to avoid cross-user collisions
    server_name = f"filesage_fsgui_{os.getuid()}"

    if _lock_file is None:
        # Try to notify the running instance to activate (raise) its window via QLocalSocket
        try:
            sock = QLocalSocket()
            sock.connectToServer(server_name)
            if sock.waitForConnected(500):
                try:
                    sock.write(b'ACTIVATE')
                    sock.flush()
                    sock.waitForBytesWritten(500)
                except Exception:
                    pass
                try:
                    sock.close()
                except Exception:
                    pass
                # Inform caller on stdout that we detected a running instance
                try:
                    print("Already running.")
                except Exception:
                    pass
                return
        except Exception as e:
            print(f"Error connecting to existing fileSage GUI instance: {e}", file=sys.stderr)
            pass

        try:
            # also print to stdout so CLI callers can detect this
            try:
                print("Already running.")
            except Exception:
                pass
            QtWidgets.QMessageBox.warning(None, "Already running", "Another instance of fileSage GUI appears to be running.")
        except Exception:
            # last resort: print to stderr
            print("Another instance of fileSage GUI appears to be running.", file=sys.stderr)
        return

    # Primary instance: start a QLocalServer to accept activation requests from secondary instances
    server = None
    try:
        try:
            QLocalServer.removeServer(server_name)
        except Exception:
            pass
        server = QLocalServer()
        def _on_new_connection():
            try:
                s = server.nextPendingConnection()
                if s is None:
                    return
                try:
                    # read any data (not strictly necessary)
                    if s.waitForReadyRead(200):
                        _ = s.readAll()
                except Exception:
                    pass
                try:
                    # bring the main window to front
                    if getattr(w, 'isMinimized', None) and w.isMinimized():
                        w.showNormal()
                    try:
                        w.raise_()
                    except Exception:
                        try:
                            w.raiseWindow()
                        except Exception:
                            pass
                    try:
                        w.activateWindow()
                    except Exception:
                        pass
                except Exception:
                    pass
                try:
                    s.disconnectFromServer()
                    s.close()
                except Exception:
                    pass
            except Exception:
                pass
        server.newConnection.connect(_on_new_connection)
        server.listen(server_name)
    except Exception:
        server = None

    try:
        w = FSCompareGUI()
        w.show()
        ret = app.exec_()
    finally:
        try:
            # release lock by closing file (OS releases flock on close)
            _lock_file.close()
        except Exception:
            pass
        try:
            if server is not None:
                server.close()
                try:
                    QtCore.QLocalServer.removeServer(server_name)
                except Exception:
                    pass
        except Exception:
            pass
    sys.exit(ret)


if __name__ == '__main__':
    main()


