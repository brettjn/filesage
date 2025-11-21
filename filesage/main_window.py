# python
# file: `filesage/main_window.py`
import os
import shutil
import signal
import subprocess
from typing import Set

from PySide6.QtWidgets import (
    QMainWindow,
    QTabWidget,
    QWidget,
    QVBoxLayout,
    QLabel,
    QPlainTextEdit,
    QPushButton,
)
from PySide6.QtGui import QGuiApplication
from PySide6.QtCore import QTimer, QCoreApplication, QThread, Signal


class SmartctlWorker(QThread):
    finished = Signal(str)
    error = Signal(str)

    def run(self) -> None:
        script_path = None
        try:
            # choose elevation method
            if os.geteuid() == 0:
                elev = []
            elif shutil.which("pkexec"):
                elev = ["pkexec"]
            else:
                elev = ["sudo"]

            shell_script = (
                "#!/bin/sh\n"
                # removed `set -e` so one failing device doesn't abort the whole script
                "smartctl --scan | awk '/^\\/dev/{print $1}' | while read dev; do\n"
                "  printf '%s\\n' '============================================================'\n"
                "  printf '%s\\n' \"$dev\"\n"
                "  printf '%s\\n' '============================================================'\n"
                # run smartctl; if it fails, print an error but continue with the next device
                "  smartctl -a \"$dev\" 2>&1 || printf 'smartctl failed for %s\\n' \"$dev\" 1>&2\n"
                "done\n"
            )

            import tempfile
            with tempfile.NamedTemporaryFile("w", delete=False, prefix="filesage_smartctl_", suffix=".sh") as tf:
                tf.write(shell_script)
                script_path = tf.name

            os.chmod(script_path, 0o700)

            # Run the script once under elevation so the user authenticates a single time
            proc = subprocess.run(elev + [script_path], capture_output=True, text=True)
            output = (proc.stdout or "") + (proc.stderr or "")

            if proc.returncode != 0 and not output:
                raise RuntimeError("elevated smartctl run failed")

            # emit result (custom signal)
            self.finished.emit(output)

        except Exception as exc:
            self.error.emit(str(exc))

        finally:
            if script_path:
                try:
                    os.remove(script_path)
                except Exception:
                    pass

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("filesage")
        self.resize(800, 600)

        # Periodic timer to keep Python signal handling responsive.
        self.timer = QTimer()
        self.timer.timeout.connect(lambda: None)
        self.timer.start(200)

        central = QWidget()
        layout = QVBoxLayout(central)

        tabs = QTabWidget()
        layout.addWidget(tabs, 3)

        info_tab = QWidget()
        info_layout = QVBoxLayout(info_tab)
        self.scan_button = QPushButton("Scan Drive")
        info_layout.addWidget(self.scan_button)
        self.scan_button.clicked.connect(self.on_scan_drive)

        self.smartctl_result = QPlainTextEdit()
        self.smartctl_result.setReadOnly(True)
        self.smartctl_result.setVisible(False)
        # make this widget take all remaining space in the tab
        info_layout.addWidget(self.smartctl_result, 1)

        info_layout.addStretch()
        tabs.addTab(info_tab, "File INFO")

        manip_tab = QWidget()
        manip_layout = QVBoxLayout(manip_tab)
        manip_layout.addWidget(QLabel("File MANIPULATION"))
        tabs.addTab(manip_tab, "File MANIPULATION")

        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setPlaceholderText("Status and log messages...")
        layout.addWidget(self.log_view, 1)

        self.setCentralWidget(central)

        signal.signal(signal.SIGINT, self._handle_sigint)

        self.worker: SmartctlWorker | None = None

    def on_scan_drive(self) -> None:
        self.log("Starting elevated smartctl scan...")
        self.scan_button.setEnabled(False)

        self.worker = SmartctlWorker()
        self.worker.finished.connect(self.on_scan_complete)
        self.worker.error.connect(self.on_scan_error)
        # cleanup after either finished or error
        self.worker.finished.connect(self._cleanup_worker)
        self.worker.error.connect(self._cleanup_worker)
        self.worker.start()

    def on_scan_complete(self, output: str) -> None:
        self.scan_button.setVisible(False)
        self.smartctl_result.setPlainText(output)
        self.smartctl_result.setVisible(True)
        self.log("Drive scan completed.")

    def on_scan_error(self, error: str) -> None:
        self.log(f"Scan error: {error}")
        self.scan_button.setEnabled(True)

    def _cleanup_worker(self, *_args) -> None:
        # ensure thread has stopped and then delete the QObject
        if not self.worker:
            return
        if self.worker.isRunning():
            # try to wait a short time for graceful stop
            self.worker.wait(500)
        try:
            self.worker.deleteLater()
        finally:
            self.worker = None

    def closeEvent(self, event) -> None:
        # Ensure worker is stopped before window is destroyed to avoid
        # "QThread: Destroyed while thread '' is still running"
        if self.worker and self.worker.isRunning():
            self.log("Stopping worker thread...")
            # best-effort safe stop: try terminate (subprocess.run is blocking)
            try:
                self.worker.terminate()
            except Exception:
                pass
            # wait up to 3s, then block until finished if necessary
            if not self.worker.wait(3000):
                self.log("Worker did not stop in time; waiting until it finishes...")
                self.worker.wait()
            # cleanup
            try:
                self.worker.deleteLater()
            except Exception:
                pass
            self.worker = None
        super().closeEvent(event)

    def log(self, message: str) -> None:
        self.log_view.appendPlainText(message)

    def _handle_sigint(self, signum, frame) -> None:
        self.log("Interrupt signal received. Exiting...")
        QCoreApplication.quit()

    def showEvent(self, event):
        super().showEvent(event)
        screen = self.screen() or QGuiApplication.primaryScreen()
        if screen:
            center = screen.availableGeometry().center()
            frame = self.frameGeometry()
            frame.moveCenter(center)
            self.move(frame.topLeft())
