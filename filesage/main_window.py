# file: filesage/main_window.py
import signal
from PySide6.QtWidgets import QMainWindow, QTabWidget, QWidget, QVBoxLayout, QLabel, QPlainTextEdit, QPushButton
from PySide6.QtGui import QGuiApplication
from PySide6.QtCore import QTimer

class MainWindow(QMainWindow):
    #def pp(self):
    #    self.log("timer....")

    def __init__(self):
        super().__init__()
        self.setWindowTitle("filesage")
        self.resize(800, 600)  # default window size

        # Run python every second
        self.timer = QTimer()
        #self.timer.timeout.connect(self.pp)
        self.timer.timeout.connect(lambda: none)
        self.timer.start(1000)
        #print(self.timer.isActive())

        central = QWidget()
        layout = QVBoxLayout(central)

        tabs = QTabWidget()
        layout.addWidget(tabs, 3)  # give tabs more stretch

        # File INFO tab
        info_tab = QWidget()
        info_layout = QVBoxLayout(info_tab)
        scan_button = QPushButton("Scan Drive")
        scan_button.clicked.connect(self.on_scan_drive)
        info_layout.addWidget(scan_button)
        info_layout.addStretch()
        tabs.addTab(info_tab, "File INFO")

        # File MANIPULATION tab
        manip_tab = QWidget()
        manip_layout = QVBoxLayout(manip_tab)
        manip_layout.addWidget(QLabel("File MANIPULATION"))  # placeholder content
        tabs.addTab(manip_tab, "File MANIPULATION")

        # Log / status area below the tabs
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setPlaceholderText("Status and log messages...")
        layout.addWidget(self.log_view, 1)  # smaller stretch so it stays below tabs

        self.setCentralWidget(central)

        # Setup CTRL+C handler
        signal.signal(signal.SIGINT, self._handle_sigint)

    def on_scan_drive(self) -> None:
        """Handle scan drive button click."""
        self.log("Scan drive initiated...")

    def log(self, message: str) -> None:
        """Append a line to the log view."""
        self.log_view.appendPlainText(message)

    def _handle_sigint(self, signum, frame) -> None:
        """Handle CTRL+C signal."""
        self.log("Interrupt signal received. Exiting...")
        self.close()

    def showEvent(self, event):
        super().showEvent(event)
        screen = self.screen() or QGuiApplication.primaryScreen()
        if screen:
            center = screen.availableGeometry().center()
            frame = self.frameGeometry()
            frame.moveCenter(center)
            self.move(frame.topLeft())

