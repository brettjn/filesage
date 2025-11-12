# main_window.py
from PySide6.QtWidgets import QMainWindow, QTabWidget, QWidget, QVBoxLayout, QLabel
from PySide6.QtGui import QGuiApplication

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("filesage")
        self.resize(800, 600)  # default window size

        central = QWidget()
        layout = QVBoxLayout(central)

        tabs = QTabWidget()
        layout.addWidget(tabs)

        # File INFO tab
        info_tab = QWidget()
        info_layout = QVBoxLayout(info_tab)
        info_layout.addWidget(QLabel("File INFO"))  # placeholder content
        tabs.addTab(info_tab, "File INFO")

        # File MANIPULATION tab
        manip_tab = QWidget()
        manip_layout = QVBoxLayout(manip_tab)
        manip_layout.addWidget(QLabel("File MANIPULATION"))  # placeholder content
        tabs.addTab(manip_tab, "File MANIPULATION")

        self.setCentralWidget(central)

    def showEvent(self, event):
        super().showEvent(event)
        screen = self.screen() or QGuiApplication.primaryScreen()
        if screen:
            center = screen.availableGeometry().center()
            frame = self.frameGeometry()
            frame.moveCenter(center)
            self.move(frame.topLeft())
