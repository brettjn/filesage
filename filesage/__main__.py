# filesage/__main__.py
import sys
from PySide6.QtWidgets import QApplication
from .main_window import MainWindow

def main(argv=None):
    app = QApplication(argv or sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
