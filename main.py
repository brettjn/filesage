#!/usr/bin/env python3

# file: filesage/main.py

import sys
from PySide6.QtWidgets import QApplication
from filesage.main_window import MainWindow

def main() -> int:
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    return app.exec()

if __name__ == "__main__":
    raise SystemExit(main())

# file: filesage/__main__.py
from .main import main

if __name__ == "__main__":
    raise SystemExit(main())
