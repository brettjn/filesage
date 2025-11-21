# Ensure project root is on sys.path so tests can import top-level modules
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
