import os
import sys
import subprocess
import tempfile
import unittest


class PrintLogValidationTest(unittest.TestCase):
    def setUp(self):
        self.fscan = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'fscan.py'))

    def run_fscan(self, args):
        cmd = [sys.executable, self.fscan] + args
        return subprocess.run(cmd, capture_output=True, text=True)

    def test_print_log_invalid_combination(self):
        # --print-log together with other unrelated flags (like --silent) should be rejected
        r = self.run_fscan(['--print-log', '--silent'])
        self.assertNotEqual(r.returncode, 0)
        self.assertIn('When using --print-log', r.stderr)

    def test_print_log_with_database_ok(self):
        # --print-log combined with --database should be allowed
        with tempfile.NamedTemporaryFile() as tf:
            r = self.run_fscan(['--print-log', '--database', tf.name])
            # should exit successfully (prints no scan_runs)
            self.assertEqual(r.returncode, 0)
            self.assertIn('No scan_runs found', r.stdout)


if __name__ == '__main__':
    unittest.main()
