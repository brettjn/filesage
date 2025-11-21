import unittest
import fscan


class CLITest(unittest.TestCase):
    def test_parser_has_help_alias(self):
        parser = fscan.make_parser()
        # ensure -? is registered as an option
        self.assertIn('-?', parser._option_string_actions)

    def test_default_db(self):
        args = fscan.parse_args([])
        self.assertEqual(args.db, './fscan_36.db')

    def test_database_flag(self):
        args = fscan.parse_args(['--database', 'my.db'])
        self.assertEqual(args.db, 'my.db')

    def test_print_log_no_value(self):
        args = fscan.parse_args(['--print-log'])
        self.assertEqual(args.print_log, 'ALL')

    def test_print_log_with_value(self):
        args = fscan.parse_args(['--print-log', '42'])
        self.assertEqual(args.print_log, '42')

    def test_restart_flag(self):
        args = fscan.parse_args(['--restart', '5'])
        self.assertEqual(args.restart, 5)


if __name__ == '__main__':
    unittest.main()
