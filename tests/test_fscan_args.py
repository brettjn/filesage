import fscan


def test_make_parser_has_license_option():
    parser = fscan.make_parser()
    opts = [o for o in parser._actions if o.option_strings]
    # find option strings
    option_names = [s for o in opts for s in o.option_strings]
    assert '--license' in option_names


def test_parse_args_license_allows_no_root():
    ns = fscan.parse_args(['--license'])
    assert getattr(ns, 'license', False) is True
    # root should be None at parse time
    assert getattr(ns, 'root', None) is None


def test_parse_args_resume_allows_no_root():
    ns = fscan.parse_args(['--resume', '7'])
    assert ns.resume == 7
    assert getattr(ns, 'root', None) is None
