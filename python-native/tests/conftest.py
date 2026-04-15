"""Pytest configuration for Plan 30-01 tests.

These tests live under `python-native/tests/` rather than `python/tests/`
so they don't pick up `python/pyproject.toml` as their rootdir (which would
inject `python/` onto `sys.path` and cause the tests to import the source-
tree `tally/` package instead of the freshly-installed wheel). Running from
`python-native/` ensures `import tally` resolves to the installed extension.
"""

import sys


def pytest_report_header(config):  # noqa: ARG001
    return f"python-native tests — python {sys.version.split()[0]}"
