"""Pytest configuration for tests/integration/.

Ensures the in-tree Python SDK (python/) and the benchmark/ package are
importable regardless of where pytest is invoked from, so the plan's
documented commands work out of the box:

    cd /data/home/tally && python -m pytest tests/integration/ -x -q
"""

from __future__ import annotations

import os
import sys

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_PYTHON_SDK = os.path.join(_PROJECT_ROOT, "python")

for path in (_PROJECT_ROOT, _PYTHON_SDK):
    if path not in sys.path:
        sys.path.insert(0, path)
