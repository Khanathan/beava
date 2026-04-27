"""Phase 19 Plan 03 — Smoke tests for the Python multi-process blast harness.

Three tests covering:

  1. ``test_blast_total_events_1000_zipfian_msgpack`` — End-to-end smoke that
     spawns ``python/benches/blast.py`` against a real beava server fixture
     (HTTP+TCP URLs). With ``--total-events 1000 --blast-shape zipfian
     --transport tcp --wire-format msgpack --pipeline small`` the harness must
     exit cleanly within 30 seconds and emit the canonical invariant tuple
     ``requested=1000 pushed=1000 acked=1000`` plus ``wall_clock_ms=``,
     ``send_drain_ms=`` and ``ack_lag_ms=`` columns. Mirrors the Rust
     harness output format so Plan 19-05 can grep both transcripts uniformly.

  2. ``test_blast_legacy_path_help_text`` — ``--help`` must print every CLI
     flag named in CONTEXT.md D-01..D-15 (--total-events, --blast-shape,
     --transport, --wire-format, --pipeline, --parallel, --pipeline-depth,
     --isolation-mode, --zipf-alpha, --cardinality, --mixed-event-count).

  3. ``test_blast_pyproject_excludes_benches`` — D-08: ``python/pyproject.toml``
     contains ``[tool.hatch.build.targets.wheel]`` AND an ``exclude`` rule that
     mentions ``benches`` — keeps the harness out of ``pip install beava``.

All three tests are RED at this commit (blast.py is not yet created and the
pyproject ``exclude`` rule has not yet been added).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
PYTHON_DIR = REPO_ROOT / "python"
BENCH_DIR = PYTHON_DIR / "benches"
BLAST_PY = BENCH_DIR / "blast.py"


@pytest.mark.phase19
def test_blast_total_events_1000_zipfian_msgpack(
    beava_server: tuple[str, str],
) -> None:
    """End-to-end smoke: blast.py pushes 1000 events with zipfian/msgpack/tcp.

    Uses the session ``beava_server`` fixture from conftest.py which spawns the
    real ``beava`` binary on ephemeral HTTP+TCP ports. Passes both URLs to
    ``--server-url http://...,tcp://...`` so blast.py registers via HTTP and
    pushes via TCP.

    Asserts:
      - exit code 0 within 30s
      - stdout/stderr contains ``requested=1000``, ``pushed=1000``, ``acked=1000``
      - contains ``wall_clock_ms=``, ``send_drain_ms=``, ``ack_lag_ms=``
      - does NOT contain ``Traceback``
    """
    if not BLAST_PY.is_file():
        pytest.fail(f"blast.py not yet created at {BLAST_PY}")

    http_url, tcp_url = beava_server
    server_url = f"{http_url},{tcp_url}"

    env = os.environ.copy()
    # Make the harness importable as `benches.*` from python/ directory.
    env.setdefault("PYTHONPATH", str(PYTHON_DIR))

    result = subprocess.run(
        [
            sys.executable,
            str(BLAST_PY),
            "--total-events", "1000",
            "--blast-shape", "zipfian",
            "--transport", "tcp",
            "--wire-format", "msgpack",
            "--pipeline", "small",
            "--parallel", "2",
            "--pipeline-depth", "8",
            "--no-ledger",
            "--isolation-mode",
            "--zipf-alpha", "1.0",
            "--cardinality", "100",
            "--server-url", server_url,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )
    combined = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        f"blast.py exited with {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    assert "requested=1000" in combined, f"missing requested=1000 in:\n{combined}"
    assert "pushed=1000" in combined, f"missing pushed=1000 in:\n{combined}"
    assert "acked=1000" in combined, f"missing acked=1000 in:\n{combined}"
    assert "wall_clock_ms=" in combined, f"missing wall_clock_ms= in:\n{combined}"
    assert "send_drain_ms=" in combined, f"missing send_drain_ms= in:\n{combined}"
    assert "ack_lag_ms=" in combined, f"missing ack_lag_ms= in:\n{combined}"
    assert "Traceback" not in combined, f"unexpected Traceback in:\n{combined}"


@pytest.mark.phase19
def test_blast_legacy_path_help_text() -> None:
    """``python python/benches/blast.py --help`` prints every CONTEXT.md CLI flag."""
    if not BLAST_PY.is_file():
        pytest.fail(f"blast.py not yet created at {BLAST_PY}")

    env = os.environ.copy()
    env.setdefault("PYTHONPATH", str(PYTHON_DIR))

    result = subprocess.run(
        [sys.executable, str(BLAST_PY), "--help"],
        capture_output=True,
        text=True,
        timeout=15,
        env=env,
    )
    combined = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, f"--help exited with {result.returncode}\n{combined}"

    # Every CONTEXT.md D-01..D-15 flag must appear in the help text.
    required_flags = [
        "--total-events",
        "--blast-shape",
        "--transport",
        "--wire-format",
        "--pipeline",
        "--parallel",
        "--pipeline-depth",
        "--isolation-mode",
        "--zipf-alpha",
        "--cardinality",
        "--mixed-event-count",
    ]
    for flag in required_flags:
        assert flag in combined, f"missing {flag} in --help output:\n{combined}"


@pytest.mark.phase19
def test_blast_pyproject_excludes_benches() -> None:
    """D-08: ``[tool.hatch.build.targets.wheel]`` excludes ``benches/`` from wheel."""
    pyproject_path = PYTHON_DIR / "pyproject.toml"
    pyproject = pyproject_path.read_text()

    assert "[tool.hatch.build.targets.wheel]" in pyproject, (
        f"missing [tool.hatch.build.targets.wheel] block in {pyproject_path}"
    )

    # The exclude rule must appear AND must mention benches/. The plan's
    # acceptance criteria check `grep -A3 "build.targets.wheel" pyproject.toml |
    # grep -c "benches"`; we mirror that with a substring search across the file.
    assert "benches" in pyproject, (
        f"pyproject.toml does not mention 'benches' (D-08 exclude rule missing); "
        f"see {pyproject_path}"
    )

    # Tighter check: the [tool.hatch.build.targets.wheel] block specifically must
    # contain an `exclude` directive AND mention `benches`.
    block_start = pyproject.index("[tool.hatch.build.targets.wheel]")
    next_section = pyproject.find("\n[", block_start + 1)
    block = pyproject[block_start : next_section if next_section != -1 else len(pyproject)]
    assert "exclude" in block, (
        f"no 'exclude' directive inside [tool.hatch.build.targets.wheel] block:\n{block}"
    )
    assert "benches" in block, (
        f"'benches' not mentioned in [tool.hatch.build.targets.wheel] block:\n{block}"
    )
