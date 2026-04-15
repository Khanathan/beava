"""Phase 39-01: unit tests for `tally.fork` — no subprocess, no network.

Covers:
  * Validation happy + every error path.
  * Seed-file content contains the same register-JSON bytes the existing
    SDK's ``_to_register_json`` / ``_collect_registrations`` produce.
  * Port auto-allocation returns a usable port.
  * ``ForkedReplica.stop()`` is idempotent.
  * Timeout path surfaces the stderr tail.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from unittest import mock

import pytest

import tally as tl
from tally import _fork as _fork_mod


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def transactions_stream():
    @tl.stream
    class Transactions:
        user_id: str
        amount: float
    return Transactions


@pytest.fixture
def txn_summary_table(transactions_stream):
    T = transactions_stream

    def _body(t: T) -> tl.Table:
        return t.group_by("user_id").agg(
            count=tl.count(window="1h"),
            total=tl.sum("amount", window="1h"),
        )

    _body.__name__ = "txn_summary"
    return tl.table(key="user_id")(_body)


@pytest.fixture
def fake_binary(tmp_path):
    """A do-nothing executable to satisfy ``_resolve_binary``."""
    p = tmp_path / "tally"
    p.write_text("#!/bin/sh\nexit 0\n")
    p.chmod(0o755)
    return p


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_happy_path(self, transactions_stream):
        names, token = _fork_mod._validate_fork_args(
            remote="127.0.0.1:6400",
            streams=[transactions_stream],
            keys=["u1", "u2"],
            key_prefix=None,
            token="t",
            pipelines=None,
            ready_timeout=30.0,
        )
        assert names == ["Transactions"]
        assert token == "t"

    def test_empty_streams_rejected(self):
        with pytest.raises(tl.ForkValidationError, match="non-empty"):
            _fork_mod._validate_fork_args(
                remote="127.0.0.1:6400",
                streams=[],
                keys=None,
                key_prefix=None,
                token="t",
                pipelines=None,
                ready_timeout=30.0,
            )

    def test_bad_remote_rejected(self, transactions_stream):
        with pytest.raises(tl.ForkValidationError, match="HOST:PORT"):
            _fork_mod._validate_fork_args(
                remote="no-port",
                streams=[transactions_stream],
                keys=None,
                key_prefix=None,
                token="t",
                pipelines=None,
                ready_timeout=30.0,
            )

    def test_non_stream_rejected(self):
        with pytest.raises(tl.ForkValidationError, match="@tl.stream"):
            _fork_mod._validate_fork_args(
                remote="h:1",
                streams=["Transactions"],
                keys=None,
                key_prefix=None,
                token="t",
                pipelines=None,
                ready_timeout=30.0,
            )

    def test_duplicate_stream_rejected(self, transactions_stream):
        with pytest.raises(tl.ForkValidationError, match="duplicate"):
            _fork_mod._validate_fork_args(
                remote="h:1",
                streams=[transactions_stream, transactions_stream],
                keys=None,
                key_prefix=None,
                token="t",
                pipelines=None,
                ready_timeout=30.0,
            )

    def test_keys_and_key_prefix_mutex(self, transactions_stream):
        with pytest.raises(tl.ForkValidationError, match="mutually exclusive"):
            _fork_mod._validate_fork_args(
                remote="h:1",
                streams=[transactions_stream],
                keys=["u1"],
                key_prefix="u",
                token="t",
                pipelines=None,
                ready_timeout=30.0,
            )

    def test_empty_keys_rejected(self, transactions_stream):
        with pytest.raises(tl.ForkValidationError, match="non-empty"):
            _fork_mod._validate_fork_args(
                remote="h:1",
                streams=[transactions_stream],
                keys=[],
                key_prefix=None,
                token="t",
                pipelines=None,
                ready_timeout=30.0,
            )

    def test_pipelines_must_be_registerable(self, transactions_stream):
        class Bogus:
            pass
        with pytest.raises(tl.ForkValidationError, match="registerable"):
            _fork_mod._validate_fork_args(
                remote="h:1",
                streams=[transactions_stream],
                keys=None,
                key_prefix=None,
                token="t",
                pipelines=[Bogus()],
                ready_timeout=30.0,
            )

    def test_missing_token_raises(self, transactions_stream, monkeypatch):
        monkeypatch.delenv("TALLY_REPLICA_TOKEN", raising=False)
        with pytest.raises(tl.ForkValidationError, match="token required"):
            _fork_mod._validate_fork_args(
                remote="h:1",
                streams=[transactions_stream],
                keys=None,
                key_prefix=None,
                token=None,
                pipelines=None,
                ready_timeout=30.0,
            )

    def test_token_from_env(self, transactions_stream, monkeypatch):
        monkeypatch.setenv("TALLY_REPLICA_TOKEN", "from-env")
        _names, token = _fork_mod._validate_fork_args(
            remote="h:1",
            streams=[transactions_stream],
            keys=None,
            key_prefix=None,
            token=None,
            pipelines=None,
            ready_timeout=30.0,
        )
        assert token == "from-env"

    def test_bad_timeout(self, transactions_stream):
        with pytest.raises(tl.ForkValidationError, match="ready_timeout"):
            _fork_mod._validate_fork_args(
                remote="h:1",
                streams=[transactions_stream],
                keys=None,
                key_prefix=None,
                token="t",
                pipelines=None,
                ready_timeout=0,
            )


# ---------------------------------------------------------------------------
# Seed-file serialisation
# ---------------------------------------------------------------------------


class TestSeedBundle:
    def test_stream_only(self, transactions_stream):
        bundle = _fork_mod._build_register_bundle([transactions_stream], None)
        # One register frame per descriptor, deduped by name.
        assert [b["name"] for b in bundle] == ["Transactions"]
        # Bytes match what App.register would send.
        expected = transactions_stream._to_register_json()
        assert bundle[0] == expected

    def test_stream_plus_aggregation_includes_both(
        self, transactions_stream, txn_summary_table
    ):
        bundle = _fork_mod._build_register_bundle(
            [transactions_stream], [txn_summary_table]
        )
        names = [b["name"] for b in bundle]
        # Aggregation upstream (Transactions) must appear before the agg
        # itself — same contract App.register relies on.
        assert "Transactions" in names
        assert "txn_summary" in names
        assert names.index("Transactions") < names.index("txn_summary")

    def test_seed_file_is_json_array(self, transactions_stream, txn_summary_table):
        bundle = _fork_mod._build_register_bundle(
            [transactions_stream], [txn_summary_table]
        )
        path = _fork_mod._write_seed_file(bundle)
        try:
            doc = json.loads(path.read_text())
            assert isinstance(doc, list)
            assert len(doc) == len(bundle)
            for lhs, rhs in zip(doc, bundle):
                assert lhs == rhs
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Port allocation
# ---------------------------------------------------------------------------


class TestPortAllocation:
    def test_picks_usable_port(self):
        p = _fork_mod._pick_free_port()
        assert isinstance(p, int) and 1024 < p < 65535
        # Should be bindable.
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", p))
        finally:
            s.close()


# ---------------------------------------------------------------------------
# Binary resolution
# ---------------------------------------------------------------------------


class TestBinaryResolution:
    def test_explicit_path(self, fake_binary):
        assert _fork_mod._resolve_binary(str(fake_binary)) == str(fake_binary)

    def test_missing_path_rejected(self, tmp_path):
        with pytest.raises(tl.ForkValidationError):
            _fork_mod._resolve_binary(str(tmp_path / "does-not-exist"))


# ---------------------------------------------------------------------------
# ForkedReplica lifecycle (without actually running the fork binary)
# ---------------------------------------------------------------------------


class _DoneProc:
    """Stand-in for a ``subprocess.Popen`` with configurable exit state."""

    def __init__(self, alive: bool = True, returncode: int = 0) -> None:
        self._alive = alive
        self._returncode = returncode
        self.signals: list[int] = []
        self.killed = False
        self.stdout = None
        self.stderr = None
        self.pid = 4242

    def poll(self):
        return None if self._alive else self._returncode

    def send_signal(self, sig):
        self.signals.append(sig)
        self._alive = False
        self._returncode = 0

    def wait(self, timeout=None):
        return self._returncode

    def kill(self):
        self.killed = True
        self._alive = False
        self._returncode = -9


def _make_replica(proc=None, seed=None, out=None, err=None):
    proc = proc or _DoneProc()
    return _fork_mod.ForkedReplica(
        proc=proc,
        local_port=9999,
        token="t",
        seed_file=seed,
        stdout_log=out,
        stderr_log=err,
        streams=[],
        pipelines=[],
    )


class TestStopIdempotent:
    def test_stop_twice_safe(self):
        r = _make_replica()
        r.stop()
        r.stop()  # must not raise
        assert r._stopped

    def test_stop_cleans_up_temp_files(self, tmp_path):
        seed = tmp_path / "seed.json"
        seed.write_text("[]")
        stdout_log = tmp_path / "stdout.log"
        stdout_log.write_text("")
        stderr_log = tmp_path / "stderr.log"
        stderr_log.write_text("")
        r = _make_replica(seed=seed, out=stdout_log, err=stderr_log)
        r.stop()
        assert not seed.exists()
        assert not stdout_log.exists()
        assert not stderr_log.exists()

    def test_context_manager_stops(self):
        r = _make_replica()
        with r:
            pass
        assert r._stopped


# ---------------------------------------------------------------------------
# Readiness polling — use a tiny in-process HTTP server
# ---------------------------------------------------------------------------


class _ReadyHandler(BaseHTTPRequestHandler):
    ready = False

    def do_GET(self):
        if self.path == "/debug/ready" and type(self).ready:
            body = json.dumps({"ready": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(503)
            self.end_headers()

    def log_message(self, *a, **kw):
        pass


class _AliveProc:
    def __init__(self):
        self.stdout = self.stderr = None
        self.pid = 1234
    def poll(self):
        return None


class TestReadyPolling:
    def test_ready_returns_quickly(self, tmp_path):
        handler = type("H", (_ReadyHandler,), {"ready": True})
        srv = HTTPServer(("127.0.0.1", 0), handler)
        port = srv.server_address[1]
        t = threading.Thread(target=srv.serve_forever, daemon=True)
        t.start()
        try:
            _fork_mod._poll_ready(port, 5.0, _AliveProc(), None)
        finally:
            srv.shutdown()
            srv.server_close()

    def test_timeout_surfaces_stderr_tail(self, tmp_path):
        stderr_log = tmp_path / "err.log"
        stderr_log.write_text(
            "\n".join(f"line-{i}" for i in range(70)) + "\n"
        )
        # Pick a port nothing is listening on.
        port = _fork_mod._pick_free_port()
        with pytest.raises(tl.ForkTimeoutError) as exc:
            _fork_mod._poll_ready(port, 0.5, _AliveProc(), stderr_log)
        msg = str(exc.value)
        assert "did not return 200" in msg
        assert "line-69" in msg  # tail included
        # Only last 50 lines — line-0 should be truncated out.
        assert "line-0\n" not in msg

    def test_early_exit_raises_subprocess_error(self, tmp_path):
        stderr_log = tmp_path / "err.log"
        stderr_log.write_text("boom\nfatal panic\n")
        class Exited:
            stdout = stderr = None
            pid = 1
            def poll(self):
                return 2
        port = _fork_mod._pick_free_port()
        with pytest.raises(tl.ForkSubprocessError, match="exited early"):
            _fork_mod._poll_ready(port, 2.0, Exited(), stderr_log)


# ---------------------------------------------------------------------------
# End-to-end spawn path with full subprocess.Popen mock
# ---------------------------------------------------------------------------


class TestForkSpawnMocked:
    def test_builds_correct_argv(
        self, transactions_stream, txn_summary_table, fake_binary, monkeypatch
    ):
        captured = {}

        class FakePopen:
            def __init__(self, argv, env=None, stdout=None, stderr=None):
                captured["argv"] = argv
                captured["env"] = env
                self.stdout = stdout
                self.stderr = stderr
                self.pid = 777
            def poll(self):
                return None
            def send_signal(self, sig):
                pass
            def wait(self, timeout=None):
                return 0
            def kill(self):
                pass

        # Patch Popen and _poll_ready to no-ops.
        monkeypatch.setattr(_fork_mod.subprocess, "Popen", FakePopen)
        monkeypatch.setattr(_fork_mod, "_poll_ready", lambda *a, **kw: None)

        replica = tl.fork(
            remote="prod.example:6400",
            streams=[transactions_stream],
            keys=["u1", "u2"],
            token="scientist-tok",
            pipelines=[txn_summary_table],
            local_port=17000,
            binary_path=str(fake_binary),
            ready_timeout=5.0,
        )

        try:
            argv = captured["argv"]
            assert argv[0] == str(fake_binary)
            assert argv[1] == "fork"
            # Core flags present.
            assert "--remote" in argv and "prod.example:6400" in argv
            assert "--streams" in argv and "Transactions" in argv
            assert "--token" in argv and "scientist-tok" in argv
            assert "--local-port" in argv and "17000" in argv
            assert "--keys" in argv and "u1,u2" in argv
            assert "--pipeline-file" in argv
            pf_idx = argv.index("--pipeline-file") + 1
            pf_path = Path(argv[pf_idx])
            # Seed file was written with the register bundle.
            doc = json.loads(pf_path.read_text())
            names = [b["name"] for b in doc]
            assert "Transactions" in names
            assert "txn_summary" in names
            # local_url property reflects the port.
            assert replica.local_url == "http://127.0.0.1:17000"
            assert replica.local_port == 17000
        finally:
            replica.stop()

    def test_key_prefix_flag(
        self, transactions_stream, fake_binary, monkeypatch
    ):
        captured = {}

        class FakePopen:
            def __init__(self, argv, env=None, stdout=None, stderr=None):
                captured["argv"] = argv
                self.stdout = stdout
                self.stderr = stderr
                self.pid = 7
            def poll(self):
                return None
            def send_signal(self, sig):
                pass
            def wait(self, timeout=None):
                return 0
            def kill(self):
                pass

        monkeypatch.setattr(_fork_mod.subprocess, "Popen", FakePopen)
        monkeypatch.setattr(_fork_mod, "_poll_ready", lambda *a, **kw: None)

        replica = tl.fork(
            remote="h:1",
            streams=[transactions_stream],
            key_prefix="user-",
            token="t",
            local_port=18000,
            binary_path=str(fake_binary),
        )
        try:
            argv = captured["argv"]
            assert "--key-prefix" in argv and "user-" in argv
            assert "--keys" not in argv
        finally:
            replica.stop()
