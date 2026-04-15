"""Plan 30-02 — end-to-end coverage for `tally.Pipeline` and
`tally_cli query / inspect` against a real tally server.

Both surfaces share the same run_clone + FrozenClient backend, so this
suite exercises them side-by-side. The fixtures live in `conftest.py`.

Key test matrix
---------------

Python (`tally.Pipeline`):
  - `.run()` completes without error against a seeded server.
  - `.get(in_scope_key, stream)` returns a value (may be `None` for
    in-scope-but-absent entities; for seeded-in-snapshot keys it is a
    dict).
  - `.get(out_of_scope_key, stream)` raises `OutOfScopeError`.
  - `.inspect()` returns `{stream: key_count}`.

CLI (`tally_cli`):
  - `query --key K --stream S` with in-scope key → rc=0, stdout is valid
    JSON.
  - `query --key OOS --stream S` with out-of-scope key → rc != 0, stderr
    contains "OutOfScope".
  - `inspect --streams S --keys ...` → rc=0, stdout is JSON
    `{"Transactions": <count>}`.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from tally import OutOfScopeError, Pipeline


class TestPipelinePython:
    @pytest.mark.timeout(60)
    def test_run_does_not_raise(self, seeded_server):
        pipe = Pipeline(
            remote=seeded_server.remote,
            streams=seeded_server.streams,
            keys=seeded_server.in_scope_keys,
            token=seeded_server.token,
            mode="historical",
        )
        pipe.run()  # must not raise

    @pytest.mark.timeout(60)
    def test_get_in_scope_returns_state_or_none(self, seeded_server):
        pipe = Pipeline(
            remote=seeded_server.remote,
            streams=seeded_server.streams,
            keys=seeded_server.in_scope_keys,
            token=seeded_server.token,
        )
        pipe.run()
        # Both u1 and u2 were in the seeded snapshot, so we expect a
        # non-None state. The exact shape is internal, but it must be a
        # dict with a `streams` field (SerializableEntityState layout).
        v1 = pipe.get("u1", stream="Transactions")
        assert v1 is not None, "u1 should be present in snapshot"
        assert isinstance(v1, dict)
        assert "streams" in v1

        v2 = pipe.get("u2", stream="Transactions")
        assert v2 is not None, "u2 should be present in snapshot"

    @pytest.mark.timeout(60)
    def test_out_of_scope_get_raises_typed_error(self, seeded_server):
        """OutOfScopeError fires when the lookup key is outside scope."""
        pipe = Pipeline(
            remote=seeded_server.remote,
            streams=seeded_server.streams,
            keys=seeded_server.in_scope_keys,  # u1, u2 — u3 is excluded
            token=seeded_server.token,
        )
        pipe.run()
        oos_key = seeded_server.out_of_scope_keys[0]  # "u3"
        with pytest.raises(OutOfScopeError):
            pipe.get(oos_key, stream="Transactions")

    @pytest.mark.timeout(60)
    def test_out_of_scope_stream_raises(self, seeded_server):
        """Out-of-scope *stream* (not key) also raises OutOfScopeError."""
        pipe = Pipeline(
            remote=seeded_server.remote,
            streams=seeded_server.streams,
            keys=seeded_server.in_scope_keys,
            token=seeded_server.token,
        )
        pipe.run()
        with pytest.raises(OutOfScopeError):
            pipe.get("u1", stream="NotInScope")

    @pytest.mark.timeout(60)
    def test_inspect_returns_counts(self, seeded_server):
        pipe = Pipeline(
            remote=seeded_server.remote,
            streams=seeded_server.streams,
            keys=seeded_server.in_scope_keys,
            token=seeded_server.token,
        )
        pipe.run()
        counts = pipe.inspect()
        assert isinstance(counts, dict)
        # Server-side scope filter should drop u3; the server loads u1+u2,
        # each of which appears in the Transactions stream once.
        assert counts.get("Transactions") == 2, f"got: {counts!r}"


class TestCLIQuery:
    @pytest.mark.timeout(60)
    def test_query_in_scope_prints_json(self, seeded_server, tally_cli_bin):
        result = subprocess.run(
            [
                str(tally_cli_bin), "query",
                "--remote", seeded_server.remote,
                "--streams", ",".join(seeded_server.streams),
                "--keys", ",".join(seeded_server.in_scope_keys),
                "--token", seeded_server.token,
                "--key", "u1",
                "--stream", "Transactions",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"rc={result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        # Must be valid JSON (could be `null` for in-scope-but-absent, or
        # an object for a present entity — u1 is seeded, so we expect an
        # object).
        parsed = json.loads(result.stdout)
        assert isinstance(parsed, dict), f"got: {parsed!r}"

    @pytest.mark.timeout(60)
    def test_query_out_of_scope_exits_nonzero_with_marker(
        self, seeded_server, tally_cli_bin
    ):
        """T-30-07: CLI surfaces OutOfScope via non-zero exit + stderr."""
        oos_key = seeded_server.out_of_scope_keys[0]
        result = subprocess.run(
            [
                str(tally_cli_bin), "query",
                "--remote", seeded_server.remote,
                "--streams", ",".join(seeded_server.streams),
                "--keys", ",".join(seeded_server.in_scope_keys),
                "--token", seeded_server.token,
                "--key", oos_key,
                "--stream", "Transactions",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode != 0, (
            f"expected non-zero exit, got {result.returncode}\nstdout: {result.stdout}"
        )
        assert "OutOfScope" in result.stderr, (
            f"stderr missing OutOfScope marker: {result.stderr!r}"
        )


class TestCLIInspect:
    @pytest.mark.timeout(60)
    def test_inspect_prints_counts_json(self, seeded_server, tally_cli_bin):
        result = subprocess.run(
            [
                str(tally_cli_bin), "inspect",
                "--remote", seeded_server.remote,
                "--streams", ",".join(seeded_server.streams),
                "--keys", ",".join(seeded_server.in_scope_keys),
                "--token", seeded_server.token,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"rc={result.returncode}\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        parsed = json.loads(result.stdout)
        # JSON-compared (not byte-compared) to avoid key-ordering flakes.
        assert parsed == {"Transactions": 2}, f"got: {parsed!r}"

    @pytest.mark.timeout(60)
    def test_inspect_empty_scope_still_lists_stream(
        self, seeded_server, tally_cli_bin
    ):
        """A declared stream with zero in-scope keys still shows as 0 —
        not missing from the dict."""
        result = subprocess.run(
            [
                str(tally_cli_bin), "inspect",
                "--remote", seeded_server.remote,
                "--streams", ",".join(seeded_server.streams),
                "--keys", "nonexistent_key",
                "--token", seeded_server.token,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
        parsed = json.loads(result.stdout)
        assert parsed == {"Transactions": 0}, f"got: {parsed!r}"
