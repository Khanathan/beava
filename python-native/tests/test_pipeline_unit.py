"""Plan 30-01: Pipeline constructor-validation unit tests.

These tests import from the top-level `tally` package, which re-exports
`Pipeline` from the native `_native` submodule. If the native extension
isn't installed (pure-Python hatch build), the module-level `Pipeline`
is `None` and we skip the whole file.
"""

from __future__ import annotations

import os

import pytest

import tally

pytestmark = pytest.mark.skipif(
    not getattr(tally, "_HAS_NATIVE", False),
    reason="tally._native not installed (pure-Python hatch build path)",
)


def test_constructs_with_keys() -> None:
    p = tally.Pipeline(
        remote="host:6400",
        streams=["Transactions"],
        keys=["u1"],
        mode="historical",
    )
    assert p is not None


def test_constructs_with_key_prefix() -> None:
    p = tally.Pipeline(
        remote="host:6400",
        streams=["Transactions"],
        key_prefix="u_",
        mode="historical",
    )
    assert p is not None


def test_constructs_stream_only_scope() -> None:
    p = tally.Pipeline(remote="host:6400", streams=["Transactions"])
    assert p is not None


def test_empty_streams_rejected() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        tally.Pipeline(remote="host:6400", streams=[])


def test_keys_and_key_prefix_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        tally.Pipeline(
            remote="host:6400",
            streams=["S"],
            keys=["k1"],
            key_prefix="pre_",
        )


def test_unknown_mode_rejected() -> None:
    with pytest.raises(ValueError, match="mode must be"):
        tally.Pipeline(remote="host:6400", streams=["S"], mode="bogus")


def test_streaming_run_not_implemented() -> None:
    p = tally.Pipeline(remote="host:6400", streams=["S"], mode="streaming")
    with pytest.raises(NotImplementedError, match="Phase 31"):
        p.run()


def test_get_before_run_raises_runtime_error() -> None:
    p = tally.Pipeline(remote="host:6400", streams=["S"])
    with pytest.raises(RuntimeError, match="run"):
        p.get("k", "S")


def test_inspect_before_run_raises_runtime_error() -> None:
    p = tally.Pipeline(remote="host:6400", streams=["S"])
    with pytest.raises(RuntimeError, match="run"):
        p.inspect()


def test_token_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TALLY_TOKEN", "env-token-abc")
    p = tally.Pipeline(remote="host:6400", streams=["S"], token=None)
    assert p._debug_effective_token() == "env-token-abc"


def test_explicit_token_wins_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TALLY_TOKEN", "env-token")
    p = tally.Pipeline(
        remote="host:6400", streams=["S"], token="explicit-token"
    )
    assert p._debug_effective_token() == "explicit-token"


def test_token_none_no_env_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TALLY_TOKEN", raising=False)
    p = tally.Pipeline(remote="host:6400", streams=["S"], token=None)
    assert p._debug_effective_token() is None
