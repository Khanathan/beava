"""Plan 30-01: typed exception hierarchy tests.

Ensures every typed replica-client exception subclasses `TallyError`,
which itself subclasses `Exception`. The round-trip from a Rust
`CloneError` variant to its matching Python class is exercised in
Plan 30-02's end-to-end suite (needs a live server to trigger each
variant); we skip that assertion here.
"""

from __future__ import annotations

import pytest

import tally

pytestmark = pytest.mark.skipif(
    not getattr(tally, "_HAS_NATIVE", False),
    reason="tally._native not installed (pure-Python hatch build path)",
)


def test_tally_error_subclasses_exception() -> None:
    assert issubclass(tally.TallyError, Exception)


@pytest.mark.parametrize(
    "name",
    [
        "OutOfScopeError",
        "ClientConnectError",
        "HandshakeError",
        "ReplicaStateError",
    ],
)
def test_typed_exception_subclasses_tally_error(name: str) -> None:
    cls = getattr(tally, name)
    assert cls is not None, f"tally.{name} should be exported"
    assert issubclass(cls, tally.TallyError), f"{name} must subclass TallyError"


@pytest.mark.parametrize(
    "name",
    [
        "TallyError",
        "OutOfScopeError",
        "ClientConnectError",
        "HandshakeError",
        "ReplicaStateError",
    ],
)
def test_exception_constructs_with_message(name: str) -> None:
    cls = getattr(tally, name)
    err = cls("a message")
    assert "a message" in str(err)


def test_exceptions_can_be_raised_and_caught_as_tally_error() -> None:
    # Each typed exception should be catchable as TallyError.
    for name in [
        "OutOfScopeError",
        "ClientConnectError",
        "HandshakeError",
        "ReplicaStateError",
    ]:
        cls = getattr(tally, name)
        try:
            raise cls("boom")
        except tally.TallyError as e:
            assert "boom" in str(e)
        else:
            pytest.fail(f"{name} should have been caught as TallyError")


def test_out_of_scope_get_raises_typed_error() -> None:
    # Full round-trip (building a FrozenClient from a mock server) lives in
    # Plan 30-02 E2E. Here we just confirm construction + identity.
    pytest.skip(
        "full FrozenClient round-trip is covered by Plan 30-02 E2E tests"
    )
