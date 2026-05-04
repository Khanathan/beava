"""Phase 13.5 Plan 02 cross-amendment: bv.App(test_mode=True) tests.

Validates D-05 (cross-amendment from 13.4 D-03):
  - Embed mode + test_mode=True propagates BEAVA_TEST_MODE=1 to the spawned
    binary via subprocess env.
  - Network mode (url is set) + test_mode=True emits a UserWarning and
    proceeds without effect (server controls test mode in network mode).

# RED-AT-COMMIT-TIME: this test file would have failed before Task 2.b committed
# the BEAVA_TEST_MODE=1 env propagation in spawn_embedded_server() and the
# UserWarning emission in App.__init__. It now serves as the regression tripwire.
"""
from __future__ import annotations

import warnings
from unittest.mock import MagicMock, patch

import beava as bv


def test_embed_test_mode_true_propagates_env() -> None:
    """Embed mode + test_mode=True must include BEAVA_TEST_MODE=1 in spawn env.

    Mocks spawn_embedded_server so we don't actually launch a binary; verifies
    test_mode=True is passed through to the spawn call.
    """
    fake_proc = MagicMock()
    fake_env = {"BEAVA_TEST_MODE": "1", "PATH": "/usr/bin"}
    with patch("beava._embed.spawn_embedded_server") as spawn:
        spawn.return_value = (fake_proc, "http://127.0.0.1:7777", "tcp://127.0.0.1:7778", fake_env)
        with patch("beava._transport.TcpTransport"):
            with bv.App(test_mode=True) as app:
                env = app._transport._spawn_env  # type: ignore[attr-defined]
        # Verify spawn_embedded_server received test_mode=True kwarg
        spawn.assert_called_once()
        assert spawn.call_args.kwargs.get("test_mode") is True
        # Verify the env dict includes the gate
        assert env.get("BEAVA_TEST_MODE") == "1"


def test_embed_test_mode_false_does_not_set_env_var() -> None:
    """Default (test_mode=False) must not set BEAVA_TEST_MODE."""
    fake_proc = MagicMock()
    fake_env = {"PATH": "/usr/bin"}  # no BEAVA_TEST_MODE
    with patch("beava._embed.spawn_embedded_server") as spawn:
        spawn.return_value = (fake_proc, "http://127.0.0.1:7777", "tcp://127.0.0.1:7778", fake_env)
        with patch("beava._transport.TcpTransport"):
            with bv.App() as app:  # default test_mode=False
                env = app._transport._spawn_env  # type: ignore[attr-defined]
        assert spawn.call_args.kwargs.get("test_mode") is False
        assert "BEAVA_TEST_MODE" not in env


def test_http_test_mode_true_emits_warning() -> None:
    """Network mode (http) + test_mode=True emits UserWarning per D-05."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        bv.App(url="http://localhost:7777", test_mode=True)
    assert any(issubclass(w.category, UserWarning) for w in caught)
    assert any("test_mode" in str(w.message).lower() for w in caught)


def test_tcp_test_mode_true_emits_warning() -> None:
    """Network mode (tcp) + test_mode=True emits UserWarning per D-05."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        bv.App(url="tcp://localhost:7778", test_mode=True)
    assert any(issubclass(w.category, UserWarning) for w in caught)
