"""Plan 25-03 Task 4: tests for the ``tally suggest-config`` CLI.

These tests exercise ``tally._cli.main`` directly with ``urllib.request.urlopen``
monkeypatched, so they do not require a running server.
"""

from __future__ import annotations

import io
import json
import socket
import urllib.error
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from tally import _cli


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class _FakeResp(io.BytesIO):
    """Context-manager-capable BytesIO matching urllib's response protocol."""

    def __enter__(self):  # noqa: D401 - minimal impl
        return self

    def __exit__(self, *exc):
        self.close()
        return False


@contextmanager
def _mock_urlopen(payload):
    body = json.dumps(payload).encode("utf-8")

    def _fake(req, *args, **kwargs):
        return _FakeResp(body)

    with patch("tally._cli.urllib.request.urlopen", side_effect=_fake) as m:
        yield m


@contextmanager
def _mock_urlopen_error(exc):
    def _fake(req, *args, **kwargs):
        raise exc

    with patch("tally._cli.urllib.request.urlopen", side_effect=_fake) as m:
        yield m


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_suggest_config_empty_recs_prints_friendly_message(capsys):
    with _mock_urlopen({"recommendations": []}):
        rc = _cli.main(["suggest-config"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "No configuration recommendations at this time." in out


def test_suggest_config_groups_by_decorator_target(capsys):
    payload = {
        "recommendations": [
            {
                "knob": "UserProfile.ttl",
                "current": "30d",
                "suggested": "60d",
                "confidence": 0.72,
                "reason": "12% reinit rate",
                "evidence": {},
                "copy_paste": '@tl.table(key="user_id", ttl="60d")',
            },
            {
                "knob": "UserProfile.history_ttl",
                "current": "30d",
                "suggested": "60d",
                "confidence": 1.0,
                "reason": "downstream",
                "evidence": {},
                "copy_paste": '@tl.stream(history_ttl="60d")',
            },
            {
                "knob": "Clicks.history_ttl",
                "current": "90d",
                "suggested": "180d",
                "confidence": 1.0,
                "reason": "downstream",
                "evidence": {},
                "copy_paste": '@tl.stream(history_ttl="180d")',
            },
        ]
    }
    with _mock_urlopen(payload):
        rc = _cli.main(["suggest-config"])
    assert rc == 0
    out = capsys.readouterr().out
    # Two target headings appear, alphabetically ordered.
    assert out.index("Clicks:") < out.index("UserProfile:")
    # Each knob line is present.
    assert "UserProfile.ttl" in out
    assert "UserProfile.history_ttl" in out
    assert "Clicks.history_ttl" in out


def test_suggest_config_prints_copy_paste_line(capsys):
    payload = {
        "recommendations": [
            {
                "knob": "UserProfile.ttl",
                "current": "30d",
                "suggested": "60d",
                "confidence": 0.9,
                "reason": "test",
                "evidence": {},
                "copy_paste": '@tl.table(key="user_id", ttl="60d")',
            }
        ]
    }
    with _mock_urlopen(payload):
        rc = _cli.main(["suggest-config"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "@tl.table(" in out
    assert 'ttl="60d"' in out
    # Confidence is rendered with two decimals.
    assert "confidence=0.90" in out


def test_suggest_config_nonzero_on_connection_refused(capsys):
    # urllib raises URLError wrapping ConnectionRefusedError for a closed port.
    exc = urllib.error.URLError(ConnectionRefusedError("refused"))
    with _mock_urlopen_error(exc):
        rc = _cli.main(["suggest-config", "--host", "127.0.0.1", "--port", "1"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "could not reach" in err


def test_suggest_config_honours_host_and_port_flags():
    """The assembled URL must incorporate --host and --port."""
    captured = {}

    def _fake(req, *args, **kwargs):
        captured["url"] = req.full_url
        return _FakeResp(b'{"recommendations": []}')

    with patch("tally._cli.urllib.request.urlopen", side_effect=_fake):
        rc = _cli.main(
            ["suggest-config", "--host", "otherhost", "--port", "7777"]
        )
    assert rc == 0
    assert captured["url"] == "http://otherhost:7777/debug/config-recommendations"


def test_suggest_config_adds_bearer_token_header():
    captured = {}

    def _fake(req, *args, **kwargs):
        captured["auth"] = req.headers.get("Authorization")
        return _FakeResp(b'{"recommendations": []}')

    with patch("tally._cli.urllib.request.urlopen", side_effect=_fake):
        rc = _cli.main(["suggest-config", "--token", "sekret"])
    assert rc == 0
    assert captured["auth"] == "Bearer sekret"
