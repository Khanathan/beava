"""Phase 18 Plan 07 — Task 7.5 tests.

Tests for the new app.upsert() / app.delete() methods on the Python SDK,
and verification that the old app.push_table() / app.delete_table() methods
no longer exist.

RED phase: these tests fail because:
  - app.upsert() / app.delete() don't exist yet on App
  - app.push_table() / app.delete_table() AttributeError tests pass now
    (these methods never existed), but the upsert/delete existence tests fail.

Per Phase 16 GA-2 decision: NO deprecation aliases.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
import beava as bv


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def user_table():
    """A simple temporal table descriptor for upsert/delete tests.

    Note: @bv.source is Phase 16 Plan 01 work (not yet landed).
    Using @bv.table directly here; Plan 16-01 will add the @bv.source
    annotation requirement. This test focuses on Plan 18-07's upsert/delete
    method existence and URL dispatch.
    """

    @bv.table(key="user_id")
    class UserProfile:
        user_id: str
        country: str

    return UserProfile


# ─── Tests ───────────────────────────────────────────────────────────────────


def test_app_has_upsert_method():
    """app.upsert must exist as a method on the App class.

    RED: fails because upsert() is not yet defined in _app.py.
    """
    app = bv.App("http://localhost:7379")
    assert hasattr(app, "upsert"), (
        "bv.App must have an 'upsert' method after Plan 18-07"
    )
    assert callable(app.upsert), "app.upsert must be callable"


def test_app_has_delete_method():
    """app.delete must exist as a method on the App class.

    RED: fails because delete() is not yet defined in _app.py.
    """
    app = bv.App("http://localhost:7379")
    assert hasattr(app, "delete"), (
        "bv.App must have a 'delete' method after Plan 18-07"
    )
    assert callable(app.delete), "app.delete must be callable"


def test_push_table_raises_attribute_error():
    """app.push_table must NOT exist — calling it raises AttributeError.

    Per Phase 16 GA-2: no deprecation aliases. push_table must never exist.
    """
    app = bv.App("http://localhost:7379")
    with pytest.raises(AttributeError):
        app.push_table(None, {})  # type: ignore[attr-defined]


def test_delete_table_raises_attribute_error():
    """app.delete_table must NOT exist — calling it raises AttributeError.

    Per Phase 16 GA-2: no deprecation aliases.
    """
    app = bv.App("http://localhost:7379")
    with pytest.raises(AttributeError):
        app.delete_table(None, key={})  # type: ignore[attr-defined]


def test_upsert_posts_to_correct_url(user_table):
    """app.upsert(UserProfile, row) POSTs to /upsert/UserProfile.

    RED: fails because upsert() doesn't exist yet.
    Uses unittest.mock to avoid a live server dependency.
    """
    # Mock the httpx client's post method to capture the URL.
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"ack_lsn": 1, "registry_version": 1}

    app = bv.App("http://localhost:7379")
    with patch.object(app._transport._client, "post", return_value=mock_response) as mock_post:
        result = app.upsert(user_table, {"user_id": "alice", "country": "US"})

    # Verify the call went to /upsert/UserProfile.
    mock_post.assert_called_once()
    call_args = mock_post.call_args
    posted_url = call_args[0][0] if call_args[0] else call_args[1].get("url", "")
    assert "/upsert/UserProfile" in posted_url or posted_url == "/upsert/UserProfile", (
        f"upsert must POST to /upsert/UserProfile, got: {posted_url}"
    )
    assert result["ack_lsn"] == 1


def test_delete_posts_to_correct_url(user_table):
    """app.delete(UserProfile, key={...}) POSTs to /delete/UserProfile with key body.

    RED: fails because delete() doesn't exist yet.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"ack_lsn": 2, "registry_version": 1}

    app = bv.App("http://localhost:7379")
    with patch.object(app._transport._client, "post", return_value=mock_response) as mock_post:
        result = app.delete(user_table, key={"user_id": "alice"})

    mock_post.assert_called_once()
    call_args = mock_post.call_args
    posted_url = call_args[0][0] if call_args[0] else call_args[1].get("url", "")
    assert "/delete/UserProfile" in posted_url or posted_url == "/delete/UserProfile", (
        f"delete must POST to /delete/UserProfile, got: {posted_url}"
    )
    # Verify the key is in the request body.
    content = call_args[1].get("content", b"")
    body = json.loads(content) if isinstance(content, bytes) else {}
    assert body.get("key") == {"user_id": "alice"}, (
        f"delete body must contain key={{user_id: alice}}, got: {body}"
    )
    assert result["ack_lsn"] == 2
