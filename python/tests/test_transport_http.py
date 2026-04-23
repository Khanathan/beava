"""Tests for beava._transport.HttpTransport.

These tests require a running beava server via the `beava_server` fixture.
They are expected to FAIL (ImportError) until python/beava/_transport.py is
created in Task 1.b.
"""

from __future__ import annotations

import pytest

from beava._errors import RegistrationError
from beava._transport import HttpTransport

# A minimal valid event registration payload (matches Phase 2 wire contract).
VALID_REGISTER_PAYLOAD = (
    b'{"nodes":[{'
    b'"kind":"event",'
    b'"name":"TestEvent",'
    b'"schema":{"fields":{"event_time":"i64","amount":"f64"},"optional_fields":[]},'
    b'"event_time_field":"event_time",'
    b'"dedupe_key":null,"dedupe_window_ms":null,'
    b'"keep_events_for_ms":null,"tolerate_delay_ms":null'
    b"}]}"
)

# Payload that uses a reserved _beava_ prefix — server returns invalid_registration.
INVALID_REGISTER_PAYLOAD = (
    b'{"nodes":[{'
    b'"kind":"event",'
    b'"name":"_beava_reserved",'
    b'"schema":{"fields":{"x":"f64"},"optional_fields":[]},'
    b'"event_time_field":null,'
    b'"dedupe_key":null,"dedupe_window_ms":null,'
    b'"keep_events_for_ms":null,"tolerate_delay_ms":null'
    b"}]}"
)


class TestHttpTransportRegister:
    def test_http_transport_register_success(self, beava_server: tuple[str, str]) -> None:
        """Successful registration returns dict with status='ok' and registry_version >= 1."""
        http_url, _ = beava_server
        with HttpTransport(http_url) as t:
            result = t.send_register(VALID_REGISTER_PAYLOAD)
        assert result["status"] == "ok"
        assert result["registry_version"] >= 1

    def test_http_transport_register_validation_error(
        self, beava_server: tuple[str, str]
    ) -> None:
        """Invalid payload raises RegistrationError with code='invalid_registration'."""
        http_url, _ = beava_server
        with HttpTransport(http_url) as t:
            with pytest.raises(RegistrationError) as exc_info:
                t.send_register(INVALID_REGISTER_PAYLOAD)
        assert exc_info.value.code == "invalid_registration"
        assert exc_info.value.path != "" or exc_info.value.message != ""

    def test_http_transport_register_unsupported_media_type(
        self, beava_server: tuple[str, str]
    ) -> None:
        """Posting with wrong Content-Type raises RegistrationError.

        Expected code: 'unsupported_media_type'.
        """
        import httpx

        http_url, _ = beava_server
        # Post with text/plain instead of application/json
        r = httpx.post(
            f"{http_url}/register",
            content=b"hello",
            headers={"Content-Type": "text/plain"},
        )
        assert r.status_code == 415
        body = r.json()
        assert body["error"]["code"] == "unsupported_media_type"

    def test_http_transport_ping_not_implemented(
        self, beava_server: tuple[str, str]
    ) -> None:
        """HttpTransport.send_ping() raises NotImplementedError (HTTP has no /ping in v0)."""
        http_url, _ = beava_server
        with HttpTransport(http_url) as t:
            with pytest.raises(NotImplementedError) as exc_info:
                t.send_ping()
        assert "tcp" in str(exc_info.value).lower() or "ping" in str(exc_info.value).lower()
