"""beava.test.MockApp — in-memory test double of bv.App.

For unit tests that exercise the SDK shape without spinning up the embed
binary. Records all calls so tests can assert sequences. Implements the
public 7-method surface (``register`` / ``push`` / ``get`` / ``batch_get``
/ ``reset`` / ``ping`` / ``close``) with reasonable canned responses.
"""
from __future__ import annotations

from typing import Any


class MockApp:
    """In-memory ``bv.App`` test double."""

    def __init__(
        self,
        url: str | None = None,
        *,
        timeout: float = 30.0,
        test_mode: bool = False,
    ) -> None:
        self._url = url
        self._timeout = timeout
        self._test_mode = test_mode
        self._closed = False
        self._calls: list[tuple[Any, ...]] = []
        self._get_responses: dict[
            tuple[str, Any], dict[str, Any]
        ] = {}
        self._registered: list[Any] = []

    def __enter__(self) -> "MockApp":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._closed = True

    def register(
        self,
        *descriptors: Any,
        force: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self._calls.append(
            ("register", descriptors, {"force": force, "dry_run": dry_run})
        )
        self._registered.extend(descriptors)
        return {"status": "ok", "registry_version": len(self._registered)}

    def push(self, event_name: str, fields: dict[str, Any]) -> dict[str, Any]:
        self._calls.append(("push", event_name, dict(fields)))
        return {"ack_lsn": len(self._calls), "registry_version": 1}

    def get(self, table: str, key: Any = None) -> dict[str, Any]:
        self._calls.append(("get", table, key))
        if key is None:
            key = ""
        lookup_key: Any = tuple(key) if isinstance(key, list) else key
        return self._get_responses.get((table, lookup_key), {})

    def batch_get(
        self, requests: list[tuple[str, Any]]
    ) -> list[dict[str, Any]]:
        self._calls.append(("batch_get", list(requests)))
        out: list[dict[str, Any]] = []
        for table, key in requests:
            lookup_key: Any = tuple(key) if isinstance(key, list) else key
            out.append(self._get_responses.get((table, lookup_key), {}))
        return out

    def reset(self) -> None:
        self._calls.append(("reset",))
        self._get_responses.clear()

    def ping(self) -> dict[str, Any]:
        self._calls.append(("ping",))
        return {
            "server_version": "0.0.0-mock",
            "registry_version": len(self._registered),
        }

    # Helpers for tests to set up canned responses.
    def _set_get_response(
        self, table: str, key: Any, response: dict[str, Any]
    ) -> None:
        lookup_key: Any = tuple(key) if isinstance(key, list) else key
        self._get_responses[(table, lookup_key)] = response
