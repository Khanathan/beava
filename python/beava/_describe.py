"""Shared ``.describe()`` serialization helpers for Stream and Table sources.

The describe format is a deterministic, JSON-serializable dict used for
introspection and error messages. Field order follows declaration order.
"""

from __future__ import annotations

from typing import Any

from beava._types_core import MISSING, FieldSpec


def _type_name(t: type) -> str:
    """Human-readable short name for a field type."""
    return getattr(t, "__name__", repr(t))


def schema_to_describe_dict(schema: dict[str, FieldSpec]) -> dict[str, dict[str, Any]]:
    """Serialize a FieldSpec map to the ``fields`` sub-dict shape."""
    out: dict[str, dict[str, Any]] = {}
    for name, spec in schema.items():
        entry: dict[str, Any] = {
            "type": _type_name(spec.py_type),
            "optional": spec.optional,
            "desc": spec.desc,
        }
        if spec.default is not MISSING:
            entry["default"] = spec.default
        out[name] = entry
    return out


def format_describe(
    *,
    name: str,
    kind: str,
    key: list[str] | None,
    mode: str | None,
    schema: dict[str, FieldSpec],
    ttl: str | None = None,
    history_ttl: str | None = None,
) -> dict[str, Any]:
    """Build the full ``describe()`` dict for a Stream or Table source.

    ``kind`` is ``"stream"`` or ``"table"``. Streams pass ``key=None`` and
    ``mode=None``. Optional ttl / history_ttl are included only when set.
    """
    d: dict[str, Any] = {
        "name": name,
        "kind": kind,
        "key": key,
        "fields": schema_to_describe_dict(schema),
    }
    if mode is not None:
        d["mode"] = mode
    if ttl is not None:
        d["ttl"] = ttl
    if history_ttl is not None:
        d["history_ttl"] = history_ttl
    return d
