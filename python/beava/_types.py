"""Core types for the Beava Python SDK.

FeatureResult: attribute-access wrapper for feature maps returned by the server.
Exception hierarchy: BeavaError > ConnectionError, ProtocolError.
"""

from __future__ import annotations


class BeavaError(Exception):
    """Base exception for all Beava SDK errors."""


class ConnectionError(BeavaError):  # noqa: A001 -- intentionally shadows builtin
    """Raised when the TCP connection to the Beava server fails."""


class ProtocolError(BeavaError):
    """Raised when a protocol-level error occurs (bad frame, server error status)."""


class FeatureResult:
    """Thin wrapper over a feature map providing attribute-style access.

    Usage::

        features = FeatureResult({"tx_count": 7, "rate": 0.14})
        features.tx_count   # 7
        features["rate"]    # 0.14
        features.to_dict()  # {"tx_count": 7, "rate": 0.14}
    """

    __slots__ = ("_data",)

    def __init__(self, data: dict) -> None:
        # Use object.__setattr__ to bypass our __slots__ restriction
        object.__setattr__(self, "_data", dict(data))

    def __getattr__(self, name: str) -> object:
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(f"no feature named '{name}'") from None

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def to_dict(self) -> dict:
        """Return a copy of the underlying feature map."""
        return dict(self._data)

    def __repr__(self) -> str:
        return f"FeatureResult({self._data!r})"
