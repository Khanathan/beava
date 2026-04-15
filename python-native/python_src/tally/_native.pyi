"""Type stubs for the tally native replica-client extension (Plan 30-01).

Hand-written to match python-native/src/pipeline.rs + python-native/src/errors.rs.
Keep in sync when adding kwargs or methods.
"""

from typing import Any, Dict, List, Optional


class TallyError(Exception):
    """Base class for all native replica-client errors."""


class OutOfScopeError(TallyError):
    """Raised by Pipeline.get when the (stream, key) falls outside declared scope."""


class ClientConnectError(TallyError):
    """Raised when TCP connect / snapshot-fetch retries are exhausted."""


class HandshakeError(TallyError):
    """Raised when the server rejects authentication or the declared scope."""


class ReplicaStateError(TallyError):
    """Raised on protocol / decode / invariant violations."""


class Pipeline:
    def __init__(
        self,
        *,
        remote: str,
        streams: List[str],
        keys: Optional[List[str]] = None,
        key_prefix: Optional[str] = None,
        mode: str = "historical",
        token: Optional[str] = None,
        since: Optional[int] = None,
    ) -> None: ...
    def run(self) -> None: ...
    def get(self, key: str, stream: str) -> Optional[Any]: ...
    def inspect(self) -> Dict[str, int]: ...
    def _debug_effective_token(self) -> Optional[str]: ...
