"""Retry policy for transient network failures.

Controls how :class:`~beava._client.BeavaClient` recovers when the TCP
connection to the server is broken mid-send (server restart, flaky
network, load-balancer redial). The policy produces a bounded
exponential-backoff delay sequence with optional jitter so many clients
reconnecting after a server restart do not thunder.

Design notes:

- The policy owns only the *delay schedule*. The decision of *what* is
  retriable lives in the client: socket-level connect/send errors are
  retried, server protocol errors returned as STATUS_ERROR frames are
  NOT retried (the server already made a decision; retrying would not
  change it).
- Policies are immutable after construction; callers who want different
  behaviour build a second policy and pass it to ``App(retry_policy=...)``.
"""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with optional jitter.

    Args:
        max_retries: Number of retries AFTER the initial attempt. Total
            attempts on a failing send = ``max_retries + 1``. Setting
            ``max_retries=0`` disables retries entirely (pre-Phase-43
            behaviour: one reconnect attempt, then raise).
        initial_delay_s: Sleep before the first retry, in seconds.
        max_delay_s: Upper bound on the delay between retries, in seconds.
        backoff_factor: Multiplier applied after each failed attempt.
            ``2.0`` doubles; ``1.0`` produces a constant delay.
        jitter: If True, each delay is multiplied by a uniform sample in
            ``[0.5, 1.0)``. Reduces thundering herd when many clients
            reconnect simultaneously after a server restart.
    """

    max_retries: int = 3
    initial_delay_s: float = 0.05
    max_delay_s: float = 1.0
    backoff_factor: float = 2.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.initial_delay_s < 0:
            raise ValueError(
                f"initial_delay_s must be >= 0, got {self.initial_delay_s}"
            )
        if self.max_delay_s < self.initial_delay_s:
            raise ValueError(
                f"max_delay_s ({self.max_delay_s}) must be >= initial_delay_s "
                f"({self.initial_delay_s})"
            )
        if self.backoff_factor < 1.0:
            raise ValueError(
                f"backoff_factor must be >= 1.0, got {self.backoff_factor}"
            )

    def compute_delay(self, attempt: int) -> float:
        """Delay before retry number ``attempt``.

        ``attempt=1`` is the first retry (after the initial failure),
        ``attempt=2`` is the second, etc. Raises ``ValueError`` for
        ``attempt < 1`` since attempt 0 is the initial try and never
        sleeps.
        """
        if attempt < 1:
            raise ValueError(f"attempt must be >= 1, got {attempt}")
        base = self.initial_delay_s * (self.backoff_factor ** (attempt - 1))
        base = min(base, self.max_delay_s)
        if self.jitter:
            base *= 0.5 + random.random() * 0.5
        return base


#: Default policy used when ``App(retry_policy=...)`` is not specified.
#: 3 retries with 50 ms / 100 ms / 200 ms backoff (ignoring jitter) and a
#: hard ceiling of 1 s per delay. Tuned so a server restart that takes
#: under ~500 ms is invisible to the caller.
DEFAULT_POLICY = RetryPolicy()

#: Policy that disables retries entirely. Callers who prefer the
#: pre-Phase-43 behaviour (single reconnect attempt, raise on second
#: failure) pass this to :class:`~beava._app.App`.
NO_RETRY = RetryPolicy(max_retries=0, backoff_factor=1.0, jitter=False)
