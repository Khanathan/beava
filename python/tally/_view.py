"""@view decorator for cross-stream view definitions.

A view is like a stream but restricted to only ``Derive`` and ``Lookup``
operators. It uses the same ``StreamMeta`` metaclass with ``_is_view=True``.

Usage::

    import tally as st

    @st.view(key="user_id")
    class UserRisk:
        tx_to_login_ratio = st.derive("Transactions.tx_count_1h / Logins.login_count_1h")
        is_suspicious     = st.derive("Transactions.tx_count_1h > 10 and Logins.login_count_1h < 2")
"""

from __future__ import annotations

from tally._stream import StreamMeta


def view(*, key: str):
    """Decorator that creates a view class restricted to derive/lookup operators.

    Usage::

        @view(key="user_id")
        class UserRisk:
            score = Derive("a + b")
    """

    def decorator(cls: type) -> StreamMeta:
        namespace = {
            k: v for k, v in cls.__dict__.items() if not k.startswith("__")
        }
        return StreamMeta(cls.__name__, cls.__bases__, namespace, key=key, _is_view=True)

    return decorator
