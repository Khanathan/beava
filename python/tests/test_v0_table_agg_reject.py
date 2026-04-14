"""Plan 21-03: Table.group_by must be rejected at registration time (v0)."""

from __future__ import annotations

import pytest

import tally as tl


def test_table_group_by_raises_exact_message():
    @tl.table(key="user_id")
    class Users:
        user_id: str
        name: str

    with pytest.raises(RuntimeError) as exc_info:
        Users.group_by("user_id")

    expected = (
        "Cannot aggregate over Table 'Users'. "
        "Tables are current-state-only in v0; Table aggregation ships in v0.1. "
        "To aggregate related data, model it as a Stream source."
    )
    assert str(exc_info.value) == expected


def test_table_group_by_rejected_for_any_key():
    @tl.table(key=["account_id", "region"])
    class Accounts:
        account_id: str
        region: str
        balance: float

    # The rejection fires regardless of whether the key is real. No other
    # exception type is acceptable.
    with pytest.raises(RuntimeError, match=r"Cannot aggregate over Table 'Accounts'"):
        Accounts.group_by("account_id")
