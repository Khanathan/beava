"""Phase 60 Wave 1 — Python `@bv.stream(salt=N)` kwarg validation + REGISTER emission.

Covers D-A1..D-A3, D-A6 from 60-CONTEXT.md:
- `salt` is a SEPARATE kwarg on `@bv.stream` (not embedded in `shard_key`).
- Client-side fast-fail validation: `salt ∈ {2,4,8,...,256}` or `None`.
- REGISTER JSON emits `"salt": N` only when declared (absent, not null, otherwise).
- Works with tuple shard_key (salt covers the composite per D-A6).
"""

from __future__ import annotations

import pytest

import beava as bv
from beava._serialize import compile_to_register_json


# ---------------------------------------------------------------------------
# Accept: valid salt values
# ---------------------------------------------------------------------------


def test_salt_kwarg_accepts_16():
    @bv.stream(shard_key="user_id", salt=16)
    class Tx:
        user_id: str
        amount: float

    assert Tx._beava_salt == 16
    assert Tx._beava_shard_key == "user_id"


def test_salt_kwarg_accepts_all_powers_of_2():
    for n in (2, 4, 8, 16, 32, 64, 128, 256):
        @bv.stream(shard_key="user_id", salt=n)
        class _Tx:
            user_id: str

        assert _Tx._beava_salt == n, f"expected salt={n}, got {_Tx._beava_salt}"


def test_salt_kwarg_accepts_none_without_error():
    @bv.stream(shard_key="user_id", salt=None)
    class Tx:
        user_id: str

    assert Tx._beava_salt is None


def test_salt_kwarg_omitted_defaults_none():
    # shard_key set but salt omitted entirely — should default to None.
    @bv.stream(shard_key="user_id")
    class Tx:
        user_id: str

    assert Tx._beava_salt is None


# ---------------------------------------------------------------------------
# Reject: invalid salt values
# ---------------------------------------------------------------------------


def test_salt_kwarg_rejects_zero():
    with pytest.raises(TypeError) as exc:
        @bv.stream(shard_key="user_id", salt=0)
        class _Tx:
            user_id: str

    assert "[2, 256]" in str(exc.value)


def test_salt_kwarg_rejects_one():
    with pytest.raises(TypeError) as exc:
        @bv.stream(shard_key="user_id", salt=1)
        class _Tx:
            user_id: str

    assert "[2, 256]" in str(exc.value)


def test_salt_kwarg_rejects_non_power_of_2():
    with pytest.raises(TypeError) as exc:
        @bv.stream(shard_key="user_id", salt=10)
        class _Tx:
            user_id: str

    assert "power of 2" in str(exc.value)


def test_salt_kwarg_rejects_out_of_range():
    with pytest.raises(TypeError) as exc:
        @bv.stream(shard_key="user_id", salt=512)
        class _Tx:
            user_id: str

    assert "[2, 256]" in str(exc.value)


def test_salt_kwarg_rejects_non_int():
    with pytest.raises(TypeError) as exc:
        @bv.stream(shard_key="user_id", salt="16")
        class _Tx:
            user_id: str

    msg = str(exc.value)
    assert "int" in msg or "str" in msg


def test_salt_kwarg_rejects_bool():
    # bool is an int subclass in Python — ensure we reject it specifically.
    with pytest.raises(TypeError):
        @bv.stream(shard_key="user_id", salt=True)
        class _Tx:
            user_id: str


# ---------------------------------------------------------------------------
# REGISTER payload emission
# ---------------------------------------------------------------------------


def test_serialize_emits_salt_field():
    @bv.stream(shard_key="user_id", salt=16)
    class Tx:
        user_id: str
        amount: float

    payload = compile_to_register_json(Tx)
    assert payload["shard_key"] == "user_id"
    assert payload["salt"] == 16


def test_serialize_omits_salt_when_none():
    @bv.stream(shard_key="user_id")
    class Tx:
        user_id: str

    payload = compile_to_register_json(Tx)
    # salt MUST be absent, not present-as-null. Server's serde(default)
    # treats absent as None; emitting null would be a semantic change.
    assert "salt" not in payload


def test_serialize_tuple_shard_key_with_salt():
    @bv.stream(shard_key=("region", "user_id"), salt=8)
    class Tx:
        region: str
        user_id: str

    payload = compile_to_register_json(Tx)
    assert payload["shard_key"] == ["region", "user_id"]
    assert payload["salt"] == 8
