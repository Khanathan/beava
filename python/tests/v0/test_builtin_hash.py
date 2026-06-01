"""TDD red — hash-builtin Python sugar (PR 3 Step 1).

What this file checks: ``bv.hash_mod(x, m)`` produces ``hash_mod(x, m)``
on the wire.

Why hash_mod matters: it takes a high-cardinality field like an email
address and reduces it to one of a fixed number of buckets. Useful when
you want a feature like "how many distinct email-buckets per user this
hour" without storing every distinct email.

Status today: fails because ``bv.hash_mod`` doesn't exist yet. Step 5
of PR 3 adds it.
"""
from __future__ import annotations

import beava as bv


def test_hash_mod_with_column() -> None:
    """Standard case: bucket the email field into 1024 slots."""
    expr = bv.hash_mod(bv.col("email"), 1024)
    assert expr.to_expr_string() == "hash_mod(email, 1024)"


def test_hash_mod_with_literal_value() -> None:
    """Both args auto-wrap as literals when not expressions. Useful
    inside ``@bv.expr`` bodies where the first arg might be a constant
    or a derived expression rather than a raw column reference."""
    expr = bv.hash_mod("constant_key", 16)
    assert expr.to_expr_string() == "hash_mod('constant_key', 16)"
