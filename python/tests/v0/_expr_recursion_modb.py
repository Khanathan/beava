"""Cross-file mutual-recursion fixture, module B (PR 5 recursion tests).

Companion to ``_expr_recursion_moda``. ``b`` calls ``moda.a``, closing the
``a → b → a`` cycle across two files. See module A for why the import is
done at module level but the call resolves lazily.
"""
from __future__ import annotations

import beava as bv

from . import _expr_recursion_moda as moda


@bv.expr
def b(x):
    return moda.a(x)
