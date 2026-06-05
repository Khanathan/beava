"""Cross-file mutual-recursion fixture, module A (PR 5 recursion tests).

``a`` calls ``modb.b``; ``modb.b`` calls back into ``a`` (see
``_expr_recursion_modb``). The two ``@bv.expr`` functions live in
different files so the frame-stack cycle report shows distinct per-hop
file paths. Neither calls *itself*, so the decoration-time static scan
passes — the loop is caught at call time by the per-thread frame stack.

Importing the *module* (not the bare name) lets the circular import
resolve: ``modb.b`` is looked up only when ``a`` actually runs.
"""
from __future__ import annotations

import beava as bv

from . import _expr_recursion_modb as modb


@bv.expr
def a(x):
    return modb.b(x)
