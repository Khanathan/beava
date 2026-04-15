"""``tl.col`` expression DSL.

``tl.col("x") + tl.col("y") > 100`` captures an expression AST via Python
operator overloading and serialises to the grammar already parsed by the
Rust engine (``src/engine/expression.rs``).

Grammar emitted (every binary op is parenthesized for unambiguous reparse):
  - Field access:    bare identifier, e.g. ``x`` or ``Stream.x``
  - Literals:        numbers, single-quoted strings, ``true`` / ``false``, ``null``
  - Arithmetic:      ``+`` ``-`` ``*`` ``/``
  - Comparison:      ``>`` ``>=`` ``<`` ``<=`` ``==`` ``!=``
  - Boolean:         ``and`` ``or`` ``not``
  - Calls:           ``cast(x, float)`` (and future builtins)

Any change to this emitter must stay in lockstep with the Rust parser.
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# AST node types
# ---------------------------------------------------------------------------


class _ExprAST:
    """Base class for the captured expression tree.

    Instances support operator overloading so that composing them with ``+``,
    ``>``, ``&``, etc. produces new AST nodes. Leaves are ``_Field`` or
    ``_Literal``; composite nodes are ``_BinOp`` / ``_UnaryOp`` / ``_Call``.
    """

    # --- arithmetic ---
    def __add__(self, other: Any) -> "_ExprAST":
        return _BinOp("+", self, _wrap(other))

    def __radd__(self, other: Any) -> "_ExprAST":
        return _BinOp("+", _wrap(other), self)

    def __sub__(self, other: Any) -> "_ExprAST":
        return _BinOp("-", self, _wrap(other))

    def __rsub__(self, other: Any) -> "_ExprAST":
        return _BinOp("-", _wrap(other), self)

    def __mul__(self, other: Any) -> "_ExprAST":
        return _BinOp("*", self, _wrap(other))

    def __rmul__(self, other: Any) -> "_ExprAST":
        return _BinOp("*", _wrap(other), self)

    def __truediv__(self, other: Any) -> "_ExprAST":
        return _BinOp("/", self, _wrap(other))

    def __rtruediv__(self, other: Any) -> "_ExprAST":
        return _BinOp("/", _wrap(other), self)

    # --- comparison ---
    def __gt__(self, other: Any) -> "_ExprAST":
        return _BinOp(">", self, _wrap(other))

    def __ge__(self, other: Any) -> "_ExprAST":
        return _BinOp(">=", self, _wrap(other))

    def __lt__(self, other: Any) -> "_ExprAST":
        return _BinOp("<", self, _wrap(other))

    def __le__(self, other: Any) -> "_ExprAST":
        return _BinOp("<=", self, _wrap(other))

    def __eq__(self, other: Any) -> "_ExprAST":  # type: ignore[override]
        return _BinOp("==", self, _wrap(other))

    def __ne__(self, other: Any) -> "_ExprAST":  # type: ignore[override]
        return _BinOp("!=", self, _wrap(other))

    # --- boolean ---
    # `&` / `|` / `~` are used instead of `and`/`or`/`not` because Python's
    # `and`/`or`/`not` cannot be overloaded.
    def __and__(self, other: Any) -> "_ExprAST":
        return _BinOp("and", self, _wrap(other))

    def __rand__(self, other: Any) -> "_ExprAST":
        return _BinOp("and", _wrap(other), self)

    def __or__(self, other: Any) -> "_ExprAST":
        return _BinOp("or", self, _wrap(other))

    def __ror__(self, other: Any) -> "_ExprAST":
        return _BinOp("or", _wrap(other), self)

    def __invert__(self) -> "_ExprAST":
        return _UnaryOp("not", self)

    # __hash__ is required because we override __eq__; hash by identity so
    # expression ASTs can live in sets / dict keys if needed.
    def __hash__(self) -> int:
        return id(self)

    # --- methods ---
    def isnull(self) -> "_ExprAST":
        """``col('x').isnull()`` → ``(x == null)``."""
        return _BinOp("==", self, _Literal(None))

    def cast(self, type_name: str) -> "_ExprAST":
        """``col('x').cast('float')`` → ``cast(x, float)``."""
        if not isinstance(type_name, str):
            raise TypeError(f"cast() type must be a string, got {type(type_name).__name__}")
        return _Call("cast", [self, _Literal(_BareIdent(type_name))])

    # --- serialization / introspection ---
    def to_expr_string(self) -> str:  # pragma: no cover - overridden
        raise NotImplementedError

    def referenced_fields(self) -> set[str]:
        """Return every field name referenced anywhere in this expression."""
        out: set[str] = set()
        self._collect_fields(out)
        return out

    def _collect_fields(self, out: set[str]) -> None:  # pragma: no cover - overridden
        raise NotImplementedError


class _BareIdent:
    """Marker for literals that should serialize as a bare identifier
    rather than a quoted string (used for ``cast(x, float)``).
    """

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


class _Field(_ExprAST):
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def to_expr_string(self) -> str:
        return self.name

    def _collect_fields(self, out: set[str]) -> None:
        out.add(self.name)


class _Literal(_ExprAST):
    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def to_expr_string(self) -> str:
        v = self.value
        if v is None:
            return "null"
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, _BareIdent):
            return v.name
        if isinstance(v, (int, float)):
            return repr(v)
        if isinstance(v, str):
            escaped = v.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        raise TypeError(f"unsupported literal type in tl.col expression: {type(v).__name__}")

    def _collect_fields(self, out: set[str]) -> None:
        return None


class _BinOp(_ExprAST):
    __slots__ = ("op", "left", "right")

    def __init__(self, op: str, left: _ExprAST, right: _ExprAST) -> None:
        self.op = op
        self.left = left
        self.right = right

    def to_expr_string(self) -> str:
        return f"({self.left.to_expr_string()} {self.op} {self.right.to_expr_string()})"

    def _collect_fields(self, out: set[str]) -> None:
        self.left._collect_fields(out)
        self.right._collect_fields(out)


class _UnaryOp(_ExprAST):
    __slots__ = ("op", "operand")

    def __init__(self, op: str, operand: _ExprAST) -> None:
        self.op = op
        self.operand = operand

    def to_expr_string(self) -> str:
        return f"({self.op} {self.operand.to_expr_string()})"

    def _collect_fields(self, out: set[str]) -> None:
        self.operand._collect_fields(out)


class _Call(_ExprAST):
    __slots__ = ("fn", "args")

    def __init__(self, fn: str, args: list[_ExprAST]) -> None:
        self.fn = fn
        self.args = args

    def to_expr_string(self) -> str:
        inner = ", ".join(a.to_expr_string() for a in self.args)
        return f"{self.fn}({inner})"

    def _collect_fields(self, out: set[str]) -> None:
        for a in self.args:
            a._collect_fields(out)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _wrap(value: Any) -> _ExprAST:
    """Promote a Python value into an AST leaf."""
    if isinstance(value, _ExprAST):
        return value
    return _Literal(value)


def col(name: str) -> _ExprAST:
    """Construct a column reference expression.

    ``col("amount")`` — bare field access.
    ``col("Stream.amount")`` — qualified field access (for cross-stream derive).
    """
    if not isinstance(name, str) or not name:
        raise TypeError("tl.col(name) requires a non-empty string")
    return _Field(name)


# Expose the AST base publicly (for isinstance checks in _stream / _table later).
Col = _ExprAST

__all__ = ["col", "Col"]
