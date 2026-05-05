"""bv.col expression DSL — Phase 13.5 Plan 03.

Operator-overloaded AST. ``_Col`` / ``_Literal`` / ``_BinOp`` / ``_UnaryOp`` /
``_CastOp`` form the AST; the public surface is ``bv.col(name)`` and
``bv.lit(value)``. ``_coerce`` wraps Python literals encountered inside an
operator overload into ``_Literal`` so users may write either::

    bv.col("amount") > 100
    bv.col("amount") > bv.lit(100)

interchangeably (per ADR-003 Decision A — explicit literal helper coexists
with implicit literal coercion).

The AST is intentionally inert at construction-time — all evaluation happens
on the server. ``to_expr_string()`` renders to the wire-JSON expression
string consumed by ``crates/beava-core/src/expr.rs`` parser.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Union

# Allowed cast targets — symmetric with docs/sdk-api/python.md § Expression DSL.
_VALID_CAST_TARGETS = ("str", "int", "float", "bool")


class _Expr:
    """Base AST node. Operator overloads produce new ``_Expr`` nodes.

    Subclasses are dataclasses with ``frozen=True`` so the AST is immutable —
    re-using the same ``_Expr`` instance in multiple chains is safe.
    """

    # Arithmetic ─────────────────────────────────────────────────────────────
    def __add__(self, other: Any) -> "_Expr":
        return _BinOp("+", self, _coerce(other))

    def __radd__(self, other: Any) -> "_Expr":
        return _BinOp("+", _coerce(other), self)

    def __sub__(self, other: Any) -> "_Expr":
        return _BinOp("-", self, _coerce(other))

    def __rsub__(self, other: Any) -> "_Expr":
        return _BinOp("-", _coerce(other), self)

    def __mul__(self, other: Any) -> "_Expr":
        return _BinOp("*", self, _coerce(other))

    def __rmul__(self, other: Any) -> "_Expr":
        return _BinOp("*", _coerce(other), self)

    def __truediv__(self, other: Any) -> "_Expr":
        return _BinOp("/", self, _coerce(other))

    def __rtruediv__(self, other: Any) -> "_Expr":
        return _BinOp("/", _coerce(other), self)

    # Comparisons ────────────────────────────────────────────────────────────
    def __gt__(self, other: Any) -> "_Expr":
        return _BinOp(">", self, _coerce(other))

    def __ge__(self, other: Any) -> "_Expr":
        return _BinOp(">=", self, _coerce(other))

    def __lt__(self, other: Any) -> "_Expr":
        return _BinOp("<", self, _coerce(other))

    def __le__(self, other: Any) -> "_Expr":
        return _BinOp("<=", self, _coerce(other))

    def __eq__(self, other: Any) -> "_Expr":  # type: ignore[override]
        return _BinOp("==", self, _coerce(other))

    def __ne__(self, other: Any) -> "_Expr":  # type: ignore[override]
        return _BinOp("!=", self, _coerce(other))

    # Boolean ────────────────────────────────────────────────────────────────
    # Phase 13.5.2: serialize Python `&` / `|` to the server's expr-grammar
    # `and` / `or` keywords (per `crates/beava-core/src/expr.rs` token table).
    # The Python operators `&` / `|` are bitwise — we overload them as boolean
    # combinators because Python forbids overloading `and` / `or`. The wire
    # form must match the expr-parser's keyword tokens.
    def __and__(self, other: Any) -> "_Expr":
        return _BinOp("and", self, _coerce(other))

    def __rand__(self, other: Any) -> "_Expr":
        return _BinOp("and", _coerce(other), self)

    def __or__(self, other: Any) -> "_Expr":
        return _BinOp("or", self, _coerce(other))

    def __ror__(self, other: Any) -> "_Expr":
        return _BinOp("or", _coerce(other), self)

    def __invert__(self) -> "_Expr":
        return _UnaryOp("~", self)

    # ``__hash__`` retained — frozen dataclasses define one. The override of
    # ``__eq__`` (which produces an ``_Expr``, not a ``bool``) makes Python
    # mark instances unhashable by default; restore it via dataclass below.

    # Helper methods ─────────────────────────────────────────────────────────
    def isnull(self) -> "_Expr":
        return _UnaryOp("isnull", self)

    def cast(self, target: str) -> "_Expr":
        if target not in _VALID_CAST_TARGETS:
            raise ValueError(
                f"cast target must be one of {_VALID_CAST_TARGETS}; got {target!r}"
            )
        return _CastOp(self, target)

    def to_expr_string(self) -> str:
        """Render this AST node to wire JSON expression-string form."""
        raise NotImplementedError


@dataclass(frozen=True, eq=False)
class _Col(_Expr):
    name: str

    def to_expr_string(self) -> str:
        return self.name

    def __hash__(self) -> int:
        return hash(("_Col", self.name))


@dataclass(frozen=True, eq=False)
class _Literal(_Expr):
    value: Any

    def to_expr_string(self) -> str:
        if self.value is None:
            return "null"
        if isinstance(self.value, bool):
            return "true" if self.value else "false"
        if isinstance(self.value, str):
            # Single-quoted per docs/pipeline-dsl/expressions.md grammar.
            return repr(self.value)
        return repr(self.value)  # int / float

    def __hash__(self) -> int:
        return hash(("_Literal", repr(self.value)))


@dataclass(frozen=True, eq=False)
class _BinOp(_Expr):
    op: str
    left: _Expr
    right: _Expr

    def to_expr_string(self) -> str:
        return f"({self.left.to_expr_string()} {self.op} {self.right.to_expr_string()})"

    def __hash__(self) -> int:
        return hash(("_BinOp", self.op, id(self.left), id(self.right)))


@dataclass(frozen=True, eq=False)
class _UnaryOp(_Expr):
    op: str
    operand: _Expr

    def to_expr_string(self) -> str:
        if self.op == "isnull":
            return f"({self.operand.to_expr_string()} == null)"
        if self.op == "~":
            return f"!({self.operand.to_expr_string()})"
        raise ValueError(f"unknown unary op: {self.op!r}")

    def __hash__(self) -> int:
        return hash(("_UnaryOp", self.op, id(self.operand)))


@dataclass(frozen=True, eq=False)
class _CastOp(_Expr):
    operand: _Expr
    target: str

    def to_expr_string(self) -> str:
        return f"cast({self.operand.to_expr_string()}, {self.target})"

    def __hash__(self) -> int:
        return hash(("_CastOp", self.target, id(self.operand)))


def _coerce(v: Any) -> _Expr:
    """Wrap a Python literal as a ``_Literal``; pass-through ``_Expr``."""
    return v if isinstance(v, _Expr) else _Literal(v)


def col(name: str) -> _Col:
    """Reference a schema field by name.

    Examples
    --------
    >>> import beava as bv
    >>> e = bv.col("amount") > 100
    >>> e.to_expr_string()
    '(amount > 100)'
    """
    return _Col(name)


def lit(value: Union[int, float, str, bool, None]) -> _Literal:  # noqa: UP007
    """Construct an explicit literal expression — public per ADR-003.

    The implicit form ``bv.col("a") > 100`` and the explicit form
    ``bv.col("a") > bv.lit(100)`` are wire-equivalent. ``bv.lit`` is
    primarily useful when the literal stands on its own (constant column,
    numerator of a division, etc.) where Python's operator dispatch alone
    would not coerce.

    Examples
    --------
    >>> import beava as bv
    >>> bv.lit(42).value
    42
    >>> bv.lit("web").to_expr_string()
    "'web'"
    >>> bv.lit(None).to_expr_string()
    'null'
    """
    return _Literal(value)
