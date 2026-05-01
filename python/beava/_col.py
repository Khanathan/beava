"""``bv.col`` expression DSL — operator-overloaded AST with canonical serialization.

``bv.col("amount") > 100`` builds an expression AST via Python operator
overloading. Calling ``.to_expr_string()`` on the resulting tree emits the
canonical parenthesized grammar that the Phase 4 server-side evaluator parses.

Grammar emitted (LOCKED — Phase 4 parser depends on this; never change without
coordinating with the server-side expression evaluator):

    expr      := field | literal | bin_op | unary_op | call
    field     := identifier | identifier "." identifier  # e.g., x, Stream.x
    literal   := number | "'" string "'" | "true" | "false" | "null"
    bin_op    := "(" expr <space> op <space> expr ")"   # EVERY binary op is parenthesized
    op        := "+" | "-" | "*" | "/" | ">" | ">=" | "<" | "<=" | "==" | "!=" | "and" | "or"
    unary_op  := "(" "not" <space> expr ")"
    call      := ident "(" expr ("," <space> expr)* ")"

String literal escaping: ``\\`` becomes ``\\\\``; ``'`` becomes ``\\'``.

Threat mitigation (T-03-02-01): string literal values are always escaped via
``_Literal.to_expr_string()`` before being embedded in the expression string,
preventing injection of bare tokens from user-supplied strings.
"""

from __future__ import annotations

from typing import Any

__all__ = ["col", "Col", "infer_output_type"]


# ---------------------------------------------------------------------------
# AST node hierarchy
# ---------------------------------------------------------------------------


class _ExprAST:
    """Base class for all expression AST nodes.

    Instances support full operator overloading so that composing them with
    ``+``, ``>``, ``&``, ``~`` etc. returns new AST nodes rather than
    evaluating Python expressions eagerly.

    Concrete subclasses must implement ``to_expr_string()`` and
    ``_collect_fields(out)``.
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

    # --- boolean combinators ---
    # Python's ``and`` / ``or`` / ``not`` keywords cannot be overloaded.
    # ``&`` / ``|`` / ``~`` are used instead and emit the grammar keywords.
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

    # ``__hash__`` is required because we override ``__eq__``; use object
    # identity so AST nodes remain hashable and can live in sets/dicts.
    def __hash__(self) -> int:
        return id(self)

    # --- convenience methods ---
    def isnull(self) -> "_ExprAST":
        """Shorthand for ``(x == null)`` — checks whether the column is null."""
        return _BinOp("==", self, _Literal(None))

    def cast(self, type_name: str) -> "_ExprAST":
        """Emit ``cast(x, type_name)`` where *type_name* renders as a bare identifier.

        Args:
            type_name: Target type as a string (e.g. ``"float"``, ``"int"``).

        Raises:
            TypeError: If *type_name* is not a string.
        """
        if not isinstance(type_name, str):
            raise TypeError(f"cast() type must be a string, got {type(type_name).__name__}")
        return _Call("cast", [self, _Literal(_BareIdent(type_name))])

    # --- serialization / introspection ---
    def to_expr_string(self) -> str:
        """Return the canonical parenthesized expression string for this node.

        The emitted form is consumed by the Phase 4 server-side evaluator.
        Override in every concrete subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def referenced_fields(self) -> set[str]:
        """Return the set of bare field names referenced anywhere in this AST.

        String literal contents and cast target names are NOT included.
        Used by Phase 4 schema validation to verify field references resolve.
        """
        out: set[str] = set()
        self._collect_fields(out)
        return out

    def _collect_fields(self, out: set[str]) -> None:
        """Recursively populate *out* with every field name in this subtree."""
        raise NotImplementedError  # pragma: no cover


# ---------------------------------------------------------------------------
# Marker types
# ---------------------------------------------------------------------------


class _BareIdent:
    """Marker for values that serialize as a bare identifier (not a quoted string).

    Used exclusively by ``cast(x, float)`` so that the type name renders as
    ``float`` rather than ``'float'``.
    """

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


# ---------------------------------------------------------------------------
# Concrete AST nodes
# ---------------------------------------------------------------------------


class _Field(_ExprAST):
    """Leaf node representing a column reference (e.g. ``x``, ``Stream.x``)."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def to_expr_string(self) -> str:
        return self.name

    def _collect_fields(self, out: set[str]) -> None:
        out.add(self.name)


class _Literal(_ExprAST):
    """Leaf node representing a scalar literal value.

    Serialization rules (T-03-02-01 mitigation — all paths escape user input):
    - ``None``       → ``null``
    - ``True``       → ``true``
    - ``False``      → ``false``
    - ``_BareIdent`` → bare name (used for cast target type)
    - ``int``/``float`` (after bool check) → ``repr(v)``
    - ``str``        → ``'...``' with ``\\`` doubled and ``'`` backslash-escaped
    """

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def to_expr_string(self) -> str:  # noqa: PLR0911 (multiple returns intentional)
        v = self.value
        if v is None:
            return "null"
        # bool MUST be checked before int because bool is a subclass of int.
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, _BareIdent):
            return v.name
        if isinstance(v, (int, float)):
            return repr(v)
        if isinstance(v, str):
            escaped = v.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        raise TypeError(f"unsupported literal type in bv.col expression: {type(v).__name__}")

    def _collect_fields(self, out: set[str]) -> None:
        # Literals carry no field references.
        return None


class _BinOp(_ExprAST):
    """Binary operation node.

    Serializes as ``(left op right)`` — the enclosing parentheses are the
    single code path that enforces the D-08 invariant: every binary op is
    parenthesized.
    """

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
    """Unary operation node.

    Serializes as ``(op operand)`` — e.g. ``(not flag)``.
    """

    __slots__ = ("op", "operand")

    def __init__(self, op: str, operand: _ExprAST) -> None:
        self.op = op
        self.operand = operand

    def to_expr_string(self) -> str:
        return f"({self.op} {self.operand.to_expr_string()})"

    def _collect_fields(self, out: set[str]) -> None:
        self.operand._collect_fields(out)


class _Call(_ExprAST):
    """Function-call node.

    Serializes as ``fn(arg1, arg2, ...)``. Used for ``cast`` and future
    built-in calls the Phase 4 evaluator will recognize.
    """

    __slots__ = ("fn", "args")

    def __init__(self, fn: str, args: list[_ExprAST]) -> None:
        self.fn = fn
        self.args = args

    def to_expr_string(self) -> str:
        inner = ", ".join(a.to_expr_string() for a in self.args)
        return f"{self.fn}({inner})"

    def _collect_fields(self, out: set[str]) -> None:
        for arg in self.args:
            arg._collect_fields(out)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wrap(value: Any) -> _ExprAST:
    """Promote a plain Python value into a ``_Literal`` AST leaf.

    If *value* is already an ``_ExprAST`` instance it is returned unchanged,
    enabling natural chaining of AST nodes on both sides of an operator.
    """
    if isinstance(value, _ExprAST):
        return value
    return _Literal(value)


# ---------------------------------------------------------------------------
# Public constructor
# ---------------------------------------------------------------------------


def col(name: str) -> _ExprAST:
    """Construct a column-reference expression leaf.

    Args:
        name: Field name — bare (``"x"``) or qualified (``"Stream.x"``).
              Must be a non-empty string.

    Returns:
        An ``_ExprAST`` leaf that can be composed with operators.

    Raises:
        TypeError: If *name* is not a non-empty string.

    Examples::

        bv.col("amount") > 100
        (bv.col("a") + bv.col("b")).to_expr_string()  # "(a + b)"
    """
    if not isinstance(name, str) or not name:
        raise TypeError("bv.col(name) requires a non-empty string")
    return _Field(name)


# Public alias for isinstance checks in _events.
Col = _ExprAST


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------

_NUMERIC_TYPES: frozenset[str] = frozenset({"i64", "f64"})
_COMPARISON_OPS: frozenset[str] = frozenset({">", ">=", "<", "<=", "==", "!="})
_BOOLEAN_OPS: frozenset[str] = frozenset({"and", "or"})
_ARITHMETIC_OPS: frozenset[str] = frozenset({"+", "-", "*", "/"})


def infer_output_type(lhs: str, rhs: str, op: str) -> str:
    """Infer the server FieldType string produced by applying *op* to *lhs* and *rhs*.

    Args:
        lhs: Left-hand operand type (server string: ``"i64"``, ``"f64"``,
             ``"str"``, ``"bool"``, ``"bytes"``, ``"datetime"``).
        rhs: Right-hand operand type.
        op: Operator string from the grammar (``"+"``, ``"-"``, ``"*"``,
            ``"/"``, ``">"``, etc., ``"and"``, ``"or"``).

    Returns:
        The output FieldType string.

    Raises:
        TypeError: If the operand types are incompatible with *op*.
        ValueError: If *op* is not a recognized operator.

    Type-inference rules:
    - Comparison ops (``> >= < <= == !=``):  always return ``"bool"``.
    - Boolean ops (``and or``): require both operands to be ``"bool"``; return ``"bool"``.
    - Arithmetic ops (``+ - * /``): require both operands to be numeric
      (``"i64"`` or ``"f64"``); ``"bool"`` is NOT numeric here.
      Division always widens to ``"f64"`` to avoid integer truncation surprises.
      Otherwise the result is ``"f64"`` if either operand is ``"f64"``, else ``"i64"``.
    """
    if op in _COMPARISON_OPS:
        return "bool"

    if op in _BOOLEAN_OPS:
        if lhs != "bool" or rhs != "bool":
            raise TypeError(
                f"boolean op {op!r} requires bool operands, got {lhs!r} and {rhs!r}"
            )
        return "bool"

    if op in _ARITHMETIC_OPS:
        if lhs not in _NUMERIC_TYPES or rhs not in _NUMERIC_TYPES:
            raise TypeError(
                f"arithmetic op {op!r} requires numeric operands (i64 or f64), "
                f"got {lhs!r} and {rhs!r}"
            )
        if op == "/":
            # Division always widens to float to avoid truncation surprises.
            return "f64"
        if lhs == "f64" or rhs == "f64":
            return "f64"
        return "i64"

    raise ValueError(f"unknown op {op!r}")
