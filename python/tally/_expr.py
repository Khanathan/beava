"""Expression tree nodes for the DataFrame-style API.

Expr objects capture arithmetic, comparison, and boolean operations as an AST
that serializes to the string expression format Tally's Rust server already
parses (e.g., ``"amount * fx_rate"``, ``"tx_count_1h > 10 and cbacks > 5"``).

Column objects are proxies for event fields or defined features. They support
operator overloading to build Expr trees, and aggregation methods that return
OperatorBase instances.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from tally._operators import (
    Avg,
    Count,
    Derive,
    DistinctCount,
    Ema,
    ExactMax,
    ExactMin,
    First,
    Lag,
    Last,
    LastN,
    Lookup,
    Max,
    Min,
    OperatorBase,
    Percentile,
    Stddev,
    Sum,
)

if TYPE_CHECKING:
    from tally._dataframe import Table


# ---------------------------------------------------------------------------
# Expression nodes
# ---------------------------------------------------------------------------


class Expr:
    """Base class for expression tree nodes.

    Subclasses implement ``to_expr_string()`` which returns a string parseable
    by the Tally server's expression evaluator.
    """

    def to_expr_string(self) -> str:
        raise NotImplementedError

    # Forward all operators so exprs compose: (a + b) > 5
    def __add__(self, other: Any) -> Expr:
        return BinOp("+", self, _wrap(other))

    def __radd__(self, other: Any) -> Expr:
        return BinOp("+", _wrap(other), self)

    def __sub__(self, other: Any) -> Expr:
        return BinOp("-", self, _wrap(other))

    def __rsub__(self, other: Any) -> Expr:
        return BinOp("-", _wrap(other), self)

    def __mul__(self, other: Any) -> Expr:
        return BinOp("*", self, _wrap(other))

    def __rmul__(self, other: Any) -> Expr:
        return BinOp("*", _wrap(other), self)

    def __truediv__(self, other: Any) -> Expr:
        return BinOp("/", self, _wrap(other))

    def __rtruediv__(self, other: Any) -> Expr:
        return BinOp("/", _wrap(other), self)

    def __gt__(self, other: Any) -> Expr:
        return BinOp(">", self, _wrap(other))

    def __lt__(self, other: Any) -> Expr:
        return BinOp("<", self, _wrap(other))

    def __ge__(self, other: Any) -> Expr:
        return BinOp(">=", self, _wrap(other))

    def __le__(self, other: Any) -> Expr:
        return BinOp("<=", self, _wrap(other))

    def __eq__(self, other: Any) -> Expr:  # type: ignore[override]
        return BinOp("==", self, _wrap(other))

    def __ne__(self, other: Any) -> Expr:  # type: ignore[override]
        return BinOp("!=", self, _wrap(other))

    def __and__(self, other: Any) -> Expr:
        return BinOp("and", self, _wrap(other))

    def __rand__(self, other: Any) -> Expr:
        return BinOp("and", _wrap(other), self)

    def __or__(self, other: Any) -> Expr:
        return BinOp("or", self, _wrap(other))

    def __ror__(self, other: Any) -> Expr:
        return BinOp("or", _wrap(other), self)

    def __invert__(self) -> Expr:
        return UnaryOp("not", self)

    def __neg__(self) -> Expr:
        return UnaryOp("-", self)

    def __repr__(self) -> str:
        return f"Expr({self.to_expr_string()!r})"


class Ref(Expr):
    """Reference to a feature or event field by name."""

    def __init__(self, name: str) -> None:
        self.name = name

    def to_expr_string(self) -> str:
        return self.name


class Literal(Expr):
    """A constant value (int, float, string, bool)."""

    def __init__(self, value: Any) -> None:
        self.value = value

    def to_expr_string(self) -> str:
        if isinstance(self.value, bool):
            return "true" if self.value else "false"
        if isinstance(self.value, str):
            return f"'{self.value}'"
        return str(self.value)


class BinOp(Expr):
    """Binary operation node (e.g., ``left + right``)."""

    def __init__(self, op: str, left: Expr, right: Expr) -> None:
        self.op = op
        self.left = left
        self.right = right

    def to_expr_string(self) -> str:
        return f"({self.left.to_expr_string()} {self.op} {self.right.to_expr_string()})"


class UnaryOp(Expr):
    """Unary operation node (e.g., ``not x``, ``-x``)."""

    def __init__(self, op: str, operand: Expr) -> None:
        self.op = op
        self.operand = operand

    def to_expr_string(self) -> str:
        return f"({self.op} {self.operand.to_expr_string()})"


class FnCall(Expr):
    """Function call expression (e.g., ``coalesce(x, 0)``, ``lower(s)``)."""

    def __init__(self, fn_name: str, args: list[Expr]) -> None:
        self.fn_name = fn_name
        self.args = args

    def to_expr_string(self) -> str:
        arg_strs = ", ".join(a.to_expr_string() for a in self.args)
        return f"{self.fn_name}({arg_strs})"


def _wrap(x: Any) -> Expr:
    """Wrap a Python value into an Expr node if it is not already one."""
    if isinstance(x, Expr):
        return x
    if isinstance(x, Column):
        return x._to_expr()
    return Literal(x)


def _fn(name: str, *args: Any) -> FnCall:
    """Convenience: build a FnCall wrapping all args."""
    return FnCall(name, [_wrap(a) for a in args])


# ---------------------------------------------------------------------------
# Column proxy
# ---------------------------------------------------------------------------


class Column(Expr):
    """Proxy for a column reference on a Table.

    Supports:
    - Operator overloading for expression building (inherited from Expr)
    - Aggregation methods: ``.sum(window=)``, ``.avg(window=)``, etc.
    - Serializes to a name reference in expressions
    """

    def __init__(self, table: Table, name: str) -> None:
        self.table = table
        self.name = name

    def to_expr_string(self) -> str:
        return self.name

    def _to_expr(self) -> Expr:
        return Ref(self.name)

    # --- Aggregation methods (return OperatorBase) ---

    def sum(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Sum this field in a sliding window."""
        return Sum(self.name, window=window, **kwargs)

    def avg(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Average this field in a sliding window."""
        return Avg(self.name, window=window, **kwargs)

    def mean(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Average this field (alias for avg)."""
        return Avg(self.name, window=window, **kwargs)

    def min(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Minimum of this field in a sliding window."""
        return Min(self.name, window=window, **kwargs)

    def max(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Maximum of this field in a sliding window."""
        return Max(self.name, window=window, **kwargs)

    def nunique(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Approximate unique count (HyperLogLog)."""
        return DistinctCount(self.name, window=window, **kwargs)

    def distinct_count(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Approximate unique count (alias for nunique)."""
        return DistinctCount(self.name, window=window, **kwargs)

    def last(self, **kwargs: Any) -> OperatorBase:
        """Most recent value of this field."""
        return Last(self.name, **kwargs)

    def count(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Count events where this field is present."""
        return Count(window=window, **kwargs)

    # --- Phase 16/17 operators ---

    def std(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Standard deviation in a sliding window."""
        return Stddev(self.name, window=window, **kwargs)

    def stddev(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Standard deviation (alias for std)."""
        return Stddev(self.name, window=window, **kwargs)

    def quantile(self, q: float, *, window: str, **kwargs: Any) -> OperatorBase:
        """Approximate percentile/quantile in a sliding window."""
        return Percentile(self.name, quantile=q, window=window, **kwargs)

    def percentile(self, q: float, *, window: str, **kwargs: Any) -> OperatorBase:
        """Approximate percentile (alias for quantile)."""
        return Percentile(self.name, quantile=q, window=window, **kwargs)

    def shift(self, n: int = 1) -> OperatorBase:
        """Lag/shift by N events (returns Nth-oldest value)."""
        return Lag(self.name, n=n)

    def lag(self, n: int = 1) -> OperatorBase:
        """Lag by N events (alias for shift)."""
        return Lag(self.name, n=n)

    def ewm(self, *, half_life: float, **kwargs: Any) -> OperatorBase:
        """Exponential moving average with half-life in seconds."""
        return Ema(self.name, half_life=half_life, **kwargs)

    def ema(self, *, half_life: float, **kwargs: Any) -> OperatorBase:
        """Exponential moving average (alias for ewm)."""
        return Ema(self.name, half_life=half_life, **kwargs)

    def tail(self, n: int) -> OperatorBase:
        """Last N values as a list."""
        return LastN(self.name, n=n)

    def first(self, **kwargs: Any) -> OperatorBase:
        """First-ever value seen for this entity."""
        return First(self.name, **kwargs)

    def exact_min(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Retractable exact minimum (BTreeMap-backed)."""
        return ExactMin(self.name, window=window, **kwargs)

    def exact_max(self, *, window: str, **kwargs: Any) -> OperatorBase:
        """Retractable exact maximum (BTreeMap-backed)."""
        return ExactMax(self.name, window=window, **kwargs)

    # --- Expression-based column methods ---

    def fillna(self, value: Any) -> Expr:
        """Replace Missing with a default value. Returns an Expr."""
        return _fn("coalesce", self, value)

    def clip(self, lower: Any, upper: Any) -> Expr:
        """Clamp value to [lower, upper] range. Returns an Expr."""
        return _fn("clamp", self, lower, upper)

    def lower(self) -> Expr:
        """Lowercase string. Returns an Expr."""
        return _fn("lower", self)

    def upper(self) -> Expr:
        """Uppercase string. Returns an Expr."""
        return _fn("upper", self)

    def str_len(self) -> Expr:
        """String length. Returns an Expr."""
        return _fn("len", self)

    def contains(self, needle: str) -> Expr:
        """Check if string contains substring. Returns an Expr (1.0/0.0)."""
        return _fn("contains", self, needle)

    def starts_with(self, prefix: str) -> Expr:
        """Check if string starts with prefix. Returns an Expr (1.0/0.0)."""
        return _fn("starts_with", self, prefix)

    def __repr__(self) -> str:
        return f"Column({self.table._name!r}, {self.name!r})"


class EventColumn(Column):
    """Column proxy for ``_event.field`` references.

    Created via ``table.event["field"]``. The name is prefixed with
    ``_event.`` so that the server expression evaluator accesses the
    raw event payload rather than stored features.
    """

    def __init__(self, table: Table, field: str) -> None:
        super().__init__(table, f"_event.{field}")
        self._field = field

    def __repr__(self) -> str:
        return f"EventColumn({self.table._name!r}, {self._field!r})"


class EventProxy:
    """Proxy returned by ``table.event`` for accessing raw event fields.

    Usage::

        txns.event["amount"]  # returns EventColumn with name "_event.amount"
    """

    def __init__(self, table: Table) -> None:
        self._table = table

    def __getitem__(self, field: str) -> EventColumn:
        return EventColumn(self._table, field)
