"""Beava Python SDK — public surface.

v0 declarative API:

    import beava as bv

    @bv.stream
    class Clicks:
        user_id: str
        url: str

    @bv.table(key="user_id")
    class Users:
        user_id: str
        name: str

    bv.col("amount") > 100     # expression DSL

The pre-v0 class-decorator surface (source / dataset decorators and
the schema / feature-bundle types that backed them) was removed in
Plan 21-01; aggregation and join operators return in Plans 21-02 and
21-03 in function form on top of ``@bv.stream`` / ``@bv.table``.
"""

from beava._types import FeatureResult, ConnectionError, ProtocolError, BeavaError

# Phase 38-01 mothball: the `beava._native` PyO3 extension (Plan 30-01) is
# gone. Scientists now run the scoped replica via `beava fork` (Phase 37)
# and talk to it through the pure-Python `beava.App` over HTTP/TCP — no
# native extension required.
from beava._app import App
from beava._protocol import (
    OP_PUSH,
    OP_GET,
    OP_SET,
    OP_MSET,
    OP_MGET,
    OP_REGISTER,
    OP_GET_MULTI,
    OP_SCAN_RESERVED,
    OP_SUBSCRIBE_RESERVED,
)

# v0 public surface
from beava._types_core import Optional, Field
from beava._col import col
from beava._stream import stream, Stream, StreamSource, StreamDerivation
from beava._table import table, Table, TableSource, TableDerivation
from beava._validate_v0 import validate, ValidationError

# Plan 21-03: bv.union stub.
from beava._union import union

# Plan 21-03: aggregation operator descriptors.
from beava._agg_ops import (
    count,
    sum,
    avg,
    min,
    max,
    variance,
    stddev,
    percentile,
    count_distinct,
    top_k,
    first,
    last,
    first_n,
    last_n,
    ema,
    lag,
)

# OperatorBase is still referenced by Plan 21-03 aggregation-spec descriptors.
from beava._operators import OperatorBase

# Phase 39-01: Python-native `bv.fork()` DX layer over the Phase 37 CLI.
from beava._fork import (
    fork,
    ForkedReplica,
    ForkError,
    ForkValidationError,
    ForkTimeoutError,
    ForkSubprocessError,
)

__all__ = [
    # Types & exceptions
    "FeatureResult",
    "BeavaError",
    "ConnectionError",
    "ProtocolError",
    # App
    "App",
    # Protocol constants
    "OP_PUSH",
    "OP_GET",
    "OP_SET",
    "OP_MSET",
    "OP_MGET",
    "OP_REGISTER",
    "OP_GET_MULTI",
    "OP_SCAN_RESERVED",
    "OP_SUBSCRIBE_RESERVED",
    # v0 declarative API
    "stream",
    "table",
    "Optional",
    "Field",
    "col",
    "Stream",
    "Table",
    "StreamSource",
    "StreamDerivation",
    "TableSource",
    "TableDerivation",
    "validate",
    "ValidationError",
    # Union stub (Plan 21-03)
    "union",
    # Aggregation operators (Plan 21-03)
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "variance",
    "stddev",
    "percentile",
    "count_distinct",
    "top_k",
    "first",
    "last",
    "first_n",
    "last_n",
    "ema",
    "lag",
    # Internal (used by Plan 21-03)
    "OperatorBase",
    # Python-native fork DX (Phase 39-01)
    "fork",
    "ForkedReplica",
    "ForkError",
    "ForkValidationError",
    "ForkTimeoutError",
    "ForkSubprocessError",
]
