"""Tally Python SDK — public surface.

v0 declarative API:

    import tally as tl

    @tl.stream
    class Clicks:
        user_id: str
        url: str

    @tl.table(key="user_id")
    class Users:
        user_id: str
        name: str

    tl.col("amount") > 100     # expression DSL

The old v2.0 ``@tl.source`` / ``@tl.dataset`` / ``EventSet`` / ``FeatureSet``
decorators are deleted as of Plan 21-01; aggregation / join operators come
back in Plans 21-02 and 21-03.
"""

from tally._types import FeatureResult, TallyError, ConnectionError, ProtocolError
from tally._app import App
from tally._protocol import (
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
from tally._types_core import Optional, Field
from tally._col import col
from tally._stream import stream, Stream, StreamSource, StreamDerivation
from tally._table import table, Table, TableSource, TableDerivation
from tally._validate_v0 import validate, ValidationError

# Plan 21-03: tl.union stub.
from tally._union import union

# Plan 21-03: aggregation operator descriptors.
from tally._agg_ops import (
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
from tally._operators import OperatorBase

__all__ = [
    # Types & exceptions
    "FeatureResult",
    "TallyError",
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
]
