from tally._types import FeatureResult, TallyError, ConnectionError, ProtocolError
from tally._operators import (
    Count as count,
    Sum as sum,
    Avg as avg,
    Min as min,
    Max as max,
    DistinctCount as distinct_count,
    Last as last,
    Derive as derive,
    Lookup as lookup,
    Lag as lag,
    Ema as ema,
    LastN as last_n,
    First as first,
    ExactMin as exact_min,
    ExactMax as exact_max,
)
from tally._stream import stream
from tally._view import view
from tally._app import App
from tally._protocol import OP_PUSH, OP_GET, OP_SET, OP_MSET, OP_MGET, OP_REGISTER

__all__ = [
    # Types and exceptions
    "FeatureResult",
    "TallyError",
    "ConnectionError",
    "ProtocolError",
    # Operator constructors (lowercase aliases)
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "distinct_count",
    "last",
    "derive",
    "lookup",
    "lag",
    "ema",
    "last_n",
    "first",
    "exact_min",
    "exact_max",
    # Decorators
    "stream",
    "view",
    # App
    "App",
    # Protocol constants
    "OP_PUSH",
    "OP_GET",
    "OP_SET",
    "OP_MSET",
    "OP_MGET",
    "OP_REGISTER",
]
