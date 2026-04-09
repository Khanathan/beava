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
)
from tally._stream import stream
from tally._view import view
from tally._protocol import OP_PUSH, OP_GET, OP_SET, OP_MSET, OP_REGISTER

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
    # Decorators
    "stream",
    "view",
    # Protocol constants
    "OP_PUSH",
    "OP_GET",
    "OP_SET",
    "OP_MSET",
    "OP_REGISTER",
]
