from tally._types import FeatureResult, TallyError, ConnectionError, ProtocolError
from tally._operators import (
    Count as count,
    Sum as sum,
    Avg as avg,
    Min as min,
    Max as max,
    DistinctCount as distinct_count,
    Last as last,
    Stddev as stddev,
    Percentile as percentile,
    Derive as derive,
    Lookup as lookup,
    Lag as lag,
    Ema as ema,
    LastN as last_n,
    First as first,
    ExactMin as exact_min,
    ExactMax as exact_max,
)
from tally._app import App
from tally._protocol import OP_PUSH, OP_GET, OP_SET, OP_MSET, OP_MGET, OP_REGISTER

# New API (v2.0)
from tally._schema import EventSet, FeatureSet, Field
from tally._source import source
from tally._dataset import dataset, group_by, union
from tally._validate import validate, ValidationError

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
    "stddev",
    "percentile",
    "derive",
    "lookup",
    "lag",
    "ema",
    "last_n",
    "first",
    "exact_min",
    "exact_max",
    # App
    "App",
    # Protocol constants
    "OP_PUSH",
    "OP_GET",
    "OP_SET",
    "OP_MSET",
    "OP_MGET",
    "OP_REGISTER",
    # New API (v2.0)
    "EventSet",
    "FeatureSet",
    "Field",
    "source",
    "dataset",
    "group_by",
    "union",
    "validate",
    "ValidationError",
]
