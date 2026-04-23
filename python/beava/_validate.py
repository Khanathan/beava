"""Local pipeline validation — DAG topo-sort, cycle detection, and schema checks.

``validate_descriptors(descriptors)`` returns a list of :class:`ValidationError`
without any network I/O.  ``topo_sort(descriptors)`` returns the same list ordered
so every upstream appears before its dependents (Kahn's algorithm).

Both are consumed by :class:`beava._app.App`:
  - ``App.validate(*descs)`` delegates to ``validate_descriptors`` and returns the list.
  - ``App.register(*descs)`` calls ``validate_descriptors`` first; if the list is
    non-empty it raises ``RegistrationError``; otherwise it calls ``topo_sort`` and
    dispatches the REGISTER payload.

Validation rules (Phase 3 scope — mirrors server register_validate.rs):
  1. duplicate_name          — two descriptors with the same ``_name`` in one batch.
  2. missing_upstream        — a derivation's upstream name not present in the batch
                               (Phase 3 only; server additionally checks the registry).
  3. cycle                   — DFS three-color cycle detection on the upstream graph.
  4. unknown_field_type      — a schema field's type string is not one of the 6 valid ones.
  5. event_time_field_invalid — event_time_field set but field is missing or wrong type.
  6. table_key_invalid        — primary_key field(s) not all in schema.
  7. bad_return_type          — derivation's _beava_kind is not one of the three valid values.
  8. schema_mismatch          — Phase 3 no-op placeholder (stateless ops land in Phase 4).

Rules NOT checked here (server-only; require registry state):
  - registration_conflict / additive_only
  - name reserved-prefix / length / pattern
"""

from __future__ import annotations

import datetime as _dt
from collections import deque
from typing import Any, Sequence

from beava._errors import ValidationError

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_FIELD_TYPES: frozenset[str] = frozenset(
    {"str", "i64", "f64", "bool", "bytes", "datetime"}
)

_VALID_EVENT_TIME_TYPES: frozenset[str] = frozenset({"i64", "datetime"})

_VALID_BEAVA_KINDS: frozenset[str] = frozenset({"event", "table", "derivation"})

# Mapping from Python type objects to wire type strings (defensive re-check).
# Mirrors _types.py, but we compare the FieldSpec.py_type rather than re-doing
# type-string conversion (which already happened at decoration time).
_PY_TYPE_TO_WIRE: dict[type, str] = {
    str: "str",
    int: "i64",
    float: "f64",
    bool: "bool",
    bytes: "bytes",
    _dt.datetime: "datetime",
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _descriptor_name(desc: Any) -> str:
    """Return the descriptor's _name attribute (guaranteed by EventSource/TableSource/etc.)."""
    return str(desc._name)


def _descriptor_upstreams(desc: Any) -> list[str]:
    """Return the descriptor's _upstreams list (always list[str] per Phase 3 contracts)."""
    ups: list[str] = getattr(desc, "_upstreams", [])
    return list(ups)


def _get_schema_fields(desc: Any) -> dict[str, Any]:
    """Return the schema dict ({field_name: FieldSpec}) for the descriptor."""
    return dict(getattr(desc, "_schema", {}))


def _get_event_time_field(desc: Any) -> str | None:
    """Return the event_time_field name or None."""
    val: str | None = getattr(desc, "_event_time_field", None)
    return val


def _get_primary_key(desc: Any) -> list[str]:
    """Return primary_key list (TableSource) or empty list."""
    key: list[str] = getattr(desc, "_primary_key", [])
    return list(key)


def _get_beava_kind(desc: Any) -> str:
    """Return the _beava_kind attribute (e.g. 'event', 'table', 'derivation')."""
    return str(getattr(desc, "_beava_kind", ""))


# ---------------------------------------------------------------------------
# DFS three-color cycle detection
# ---------------------------------------------------------------------------

_WHITE = 0  # unvisited
_GRAY = 1  # in current DFS path
_BLACK = 2  # fully explored


def _detect_cycle_dfs(graph: dict[str, set[str]]) -> list[str] | None:
    """DFS three-color algorithm.  Returns the cycle path as a list of names,
    or None if no cycle exists.

    The returned list starts and ends with the same node (e.g. ["A", "B", "A"]).
    """
    color: dict[str, int] = {n: _WHITE for n in graph}
    parent: dict[str, str | None] = {n: None for n in graph}

    def _dfs(node: str) -> list[str] | None:
        color[node] = _GRAY
        for neighbor in sorted(graph.get(node, set())):  # sorted for determinism
            if neighbor not in color:
                # External node (missing_upstream will catch this); skip.
                continue
            if color[neighbor] == _GRAY:
                # Found a back-edge → cycle.  Reconstruct the cycle path.
                cycle: list[str] = [neighbor, node]
                cur: str | None = node
                while cur is not None and cur != neighbor:
                    cur = parent.get(cur)
                    if cur is not None:
                        cycle.append(cur)
                cycle.reverse()
                cycle.append(neighbor)
                return cycle
            if color[neighbor] == _WHITE:
                parent[neighbor] = node
                result = _dfs(neighbor)
                if result is not None:
                    return result
        color[node] = _BLACK
        return None

    for node in list(graph.keys()):
        if color[node] == _WHITE:
            result = _dfs(node)
            if result is not None:
                return result
    return None


# ---------------------------------------------------------------------------
# Kahn's algorithm — topological sort
# ---------------------------------------------------------------------------


def topo_sort(descriptors: Sequence[Any]) -> list[Any]:
    """Sort descriptors so every upstream appears before its dependents (Kahn's algorithm).

    Raises ``ValidationError(kind='cycle', ...)`` if the graph contains a cycle.
    Preserves input order as the tiebreaker for nodes with the same in-degree.

    Args:
        descriptors: Sequence of descriptor objects (EventSource, TableSource, etc.)

    Returns:
        New list in dependency order.

    Raises:
        ValidationError: If a cycle is detected.
    """
    desc_list = list(descriptors)
    name_to_desc: dict[str, Any] = {_descriptor_name(d): d for d in desc_list}
    names_in_order = [_descriptor_name(d) for d in desc_list]

    # Build in-degree map and reverse adjacency (who depends on this node).
    in_degree: dict[str, int] = {n: 0 for n in names_in_order}
    dependents: dict[str, list[str]] = {n: [] for n in names_in_order}

    for desc in desc_list:
        name = _descriptor_name(desc)
        for upstream in _descriptor_upstreams(desc):
            if upstream in name_to_desc:
                in_degree[name] += 1
                dependents[upstream].append(name)

    # Enqueue nodes with zero in-degree, in input order (stable tiebreaker).
    queue: deque[str] = deque(n for n in names_in_order if in_degree[n] == 0)
    sorted_names: list[str] = []

    while queue:
        node = queue.popleft()
        sorted_names.append(node)
        # Process dependents in their original input order for determinism.
        for dep in sorted(dependents[node], key=lambda n: names_in_order.index(n)):
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)

    if len(sorted_names) < len(desc_list):
        # Cycle — build the graph and detect.
        graph: dict[str, set[str]] = {}
        for desc in desc_list:
            name = _descriptor_name(desc)
            graph[name] = {
                u for u in _descriptor_upstreams(desc) if u in name_to_desc
            }
        cycle_path = _detect_cycle_dfs(graph)
        if cycle_path:
            path_str = " -> ".join(cycle_path)
            err = ValidationError(
                kind="cycle",
                path=path_str,
                message=f"dependency cycle detected: {path_str}",
            )
            raise ValueError(str(err)) from None
        # Fallback: cycle exists but DFS couldn't reconstruct it
        err_fallback = ValidationError(
            kind="cycle",
            path="(unknown)",
            message="dependency cycle detected (could not reconstruct path)",
        )
        raise ValueError(str(err_fallback)) from None

    return [name_to_desc[n] for n in sorted_names]


# ---------------------------------------------------------------------------
# Per-descriptor defensive checks
# ---------------------------------------------------------------------------


def _check_field_types(desc: Any, errors: list[ValidationError]) -> None:
    """Rule 4: all schema field types must be one of the 6 valid wire types."""
    from beava._types import py_type_to_field_type

    name = _descriptor_name(desc)
    schema = _get_schema_fields(desc)
    for field_name, spec in schema.items():
        try:
            wire_type = py_type_to_field_type(spec.py_type)
        except (TypeError, KeyError):
            wire_type = None
        if wire_type is None or wire_type not in _VALID_FIELD_TYPES:
            errors.append(
                ValidationError(
                    kind="unknown_field_type",
                    path=f"{name}.{field_name}",
                    message=(
                        f"field {field_name!r} has unsupported type {spec.py_type!r}; "
                        f"supported: {sorted(_VALID_FIELD_TYPES)}"
                    ),
                )
            )


def _check_event_time_field(desc: Any, errors: list[ValidationError]) -> None:
    """Rule 5: event_time_field (if set) must exist in schema with type i64 or datetime."""
    from beava._types import py_type_to_field_type

    name = _descriptor_name(desc)
    etf = _get_event_time_field(desc)
    if etf is None:
        return
    schema = _get_schema_fields(desc)
    if etf not in schema:
        errors.append(
            ValidationError(
                kind="event_time_field_invalid",
                path=f"{name}.{etf}",
                message=(
                    f"event_time_field {etf!r} is not declared in schema; "
                    f"available: {sorted(schema.keys())}"
                ),
            )
        )
        return
    spec = schema[etf]
    try:
        wire_type = py_type_to_field_type(spec.py_type)
    except (TypeError, KeyError):
        wire_type = None
    if wire_type not in _VALID_EVENT_TIME_TYPES:
        errors.append(
            ValidationError(
                kind="event_time_field_invalid",
                path=f"{name}.{etf}",
                message=(
                    f"event_time_field {etf!r} must be i64 or datetime, "
                    f"got {wire_type!r}"
                ),
            )
        )


def _check_table_primary_key(desc: Any, errors: list[ValidationError]) -> None:
    """Rule 6: all primary_key fields must exist in schema."""
    name = _descriptor_name(desc)
    pk = _get_primary_key(desc)
    if not pk:
        return
    schema = _get_schema_fields(desc)
    for k in pk:
        if k not in schema:
            errors.append(
                ValidationError(
                    kind="table_key_invalid",
                    path=f"{name}.primary_key",
                    message=(
                        f"primary_key field {k!r} is not in schema; "
                        f"available: {sorted(schema.keys())}"
                    ),
                )
            )


def _check_bad_return_type(desc: Any, errors: list[ValidationError]) -> None:
    """Rule 7: _beava_kind must be one of the valid kinds."""
    name = _descriptor_name(desc)
    kind = _get_beava_kind(desc)
    if kind not in _VALID_BEAVA_KINDS:
        errors.append(
            ValidationError(
                kind="bad_return_type",
                path=name,
                message=(
                    f"descriptor {name!r} has invalid _beava_kind {kind!r}; "
                    f"must be one of {sorted(_VALID_BEAVA_KINDS)}"
                ),
            )
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def validate_descriptors(descriptors: Sequence[Any]) -> list[ValidationError]:
    """Validate a batch of descriptors locally, without any network I/O.

    Checks all Phase 3 local rules (fail-soft — collects all errors):
      1. duplicate_name       — two descriptors with the same ``_name``
      2. missing_upstream     — a derivation references a name not in the batch
      3. cycle                — DFS cycle detection on the upstream graph
      4. unknown_field_type   — unsupported field type in any schema
      5. event_time_field_invalid — bad event_time_field
      6. table_key_invalid    — primary_key fields not in schema
      7. bad_return_type      — invalid _beava_kind
      8. schema_mismatch      — Phase 3 no-op placeholder (ops are empty in Phase 3)

    Returns:
        list[ValidationError] — empty list means the batch is valid.
    """
    desc_list = list(descriptors)
    errors: list[ValidationError] = []

    # Rule 1: duplicate names
    seen_names: dict[str, int] = {}
    for desc in desc_list:
        n = _descriptor_name(desc)
        if n in seen_names:
            errors.append(
                ValidationError(
                    kind="duplicate_name",
                    path=n,
                    message=f"duplicate descriptor name {n!r} in registration batch",
                )
            )
        else:
            seen_names[n] = 1

    # Collect unique names for upstream checks (use first occurrence)
    name_set: set[str] = set(seen_names.keys())

    # Rules 4-7: per-descriptor checks
    for desc in desc_list:
        _check_field_types(desc, errors)
        _check_event_time_field(desc, errors)
        _check_table_primary_key(desc, errors)
        _check_bad_return_type(desc, errors)

    # Rule 2: missing upstream (derivations only)
    for desc in desc_list:
        ups = _descriptor_upstreams(desc)
        if not ups:
            continue
        name = _descriptor_name(desc)
        for upstream_name in ups:
            if upstream_name not in name_set:
                errors.append(
                    ValidationError(
                        kind="missing_upstream",
                        path=name,
                        message=(
                            f"upstream {upstream_name!r} is not in the registration "
                            f"batch; declare it in the same register() call or ensure "
                            f"it was already registered"
                        ),
                    )
                )

    # Rule 3: cycle detection (skip if duplicates exist — graph is already malformed)
    # Build the graph from valid (unique) nodes only.
    unique_descs = []
    seen2: set[str] = set()
    for desc in desc_list:
        n = _descriptor_name(desc)
        if n not in seen2:
            unique_descs.append(desc)
            seen2.add(n)

    graph: dict[str, set[str]] = {}
    for desc in unique_descs:
        name = _descriptor_name(desc)
        graph[name] = {u for u in _descriptor_upstreams(desc) if u in name_set}

    cycle_path = _detect_cycle_dfs(graph)
    if cycle_path:
        path_str = " -> ".join(cycle_path)
        errors.append(
            ValidationError(
                kind="cycle",
                path=path_str,
                message=f"dependency cycle detected: {path_str}",
            )
        )

    # Rule 8: schema_mismatch — Phase 3 no-op placeholder (ops empty in Phase 3).
    # Phase 4 will walk each derivation's _ops chain here.

    return errors
