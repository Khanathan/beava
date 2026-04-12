"""Pure-Python DAG validation for pipeline definitions.

Detects cycles, missing dependencies, and type mismatches without
requiring a server connection. All validation is local.

Usage::

    from tally._validate import validate, ValidationError

    errors = validate(source, dataset_a, dataset_b)
    if errors:
        for e in errors:
            print(e)
"""

from __future__ import annotations

from collections import deque


class ValidationError:
    """A validation issue found in a pipeline definition.

    Attributes:
        path: Dot-separated path showing where the error is (e.g. "A -> B").
        message: Human-readable description of the issue.
        kind: One of ``"cycle"``, ``"missing_dep"``, ``"type_mismatch"``.
    """

    def __init__(self, path: str, message: str, kind: str) -> None:
        self.path = path
        self.message = message
        self.kind = kind

    def __repr__(self) -> str:
        return f"ValidationError(kind={self.kind!r}, path={self.path!r}, message={self.message!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ValidationError):
            return NotImplemented
        return self.path == other.path and self.message == other.message and self.kind == other.kind


def _topological_sort(nodes: dict[str, list[str]]) -> list[str] | None:
    """Kahn's algorithm for topological sort.

    Args:
        nodes: Map of node name -> list of dependency names.

    Returns:
        Sorted list if acyclic, None if a cycle exists.
    """
    in_degree: dict[str, int] = {n: 0 for n in nodes}
    dependents: dict[str, list[str]] = {n: [] for n in nodes}

    for n, deps in nodes.items():
        for d in deps:
            if d in dependents:
                dependents[d].append(n)
                in_degree[n] += 1

    queue = deque(n for n, deg in in_degree.items() if deg == 0)
    order: list[str] = []

    while queue:
        n = queue.popleft()
        order.append(n)
        for dep in dependents[n]:
            in_degree[dep] -= 1
            if in_degree[dep] == 0:
                queue.append(dep)

    return order if len(order) == len(nodes) else None


def _resolve_dep_names(dep_list: list) -> list[str]:
    """Resolve a depends_on list to flat string names."""
    from tally._dataset import UnionSource

    names: list[str] = []
    for dep in dep_list:
        if isinstance(dep, UnionSource):
            names.extend(dep._get_depends_on_names())
        elif hasattr(dep, "_name"):
            names.append(dep._name)
        else:
            names.append(str(dep))
    return names


def _get_upstream_event_schema(dep_name: str, defs_map: dict) -> type | None:
    """Get the EventSet schema from an upstream source/dataset, if any."""
    dep = defs_map.get(dep_name)
    if dep is None:
        return None
    return getattr(dep, "_event_schema", None)


def validate(*definitions) -> list[ValidationError]:
    """Validate a set of pipeline definitions for correctness.

    Checks for:
    - **Cycles** in the dependency graph (kind="cycle")
    - **Missing dependencies** not in the provided definitions (kind="missing_dep")
    - **Type mismatches** where operator field references don't exist in
      the upstream EventSet schema (kind="type_mismatch")

    Args:
        *definitions: Mix of SourceDef and DatasetDef objects.

    Returns:
        List of ValidationError. Empty list means the pipeline is valid.
    """
    errors: list[ValidationError] = []

    # Build name -> definition map
    defs_map: dict[str, object] = {}
    for defn in definitions:
        name = getattr(defn, "_name", None)
        if name is not None:
            defs_map[name] = defn

    # Build adjacency graph: node -> [dependency names]
    graph: dict[str, list[str]] = {}
    for name, defn in defs_map.items():
        dep_list = getattr(defn, "_depends_on", None)
        if dep_list is not None:
            graph[name] = _resolve_dep_names(dep_list)
        else:
            graph[name] = []

    # --- Missing dependency detection ---
    for name, deps in graph.items():
        for dep_name in deps:
            if dep_name not in defs_map:
                errors.append(ValidationError(
                    path=name,
                    message=f"depends on '{dep_name}' which is not in the provided definitions",
                    kind="missing_dep",
                ))

    # --- Cycle detection (Kahn's algorithm) ---
    # Only check nodes that are in the graph (filter deps to known nodes for topo sort)
    topo_graph: dict[str, list[str]] = {}
    for name, deps in graph.items():
        topo_graph[name] = [d for d in deps if d in defs_map]

    order = _topological_sort(topo_graph)
    if order is None:
        # Find nodes involved in cycle (those with non-zero in-degree after sort attempt)
        in_degree: dict[str, int] = {n: 0 for n in topo_graph}
        for n, deps in topo_graph.items():
            for d in deps:
                if d in in_degree:
                    in_degree[d] = in_degree.get(d, 0)
                    in_degree[n] += 1

        # Re-run to find remaining
        remaining_queue = deque(n for n, deg in in_degree.items() if deg == 0)
        visited: set[str] = set()
        while remaining_queue:
            n = remaining_queue.popleft()
            visited.add(n)
            for name, deps in topo_graph.items():
                if n in deps and name not in visited:
                    in_degree[name] -= 1
                    if in_degree[name] == 0:
                        remaining_queue.append(name)

        cycle_nodes = [n for n in topo_graph if n not in visited]
        if cycle_nodes:
            cycle_path = " -> ".join(sorted(cycle_nodes))
            errors.append(ValidationError(
                path=cycle_path,
                message=f"circular dependency detected among: {', '.join(sorted(cycle_nodes))}",
                kind="cycle",
            ))

    # --- Type mismatch detection ---
    from tally._operators import OperatorBase

    for name, defn in defs_map.items():
        # Get the grouped dataset features
        grouped = getattr(defn, "_grouped_dataset", None)
        if grouped is None:
            continue

        # Find upstream event schemas
        dep_list = getattr(defn, "_depends_on", None) or []
        dep_names = _resolve_dep_names(dep_list)

        # Collect all EventSet fields from upstream sources
        upstream_fields: set[str] | None = None
        for dep_name in dep_names:
            schema = _get_upstream_event_schema(dep_name, defs_map)
            if schema is not None:
                if upstream_fields is None:
                    upstream_fields = set()
                upstream_fields.update(schema._fields.keys())

        if upstream_fields is None:
            # No EventSet defined upstream, skip type checking
            continue

        # Check each operator's field reference
        for feat_name, op in grouped._features.items():
            field_ref = getattr(op, "field", None)
            if field_ref is not None and field_ref not in upstream_fields:
                errors.append(ValidationError(
                    path=f"{name}.{feat_name}",
                    message=f"operator references field '{field_ref}' not found in upstream EventSet (available: {sorted(upstream_fields)})",
                    kind="type_mismatch",
                ))

        # Also check extra features
        extra = getattr(defn, "_extra_features", None) or {}
        for feat_name, op in extra.items():
            field_ref = getattr(op, "field", None)
            if field_ref is not None and field_ref not in upstream_fields:
                errors.append(ValidationError(
                    path=f"{name}.{feat_name}",
                    message=f"operator references field '{field_ref}' not found in upstream EventSet (available: {sorted(upstream_fields)})",
                    kind="type_mismatch",
                ))

    return errors
