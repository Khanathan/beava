"""DAG construction and cycle detection for v0 pipelines.

Given a collection of Stream/Table descriptors (sources + derivations),
:func:`build_dag` produces a :class:`DAG` whose edges point from each node
to its parameter-declared upstreams. The input to a derivation is captured
at registration time as ``_upstreams`` (set by ``@tl.stream`` / ``@tl.table``
function-form decorators); we trust that list and just resolve names.

:class:`DAG.topological_order` uses Kahn's algorithm; on failure it traces
the remaining subgraph and raises :class:`CycleError` with a deterministic
``A → B → C → A`` path.
"""

from __future__ import annotations

from typing import Any


class MissingDependency(Exception):
    """Raised when a derivation declares an upstream that isn't registered."""

    def __init__(self, missing: str, context: str) -> None:
        self.missing = missing
        self.context = context
        super().__init__(
            f"derivation {context!r} depends on {missing!r}, but {missing!r} "
            f"was not passed to register() / validate()"
        )


class CycleError(Exception):
    """Raised when the pipeline graph contains a cycle.

    The :attr:`cycle_path` is a list of node names with the first and last
    repeated (e.g. ``["A", "B", "C", "A"]``), suitable for joining with
    ``→`` in a user-facing message.
    """

    def __init__(self, cycle_path: list[str]) -> None:
        self.cycle_path = cycle_path
        super().__init__(
            f"Circular dependency detected: {' → '.join(cycle_path)}. "
            f"Break the cycle by removing one edge."
        )


class DAG:
    """Lightweight adjacency-map graph over descriptor names."""

    def __init__(
        self,
        nodes: dict[str, Any],
        edges: dict[str, list[str]],
    ) -> None:
        self.nodes = nodes
        self.edges = edges

    def topological_order(self) -> list[str]:
        """Return nodes in topological order (upstreams first).

        Kahn's algorithm — counts in-edges, peels off roots, re-inserts any
        node whose last remaining predecessor was just peeled. On cycle:
        trace the residual subgraph and raise CycleError with a named path.
        """
        # Edges go node → [upstreams]; in-degree here counts upstream deps
        # that must be emitted before the node. So "in-degree" = len(upstreams).
        indegree: dict[str, int] = {n: len(ups) for n, ups in self.edges.items()}
        # Build the downstream map (upstream → [dependents]) for peeling.
        downstream: dict[str, list[str]] = {n: [] for n in self.nodes}
        for node, upstreams in self.edges.items():
            for u in upstreams:
                if u in downstream:
                    downstream[u].append(node)

        # Peel roots in deterministic (alphabetical) order.
        ready = sorted([n for n, d in indegree.items() if d == 0])
        order: list[str] = []
        while ready:
            # Always consume the alphabetically-first ready node for stability.
            ready.sort()
            n = ready.pop(0)
            order.append(n)
            for dep in downstream.get(n, []):
                indegree[dep] -= 1
                if indegree[dep] == 0:
                    ready.append(dep)

        if len(order) == len(self.nodes):
            return order

        # Cycle — find one and report it.
        remaining = [n for n, d in indegree.items() if d > 0]
        cycle = _find_cycle(remaining, self.edges)
        raise CycleError(cycle)


def _find_cycle(nodes: list[str], edges: dict[str, list[str]]) -> list[str]:
    """DFS from the alphabetically-first residual node to find a cycle.

    Returns the cycle as ``[start, ..., start]``. Deterministic: starts from
    the alphabetically-smallest residual node and explores upstreams in
    alphabetical order, so the reported path is reproducible across runs.
    """
    if not nodes:
        return []
    start = sorted(nodes)[0]
    stack: list[str] = [start]
    on_stack: set[str] = {start}

    def dfs(node: str) -> list[str] | None:
        for nxt in sorted(edges.get(node, [])):
            if nxt not in nodes:
                continue
            if nxt in on_stack:
                # Found a cycle; extract the slice from nxt → ... → node → nxt.
                idx = stack.index(nxt)
                return stack[idx:] + [nxt]
            stack.append(nxt)
            on_stack.add(nxt)
            found = dfs(nxt)
            if found is not None:
                return found
            stack.pop()
            on_stack.remove(nxt)
        return None

    cycle = dfs(start)
    if cycle is not None:
        return cycle
    # Fallback (shouldn't happen if nodes are actually in a cycle): return
    # a degenerate 2-node path so CycleError still carries something useful.
    return [start, start]


def build_dag(descriptors: list[Any]) -> DAG:
    """Build the DAG from a list of Stream/Table descriptors.

    Each descriptor contributes a node keyed by ``_name``; edges come from
    ``_upstreams`` (populated by the function-form decorators). If a
    descriptor lists an upstream that isn't in ``descriptors``, this raises
    :class:`MissingDependency`.
    """
    # Index by name, by identity (for the pass where derivations reference
    # an upstream class literal directly).
    nodes: dict[str, Any] = {}
    id_to_name: dict[int, str] = {}
    for d in descriptors:
        name = getattr(d, "_name", None)
        if name is None:
            raise TypeError(
                f"descriptor {d!r} has no _name — not a valid Stream/Table"
            )
        if name in nodes and nodes[name] is not d:
            # Allow passing the same descriptor twice but not two distinct
            # descriptors colliding on a name.
            raise TypeError(
                f"two distinct descriptors share the name {name!r}"
            )
        nodes[name] = d
        id_to_name[id(d)] = name

    edges: dict[str, list[str]] = {}
    for name, d in nodes.items():
        ups = list(getattr(d, "_upstreams", []) or [])
        resolved: list[str] = []
        for u in ups:
            # Match by identity first — the upstream descriptor itself.
            u_name = id_to_name.get(id(u))
            if u_name is None:
                # Try match by ._name on whatever u is; missing if unknown.
                u_name = getattr(u, "_name", None)
                if u_name is None or u_name not in nodes:
                    raise MissingDependency(
                        missing=getattr(u, "__name__", repr(u)),
                        context=name,
                    )
            resolved.append(u_name)
        edges[name] = resolved

    return DAG(nodes=nodes, edges=edges)


__all__ = ["DAG", "MissingDependency", "CycleError", "build_dag"]
