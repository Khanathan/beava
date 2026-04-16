"""Local pipeline validation — DAG build + cycle detection + schema re-check.

``validate(*descriptors)`` returns a list of :class:`ValidationError`. It
performs no TCP work; safe for unit tests and for ``App.validate()``.

``App.register`` calls ``validate`` first; if any errors come back, it
raises the first one (with a tail count in the message) and sends nothing.
Otherwise it topologically orders the descriptors, calls
``_collect_registrations`` on each, dedupes by name, and forwards each
REGISTER frame to the underlying client.
"""

from __future__ import annotations

from typing import Any

from beava._dag import CycleError, MissingDependency, build_dag


class ValidationError(Exception):
    """Surgical validation error with kind / path / message structure.

    ``kind`` ∈ {``cycle``, ``missing_dep``, ``schema_mismatch``,
    ``bad_return_type``}. ``path`` is a human-readable pipeline location
    (e.g. ``"UserTxns → Transactions"`` or ``"Checkouts.filter"``).
    ``message`` is the surgical error body.
    """

    def __init__(self, kind: str, path: str, message: str) -> None:
        self.kind = kind
        self.path = path
        self.message = message
        super().__init__(self.__str__())

    def __str__(self) -> str:
        return f"[{self.kind}] at {self.path}: {self.message}"


def _reparse_referenced_fields(expr: str) -> set[str]:
    """Best-effort extraction of bare identifiers from a serialized expr string.

    Strips single-quoted string literals (with ``\\'`` / ``\\\\`` escapes)
    and ``cast(x, <type>)`` target identifiers before tokenizing, so
    ``page == '/checkout'`` doesn't spuriously report ``checkout`` as a
    missing field. Identifiers that match cast target names (``int`` /
    ``float`` / ``str`` / ``bool``) are filtered as keywords.
    """
    import re
    KEYWORDS = {
        "and", "or", "not", "true", "false", "null",
        "cast", "int", "float", "str", "bool",
    }
    # Strip single-quoted string literals (honour \' and \\ escapes).
    stripped = re.sub(r"'(?:\\.|[^'\\])*'", "''", expr)
    toks = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", stripped)
    return {t for t in toks if t not in KEYWORDS}


def _propagate_and_check_ops(
    desc: Any,
    initial_schema: dict[str, Any],
    errors: list[ValidationError],
) -> None:
    """Re-run the op chain against ``initial_schema``; append schema_mismatch
    errors for any field reference that doesn't resolve."""
    schema_fields = set(initial_schema.keys())
    name = desc._name
    for op_idx, op in enumerate(desc._ops):
        op_kind = op.get("op")
        if op_kind == "filter":
            refs = _reparse_referenced_fields(op.get("expr", ""))
            missing = [r for r in refs if r not in schema_fields]
            for r in missing:
                errors.append(
                    ValidationError(
                        kind="schema_mismatch",
                        path=f"{name}.{op_kind}[{op_idx}]",
                        message=(
                            f"field {r!r} not in {name} (after preceding ops); "
                            f"available: [{', '.join(sorted(schema_fields))}]"
                        ),
                    )
                )
        elif op_kind == "select":
            fields = op.get("fields", [])
            for f in fields:
                if f not in schema_fields:
                    errors.append(
                        ValidationError(
                            kind="schema_mismatch",
                            path=f"{name}.{op_kind}[{op_idx}]",
                            message=f"field {f!r} not in {name}",
                        )
                    )
            schema_fields = set(fields) & schema_fields
        elif op_kind == "drop":
            fields = op.get("fields", [])
            schema_fields = schema_fields - set(fields)
        elif op_kind == "rename":
            mapping = op.get("mapping", {})
            for old, new in mapping.items():
                if old in schema_fields:
                    schema_fields.discard(old)
                    schema_fields.add(new)
        elif op_kind == "with_columns":
            exprs = op.get("exprs", {})
            for new_name, expr_str in exprs.items():
                refs = _reparse_referenced_fields(expr_str)
                for r in refs:
                    if r not in schema_fields:
                        errors.append(
                            ValidationError(
                                kind="schema_mismatch",
                                path=f"{name}.{op_kind}[{op_idx}]",
                                message=f"field {r!r} not in {name}",
                            )
                        )
                schema_fields.add(new_name)
        elif op_kind == "cast":
            # Type changes only — no field reference additions.
            pass
        elif op_kind == "fillna":
            pass


def validate(*descriptors: Any) -> list[ValidationError]:
    """Validate a pipeline without any network IO.

    Runs, in order:
      1. DAG construction (surfaces :class:`MissingDependency`).
      2. Topological order (surfaces :class:`CycleError`).
      3. Schema propagation through each derivation's ``_ops``.

    Returns a list of :class:`ValidationError`; empty on success.
    """
    errors: list[ValidationError] = []
    desc_list = list(descriptors)

    try:
        dag = build_dag(desc_list)
    except MissingDependency as e:
        errors.append(
            ValidationError(
                kind="missing_dep",
                path=e.context,
                message=str(e),
            )
        )
        return errors
    except TypeError as e:
        errors.append(
            ValidationError(kind="bad_return_type", path="<dag>", message=str(e))
        )
        return errors

    try:
        order = dag.topological_order()
    except CycleError as e:
        errors.append(
            ValidationError(
                kind="cycle",
                path=" → ".join(e.cycle_path),
                message=str(e),
            )
        )
        return errors

    # Re-check schema propagation per derivation (defence in depth — the op
    # constructors already validated, but validate() should not trust call-site
    # checks in case a descriptor was mutated post-construction).
    for node_name in order:
        desc = dag.nodes[node_name]
        ops = getattr(desc, "_ops", None)
        if not ops:
            continue
        # The derivation's starting schema is the direct upstream's output
        # schema. A derivation may have multiple upstreams; we pick the first
        # (matching how stateless ops chain onto _upstream). For multi-input
        # operators that arrive in 21-03, this will be augmented.
        upstreams = getattr(desc, "_upstreams", []) or []
        if upstreams:
            base = upstreams[0]
            base_schema = dict(getattr(base, "_schema", {}))
        else:
            base_schema = dict(getattr(desc, "_schema", {}))
        _propagate_and_check_ops(desc, base_schema, errors)

    return errors


__all__ = ["validate", "ValidationError"]
