#!/usr/bin/env python3
"""
Phase 13.0 wire-spec validation harness.

Loads every JSON Schema in examples/wire/schemas/ and asserts every
worked example in examples/wire/<name>-<scenario>.<request|response|error>.json
validates against its corresponding schema.

Filename → schema inference:
- examples/wire/<op>-<scenario>.request.json → examples/wire/schemas/<op>.request.schema.json
- examples/wire/<op>-<scenario>.response.json → examples/wire/schemas/<op>.response.schema.json
- examples/wire/<op>-<scenario>.error.json → examples/wire/schemas/error.schema.json (shared envelope)
- examples/wire/<op>-request.json (no scenario tag) → examples/wire/schemas/<op>.request.schema.json
- examples/wire/<op>-response.json (no scenario tag) → examples/wire/schemas/<op>.response.schema.json

Exits 0 if all examples validate; exits 1 with diagnostics otherwise.

Used as a CI gate by Phase 13.4's Rust validation harness (different
implementation, same fixtures); the SDK ports in 13.5 (Python) and 13.6
(TypeScript + Go) consume the same fixtures via language-native validators.
"""
from __future__ import annotations

import json
import pathlib
import sys

try:
    from jsonschema import Draft202012Validator
except ImportError:  # pragma: no cover
    print("Install jsonschema: pip install 'jsonschema>=4.0'", file=sys.stderr)
    sys.exit(2)


WIRE_DIR = pathlib.Path(__file__).parent
SCHEMAS_DIR = WIRE_DIR / "schemas"


def schema_for_example(example_path: pathlib.Path) -> pathlib.Path | None:
    """Infer the schema path for a given example file.

    Conventions:
    - <name>.error.json → error.schema.json (shared envelope)
    - <op>-<scenario>.<request|response>.json → <op>.<request|response>.schema.json
    - <op>-request.json / <op>-response.json (no scenario tag) → <op>.<request|response>.schema.json
    """
    name = example_path.stem  # e.g. "register-fraud-team.request"
    if name.endswith(".error"):
        return SCHEMAS_DIR / "error.schema.json"
    if "." not in name:
        # No <kind> dot suffix — pattern is "<op>-request" or "<op>-response"
        if name.endswith("-request"):
            op = name[: -len("-request")]
            return SCHEMAS_DIR / f"{op}.request.schema.json"
        if name.endswith("-response"):
            op = name[: -len("-response")]
            return SCHEMAS_DIR / f"{op}.response.schema.json"
        return None
    base, kind = name.rsplit(".", 1)  # "register-fraud-team", "request"
    op = base.split("-", 1)[0]  # "register"
    return SCHEMAS_DIR / f"{op}.{kind}.schema.json"


def main() -> int:
    errors: list[str] = []
    examples = sorted(p for p in WIRE_DIR.glob("*.json") if p.parent == WIRE_DIR)
    if not examples:
        print("No examples found under examples/wire/", file=sys.stderr)
        return 1

    for example_path in examples:
        schema_path = schema_for_example(example_path)
        if schema_path is None or not schema_path.exists():
            errors.append(
                f"NO SCHEMA for {example_path.name} (looked for {schema_path})"
            )
            continue
        with open(schema_path) as f:
            schema = json.load(f)
        with open(example_path) as f:
            instance = json.load(f)
        validator = Draft202012Validator(schema)
        validation_errors = sorted(
            validator.iter_errors(instance), key=lambda e: list(e.path)
        )
        if validation_errors:
            for ve in validation_errors:
                path_str = "/".join(str(p) for p in ve.path) or "<root>"
                errors.append(
                    f"INVALID {example_path.name} (schema={schema_path.name}) "
                    f"at {path_str}: {ve.message}"
                )

    if errors:
        print("FAIL — wire-spec example validation:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1

    print(f"OK — all {len(examples)} examples validate against their schemas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
