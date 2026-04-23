"""Phase 3 SDK REGISTER-compile bench (plan 05.5-05). RED state — pytest-benchmark
is not yet installed; this test errors on fixture lookup until Task 1.b lands.
"""

from __future__ import annotations

import json

import pytest

import beava as bv
from beava._validate import topo_sort, validate_descriptors


def _build_10_descriptor_dag() -> list[object]:
    """Return a list of exactly 10 decorator-produced descriptors."""
    # FILL AT WRITE TIME — see python/tests/test_phase3_smoke.py / test_app.py
    # for multi-descriptor DAG patterns. Must produce 10 distinct objects.
    raise NotImplementedError("populated in Task 1.b")


@pytest.mark.bench
def test_register_compile_10_descriptors(benchmark):  # type: ignore[no-untyped-def]
    descriptors = _build_10_descriptor_dag()
    assert len(descriptors) == 10

    def compile_register_json() -> bytes:
        errs = validate_descriptors(descriptors)
        assert errs == []
        sorted_descs = topo_sort(descriptors)
        payload = {"nodes": [d._to_register_json() for d in sorted_descs]}
        return json.dumps(payload).encode("utf-8")

    result = benchmark(compile_register_json)
    # Sanity: output is non-empty JSON bytes.
    assert result.startswith(b"{")
    assert b'"nodes"' in result
