"""Phase 13.5 Plan 06: bundled demo dataset round-trip tests."""
from __future__ import annotations

import importlib.resources
import json

import pytest

import beava as bv


@pytest.mark.parametrize("name", ["adtech", "fraud", "ecommerce"])
def test_demo_loads(name: str) -> None:
    """``bv.demo('<name>')`` must load successfully (no RuntimeError post-Plan-06)."""
    result = bv.demo(name)
    assert isinstance(result, dict)
    assert result["name"] == name
    assert "schema" in result
    assert "events" in result


@pytest.mark.parametrize("name", ["adtech", "fraud", "ecommerce"])
def test_demo_event_count(name: str) -> None:
    """Each dataset has >= 1000 events (target ~10K per D-02)."""
    result = bv.demo(name)
    assert len(result["events"]) >= 1000, (
        f"{name} has {len(result['events'])} events; target >= 1000"
    )


@pytest.mark.parametrize("name", ["adtech", "fraud", "ecommerce"])
def test_demo_schema_is_list_of_descriptors(name: str) -> None:
    result = bv.demo(name)
    schema = result["schema"]
    assert isinstance(schema, list)
    assert len(schema) >= 1
    for desc in schema:
        assert "kind" in desc
        assert "name" in desc


def test_adtech_has_impression_click_conversion() -> None:
    schema = bv.demo("adtech")["schema"]
    names = {d["name"] for d in schema if d.get("kind") == "event"}
    assert "Impression" in names
    assert "Click" in names
    assert "Conversion" in names


def test_fraud_has_txn_login() -> None:
    schema = bv.demo("fraud")["schema"]
    names = {d["name"] for d in schema if d.get("kind") == "event"}
    assert "Txn" in names
    assert "Login" in names


def test_ecommerce_has_pageview_addtocart_purchase() -> None:
    schema = bv.demo("ecommerce")["schema"]
    names = {d["name"] for d in schema if d.get("kind") == "event"}
    assert "PageView" in names
    assert "AddToCart" in names
    assert "Purchase" in names


def test_event_lines_are_valid_json() -> None:
    """Every events.jsonl line must parse as JSON."""
    for name in ("adtech", "fraud", "ecommerce"):
        ref = importlib.resources.files("beava.demos") / name / "events.jsonl"
        with ref.open("r") as f:
            for i, line in enumerate(f):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    json.loads(stripped)
                except json.JSONDecodeError as e:
                    pytest.fail(f"{name}/events.jsonl line {i}: invalid JSON: {e}")
