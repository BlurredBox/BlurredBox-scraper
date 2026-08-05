"""The d=5 guard and the must-collect coverage gate."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from scraperd import schema

MUST_COLLECT = Path(__file__).resolve().parents[1] / "must-collect.md"


def test_core_v1_is_frozen():
    assert schema.CORE_V1 == (
        "cpu_usage",
        "memory_usage_mb",
        "gpu_usage",
        "gpu_vram_mb",
        "latency_ms",
    )
    assert len(schema.CORE_V1) == 5


def test_core_row_builds_in_order():
    row = {
        "cpu_usage": 12.5,
        "memory_usage_mb": 900.0,
        "gpu_usage": 40.0,
        "gpu_vram_mb": 2048.0,
        "latency_ms": 31.7,
        "request_id": "abc",  # extra keys are ignored, never appended
    }
    assert schema.core_row(row) == [12.5, 900.0, 40.0, 2048.0, 31.7]


@pytest.mark.parametrize("missing", schema.CORE_V1)
def test_core_row_raises_on_missing_field(missing):
    row = dict.fromkeys(schema.CORE_V1, 1.0)
    del row[missing]
    with pytest.raises(KeyError, match=missing):
        schema.core_row(row)


def test_core_row_raises_on_null_latency():
    row = dict.fromkeys(schema.CORE_V1, 1.0) | {"latency_ms": None}
    with pytest.raises(KeyError, match="latency_ms"):
        schema.core_row(row)


def test_samples_table_has_no_latency():
    assert "latency_ms" not in schema.SAMPLE_FIELDS
    assert "latency_ms" not in schema.SAMPLES_SCHEMA.names


def test_requests_table_covers_the_full_d5():
    assert set(schema.CORE_V1) <= set(schema.REQUEST_FIELDS)


def test_deprecated_alias_is_never_written():
    for name in schema.ALIASES:
        assert name not in schema.SAMPLE_FIELDS
        assert name not in schema.REQUEST_FIELDS


def _must_collect_field_names() -> set[str]:
    """Every backticked lowercase identifier in must-collect.md.

    Paths, types and kwargs carry `/ . : = [ ]` or capitals and drop out, so
    what survives is field names.
    """
    text = MUST_COLLECT.read_text()
    return {
        tok for tok in re.findall(r"`([^`]+)`", text)
        if re.fullmatch(r"[a-z][a-z0-9_]*", tok)
    }


def test_every_must_collect_field_is_declared():
    undeclared = _must_collect_field_names() - schema.DECLARED_FIELDS
    assert not undeclared, (
        f"must-collect.md names {sorted(undeclared)}, declared nowhere in "
        "schema.py. Add each to SAMPLE_FIELDS/REQUEST_FIELDS/MANIFEST_FIELDS "
        "if collected, or to DEFERRED_FIELDS with its collector."
    )


def test_no_phantom_declarations():
    """Nothing declared that must-collect.md does not name (except our own)."""
    ours = {"schema_version", "run_started_at", "hostname", "platform",
            "python_version", "cpu_count", "total_memory_mb"}
    phantom = schema.DECLARED_FIELDS - _must_collect_field_names() - ours
    assert not phantom, f"declared but not in must-collect.md: {sorted(phantom)}"
