# 02 — `schema.py` and the d=5 guard

Build order step 1. Half a day. This is the file the whole repo exists to protect: one frozen
tuple, one function, one test. Invariants 1–4 all live here.

## Do

### 1. `scraperd/schema.py`

```python
"""Field declarations, the frozen d=5 tuple, and the pyarrow schemas.

Every field name in must-collect.md is declared in exactly one of the tuples
below. tests/test_schema.py enforces that, which turns "we forgot a field" into
a CI failure. DEFERRED_FIELDS is a declaration, not a promise to collect.
"""

from __future__ import annotations

import pyarrow as pa

SCHEMA_VERSION = "core_v1"

# The d=5 serving contract. Frozen: test_schema.py asserts these five names in
# this order. Changing it is a schema version bump, not an edit.
CORE_V1: tuple[str, ...] = (
    "cpu_usage",
    "memory_usage_mb",
    "gpu_usage",
    "gpu_vram_mb",
    "latency_ms",
)

# The 1 Hz poll. No latency_ms — the poller has none to emit.
SAMPLE_FIELDS: tuple[str, ...] = (
    "process_id",
    "cpu_usage",
    "memory_usage_mb",
    "memory_pct",
    "gpu_usage",
    "gpu_vram_mb",
    "gpu_temperature",
    "num_threads",
    "num_fds",
    "io_read_bytes",
    "io_write_bytes",
    "num_connections",
    "ctx_switches",
    "disk_usage_pct",
    "freqtrade_cpu",
    "freqtrade_memory_mb",
    "timestamp",
)

# One row per inference. The only table carrying a real latency_ms.
REQUEST_FIELDS: tuple[str, ...] = (
    "request_id",
    "process_id",
    *CORE_V1,
    "input_size",
    "output_size",
    "status",
    "prediction",
    "timestamp",
)

# Constant for the whole run — one manifest.json, not 7.8M repeats.
MANIFEST_FIELDS: tuple[str, ...] = (
    "entity_id",
    "model_id",
    "model_version_id",
    "slug",
    "tenant_id",
    "operator_id",
    "environment",
    "artifact_id",
    "version",
    "schema_version",
    "run_started_at",
    "hostname",
    "platform",
    "python_version",
    "cpu_count",
    "total_memory_mb",
)

# Deprecated names that exist upstream. Mapped on ingest, never written.
ALIASES: dict[str, str] = {
    "gpu_memory_mb": "gpu_vram_mb",
    "cpu_pct": "cpu_usage",
}

# Declared, not collected. Each belongs to a deferred collector in
# file-layout.md §6; nothing here has a producer yet. Listing them keeps the
# must-collect coverage gate honest instead of silently narrowing the inventory.
DEFERRED_FIELDS: frozenset[str] = frozenset({
    # drift / statistical  (drift_stats collector)
    "psi_score", "error_rate", "count", "mean", "variance", "std_dev",
    # model performance    (model_perf collector)
    "accuracy", "f1_score", "in_flight_requests", "outcome_label", "labeled_at",
    "trust_score", "enclave_signature", "raw_payload",
    # fairness             (fairness collector)
    "ground_truth", "privileged_group", "disparate_impact", "tpr_delta",
    "positive_rate",
    # policy engine        (policy_signals collector)
    "predicted_failure_prob", "predicted_failure_risk",
    "predicted_drift_violation_prob", "flicker_score", "pilot_mode",
    "gru_suppressed", "signal_source", "temperature", "pinned_frac",
    "saturating", "iso_baseline_unique_rows",
    # action_taker daemon  (action_taker_io collector)
    "action", "target_name", "version_target", "target_pid",
    "predicted_fail_prob", "cryptographic_signature", "status_before",
    "execution_tries", "execution_latency_ms", "status_message", "new_pid",
    # per-request metadata the SDK does not emit yet
    "correlation_id",
    # training label — joined from incident records, never scraped
    "label", "failed", "failure",
})

DECLARED_FIELDS: frozenset[str] = frozenset(
    SAMPLE_FIELDS + REQUEST_FIELDS + MANIFEST_FIELDS
) | frozenset(ALIASES) | DEFERRED_FIELDS

_TS = pa.timestamp("us", tz="UTC")

SAMPLES_SCHEMA = pa.schema([
    ("process_id", pa.int64()),
    ("cpu_usage", pa.float64()),
    ("memory_usage_mb", pa.float64()),
    ("memory_pct", pa.float64()),
    ("gpu_usage", pa.float64()),
    ("gpu_vram_mb", pa.float64()),
    ("gpu_temperature", pa.float64()),
    ("num_threads", pa.int64()),
    ("num_fds", pa.int64()),
    ("io_read_bytes", pa.int64()),
    ("io_write_bytes", pa.int64()),
    ("num_connections", pa.int64()),
    ("ctx_switches", pa.int64()),
    ("disk_usage_pct", pa.float64()),
    ("freqtrade_cpu", pa.float64()),
    ("freqtrade_memory_mb", pa.float64()),
    ("timestamp", _TS),
])

REQUESTS_SCHEMA = pa.schema([
    ("request_id", pa.string()),
    ("process_id", pa.int64()),
    ("cpu_usage", pa.float64()),
    ("memory_usage_mb", pa.float64()),
    ("gpu_usage", pa.float64()),
    ("gpu_vram_mb", pa.float64()),
    ("latency_ms", pa.float64()),
    ("input_size", pa.int64()),
    ("output_size", pa.int64()),
    ("status", pa.string()),
    ("prediction", pa.string()),
    ("timestamp", _TS),
])

assert tuple(SAMPLES_SCHEMA.names) == SAMPLE_FIELDS
assert tuple(REQUESTS_SCHEMA.names) == REQUEST_FIELDS


def core_row(d: dict) -> list[float]:
    """Build the d=5 model input from a `requests` row.

    The only place in this codebase a model input is constructed. Raises on any
    missing or null field: a row without a real latency is not a d=5 row, and
    silently defaulting one to 0.0 is how the d=32 mistake happens again.
    """
    missing = [k for k in CORE_V1 if d.get(k) is None]
    if missing:
        raise KeyError(f"core_row: missing d=5 field(s): {missing}")
    return [float(d[k]) for k in CORE_V1]
```

### 2. `tests/test_schema.py`

```python
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
```

## Verify

```bash
.venv/bin/pytest tests/test_schema.py -q
```

```
.............                                                            [100%]
13 passed in 0.05s
```

Now prove the gate actually bites. Add a fake field to must-collect.md and watch CI fail:

```bash
printf '\n- `totally_new_field`\n' >> must-collect.md
.venv/bin/pytest tests/test_schema.py -q -k must_collect
git checkout must-collect.md
```

Expected failure — this is the point of the file:

```
AssertionError: must-collect.md names ['totally_new_field'], declared nowhere in schema.py.
```

## Why it is shaped this way

**79 field names.** The coverage test scrapes every backticked lowercase identifier out of
must-collect.md; that yields exactly 79. 32 are collected, 2 are deprecated aliases, 45 are
declared in `DEFERRED_FIELDS`. Nothing is silently dropped. `test_no_phantom_declarations`
closes the other direction — you cannot satisfy the gate by inventing names.

**`DEFERRED_FIELDS` is not dead weight.** Without it the only way to pass the coverage gate
is to collect all 79 fields — including `predicted_failure_prob` and `cryptographic_signature`,
which have no producer. Declaring them as deferred keeps the inventory complete and the
collection honest at the same time.

**`core_row` rejects `None`, not just missing keys.** A request row that reached the writer
with a null latency is exactly the row that must never become training data. `d.get(k) is None`
catches both cases in one condition.

**No numpy.** `core_row` returns a plain `list[float]`. numpy is on the do-not-add list and
a five-element list is what every consumer can accept anyway.

**The `assert` at module scope** pins schema column order to the field tuples, so the two can
never drift apart silently. It runs at import, costs nothing, and fails loudly.

## Done when

- [ ] `.venv/bin/pytest tests/test_schema.py -q` → 13 passed
- [ ] appending a fake field to must-collect.md fails the suite
- [ ] `git checkout must-collect.md` undid that
- [ ] `grep -rn "CORE_V1" scraperd/` shows it defined once and used only by `core_row`

→ [03-writer.md](03-writer.md)
