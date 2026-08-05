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
