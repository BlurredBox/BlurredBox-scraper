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

# ── monitor_v4: one row per window, computed on the partner host ─────────────
# Operations metrics. Each one is written with a sibling `<name>_valid` bool:
# a null value with valid=False means "not measurable this window", which a
# consumer must never read as zero.
OPS_FIELDS: tuple[str, ...] = (
    "inference_latency_p50",
    "inference_latency_p95",
    "inference_latency_p99",
    "cpu_utilization",
    "gpu_utilization",
    "memory_headroom",
    "request_rate",
    "timeout_rate",
    "exception_rate",
    "retry_rate",
    "queue_depth",
    "feed_staleness_s",
    "host_anomaly_score",
)

MONITOR_FIELDS: tuple[str, ...] = (
    "window_start",
    "window_seconds",
    "n_inferences",
    "n_outcomes",
    # 1. input drift. Per-feature stats stay in FEATURE_DRIFT_FIELDS (partner
    # only); psi_aggregate is the one per-feature number that leaves.
    "psi_aggregate",
    "mmd",
    "mahalanobis_distance",
    "covariance_delta",
    "top_eigenvalue_ratio",
    "pca_reconstruction_error",
    "domain_classifier_drift",
    # 2. anomaly detectors on inputs
    "if_anomaly_mean",
    "if_anomaly_fraction_high",
    "hstrees_score",
    "novelty_score",
    # 3. model output shape
    "score_mean",
    "score_std",
    "score_skew",
    "score_kurtosis",
    "score_p05",
    "score_p25",
    "score_p50",
    "score_p75",
    "score_p95",
    "predictive_entropy",
    "positive_decision_rate",
    # 4. calibration and accuracy — only from outcomes that arrived this window
    "brier",
    "nll",
    "ece",
    "mce",
    "ci_width",
    "realised_metric",
    "realised_metric_lag_s",
    # 5. trading signals
    "tr_hit_rate",
    "tr_hit_rate_rolling",
    "tr_realised_error",
    "tr_signal_move_corr",
    "tr_signal_move_ic",
    "tr_realised_vol",
    "tr_amihud_illiquidity",
    "tr_log_volume_ratio",
    "tr_spread_proxy",
    "tr_cross_asset_corr",
    "tr_cross_asset_corr_rolling",
    "tr_turnover",
    "tr_signal_autocorr",
    "tr_output_scale_drift",
    # 6. operations, each followed by its _valid flag
    *(f for name in OPS_FIELDS for f in (name, f"{name}_valid")),
    # 7. data quality and status
    "row_count_vs_expected",
    "ingest_incomplete_flag",
    "ingest_dropped_fraction",
    "unauthorized_deploy_flag",
    "warmup_state",
    "detector_alarms_fired",
    "estimator_valid",
    "aux_model_valid",
    "shed_event_flag",
    "rebaseline_event_flag",
    "timestamp",
)

# Set only by our servers. scraperd runs on the partner host, so it always
# writes null here and strips the key from anything the SDK sends.
SERVER_ONLY_FIELDS: frozenset[str] = frozenset({"unauthorized_deploy_flag"})

# Per-feature drift, one row per feature per window. Carries feature names, so
# it is written under monitor.partner_local_root and never under output_root.
FEATURE_DRIFT_FIELDS: tuple[str, ...] = (
    "window_start",
    "feature",
    "feature_kind",
    "psi",
    "js_divergence",
    "ks_statistic",
    "wasserstein",
    "chi_square",
    "hellinger",
    "new_category_rate",
    "cardinality_change",
    "domain_attribution",
    "timestamp",
)

DECLARED_FIELDS = DECLARED_FIELDS | frozenset(MONITOR_FIELDS) | frozenset(FEATURE_DRIFT_FIELDS)

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

_MONITOR_TYPES: dict[str, pa.DataType] = {
    "window_start": _TS,
    "window_seconds": pa.int64(),
    "n_inferences": pa.int64(),
    "n_outcomes": pa.int64(),
    "row_count_vs_expected": pa.float64(),
    "ingest_incomplete_flag": pa.bool_(),
    "ingest_dropped_fraction": pa.float64(),
    "unauthorized_deploy_flag": pa.bool_(),
    "warmup_state": pa.string(),
    "detector_alarms_fired": pa.int64(),
    "estimator_valid": pa.bool_(),
    "aux_model_valid": pa.bool_(),
    "shed_event_flag": pa.bool_(),
    "rebaseline_event_flag": pa.bool_(),
    "timestamp": _TS,
} | {f"{name}_valid": pa.bool_() for name in OPS_FIELDS}

# Everything not typed above is a nullable float metric.
MONITOR_SCHEMA = pa.schema(
    [(f, _MONITOR_TYPES.get(f, pa.float64())) for f in MONITOR_FIELDS]
)

FEATURE_DRIFT_SCHEMA = pa.schema(
    [(f, {"window_start": _TS, "timestamp": _TS, "feature": pa.string(),
          "feature_kind": pa.string()}.get(f, pa.float64()))
     for f in FEATURE_DRIFT_FIELDS]
)

assert tuple(SAMPLES_SCHEMA.names) == SAMPLE_FIELDS
assert tuple(REQUESTS_SCHEMA.names) == REQUEST_FIELDS
assert tuple(MONITOR_SCHEMA.names) == MONITOR_FIELDS
assert tuple(FEATURE_DRIFT_SCHEMA.names) == FEATURE_DRIFT_FIELDS


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
