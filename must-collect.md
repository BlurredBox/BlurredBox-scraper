# Scraper Data Fields — Full Inventory

Every field the system collects/consumes for governance decisions, across all subsystems (not just the d=5 serving model — see caveat below). Source of truth: code, not spec.

**Caveat:** production failure model (GRU/IsolationForest) only consumes the fixed d=5 schema in [DATA_REQUIREMENTS.md](DATA_REQUIREMENTS.md). Don't feed a scraper's full output straight into that model — that's the d=32 mistake (train on fields prod never emits). This doc is the superset for building a scraper that also feeds drift, fairness, and policy subsystems, which do use more fields.

## Core telemetry (feeds GRU / IsolationForest — d=5)
Source: `sdk/blurredbox_agd/metrics.py:112-134`, `src/connectors/system_metrics_poller.py:274-305`, `src/core/forecasters/failure_predictor.py:158-163`

| field | type | notes |
|---|---|---|
| `cpu_usage` | float 0-100 | per-process %, psutil |
| `memory_usage_mb` | float | RSS MB |
| `gpu_usage` | float % | 0.0 on CPU-only hosts |
| `gpu_vram_mb` | float MB | canonical key (`gpu_memory_mb` is deprecated alias) |
| `latency_ms` | float | request/inference wall-clock |

## Extended resource metrics
Source: `sdk/blurredbox_agd/metrics.py:112-134`, `src/connectors/system_metrics_poller.py:190-221`

- `process_id`
- `num_threads`
- `num_fds`
- `io_read_bytes`
- `io_write_bytes`
- `num_connections`
- `ctx_switches`
- `memory_pct`
- `gpu_temperature`
- `freqtrade_cpu`, `freqtrade_memory_mb` (Freqtrade-specific)
- `disk_usage_pct`, `cpu_pct` (poller aliases)

## Drift / statistical metrics
Source: `src/core/forecasters/drift_forecaster.py:19-25,215-247`, `src/models/monitoring/drift_monitor.py`, `src/core/algorithms/drift.py`, `src/policy/adaptive/triggers.py:172-241`

- `psi_score` (scalar + per-feature vector, `n_bins=10`, `n_feat=32`)
- `error_rate`
- Welford running stats per metric: `count`, `mean`, `M2` → derived `variance`, `std_dev` (tracked for `latency_ms`, `psi_score`, `cpu_usage`, `gpu_usage`, `gpu_vram_mb`, `error_rate`, keyed per `model_id:metric`)

## Model performance metrics
Source: `src/api/monitoring/models.py:61-80`, `src/api/db/models.py:82-102`, `sdk/blurredbox_agd/client.py:552-618`

- `accuracy` (0-1)
- `f1_score` (0-1)
- `psi_score` (0-1)
- `error_rate`
- `in_flight_requests` (int)
- `outcome_label`, `labeled_at` (outcome-confirmation loop)
- `trust_score` (default 1.0, Layer-4 governance input)
- `input_size`, `output_size`
- `status`
- `request_id`, `process_id`
- `enclave_signature`
- `flags: Dict[str,bool]`
- `extra: Dict[str,Any]` (capped 64 keys)
- `raw_payload` (JSONB)

## Fairness metrics
Source: `src/api/services/fairness.py:362-399,635-684`

- `prediction`
- `ground_truth` (nullable)
- `protected_attributes.demographic_region` (fallback `"unknown"`)
- derived: `privileged_group`, `disparate_impact`, `tpr_delta`, `positive_rate` per group

## Policy engine / decision signals
Source: `src/policy/core/models.py:32-48` (`_METRIC_BOUNDS`), `src/policy/adaptive/triggers.py`, `src/core/engine.py:244-281`

- Watched metrics: `cpu_usage`, `gpu_usage`, `psi_score`, `error_rate`, `accuracy`, `f1_score`, `latency_ms`, `memory_usage_mb`
- Trigger outputs: `psi_score` (DriftTrigger, thresh 0.2), `latency_ms` std-dev (InstabilityTrigger), `cpu_usage` z-score (AnomalyTrigger), `gpu_usage`/`gpu_vram_mb` z-score (GPUAnomalyTrigger), `error_rate` (DegradationTrigger)
- Engine outputs: `predicted_failure_prob`, `predicted_failure_risk`, `predicted_drift_violation_prob`, `flicker_score`, `pilot_mode`, `gru_suppressed`, `signal_source`, `temperature`, `pinned_frac`, `saturating`, `iso_baseline_unique_rows`

## action_taker daemon inputs/outputs
Source: `src/daemons/proto/action_taker.proto:28-47`

- `action` (enum: HARD_BLOCK→ACTION_SHUTDOWN, RESTART, ROLLBACK)
- `target_name`, `version_target`, `target_pid`
- `predicted_fail_prob`
- `cryptographic_signature` (ECDSA)
- `operator_id`, `correlation_id`
- response: `status_before`, `execution_tries`, `execution_latency_ms`, `status_message`, `new_pid`

## Metadata / identifiers (attach to every row)
- `entity_id` (host/VM/process — required for leak-free per-entity splits)
- `timestamp` (ISO-8601 or epoch, time-ordered per entity)
- `model_id`, `model_version_id`, `slug`
- `tenant_id`, `operator_id`, `environment`
- `artifact_id`, `version`
- `correlation_id`, `request_id`

## Label (for training, not scraped — joined from incident records)
- `label` / `failed` / `failure`: 1 at the failure event step, else 0

---
Existing ingestion points to reuse rather than duplicate: `SystemMetricsPoller` (`src/connectors/system_metrics_poller.py`) and SDK `get_process_metrics()` (`sdk/blurredbox_agd/metrics.py`). No standalone scraper module exists yet — this is the field list to build one from.
