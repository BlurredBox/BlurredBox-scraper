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

## monitor_v4 — windowed health metrics (computed on the partner host)
Source: this repo, `scraperd/monitor.py` + `scraperd/detectors.py`. One row per `window_seconds` window in the **monitor** table, from inputs/outputs/outcomes/market ticks sent through sdk_hook. Every metric is null when it cannot be computed honestly; nothing defaults to 0.0.

Window bookkeeping: `window_start`, `window_seconds`, `n_inferences`, `n_outcomes`, `timestamp` (window end).

### 1. Input drift
Per-feature, against a frozen reference sample. **Feature names never leave the partner**: these go to the partner-local **feature_drift** table (`feature`, `feature_kind`, `window_start`, `timestamp`), never under output_root.

| field | definition |
|---|---|
| `psi` | 10 reference-quantile bins (categories for categoricals), eps 1e-4 |
| `js_divergence` | Jensen–Shannon divergence on the same bins, base 2, in [0,1] |
| `ks_statistic` | two-sample KS statistic (numeric only) |
| `wasserstein` | W1 distance / reference std (numeric only) |
| `chi_square` | Pearson chi-square of window counts vs reference proportions |
| `hellinger` | Hellinger distance on the bins |
| `new_category_rate` | share of window rows whose category never appeared in the reference |
| `cardinality_change` | window distinct / distinct of an equal-size reference draw − 1 |
| `domain_attribution` | the feature's share of the domain classifier's weight |

The only per-feature number that leaves: `psi_aggregate` (mean PSI across features).

Multivariate, on reference-standardised inputs (categoricals frequency-encoded):

| field | definition |
|---|---|
| `mmd` | unbiased MMD² (RBF, median-heuristic bandwidth), clipped at 0 |
| `mahalanobis_distance` | mean Mahalanobis distance to the reference (Ledoit–Wolf) / sqrt(d); ~1 = no shift |
| `covariance_delta` | RMS of window cov − reference cov |
| `top_eigenvalue_ratio` | window top eigenvalue / reference top eigenvalue |
| `pca_reconstruction_error` | window / reference mean error of a 90%-variance PCA |
| `domain_classifier_drift` | 5-fold CV AUC of logistic regression (x, x²) reference vs window; 0.5 = no shift |

### 2. Anomaly detectors on inputs
- `if_anomaly_mean` — mean IsolationForest anomaly score (0–1, higher = more anomalous)
- `if_anomaly_fraction_high` — share above the reference's 99th percentile score (~0.01 = no shift)
- `hstrees_score` — 1 − window/reference mean half-space-trees mass score (0 = no shift)
- `novelty_score` — share flagged by LOF novelty detection (contamination 0.01)

### 3. Model output shape
`score_mean`, `score_std`, `score_skew`, `score_kurtosis` (excess), `score_p05`, `score_p25`, `score_p50`, `score_p75`, `score_p95`, `predictive_entropy` (mean, nats), `positive_decision_rate`.

### 4. Calibration and accuracy (needs real outcomes)
Computed over outcomes that *arrived* in the window, joined to their prediction by `request_id`: `brier` (binary: (p−y)²; multiclass: summed over classes), `nll`, `ece`, `mce` (10 bins), `ci_width` (95% bootstrap CI width of `realised_metric`), `realised_metric` (accuracy; realised hit rate when there are no class labels), `realised_metric_lag_s` (median prediction→outcome lag).

### 5. Trading signals (new in v4)
- Realised performance: `tr_hit_rate`, `tr_hit_rate_rolling`, `tr_realised_error`, `tr_signal_move_corr`, `tr_signal_move_ic`
- Market regime: `tr_realised_vol`, `tr_amihud_illiquidity`, `tr_log_volume_ratio`, `tr_spread_proxy`, `tr_cross_asset_corr`, `tr_cross_asset_corr_rolling`
- Model behaviour: `tr_turnover`, `tr_signal_autocorr`, `tr_output_scale_drift`

### 6. Operations (each has a `_valid` flag)
`inference_latency_p50`, `inference_latency_p95`, `inference_latency_p99`, `cpu_utilization`, `gpu_utilization`, `memory_headroom`, `request_rate`, `timeout_rate`, `exception_rate`, `retry_rate`, `queue_depth`, `feed_staleness_s`, `host_anomaly_score` (from System A, via `sdk_hook.record_signal`).

Flags: `inference_latency_p50_valid`, `inference_latency_p95_valid`, `inference_latency_p99_valid`, `cpu_utilization_valid`, `gpu_utilization_valid`, `memory_headroom_valid`, `request_rate_valid`, `timeout_rate_valid`, `exception_rate_valid`, `retry_rate_valid`, `queue_depth_valid`, `feed_staleness_s_valid`, `host_anomaly_score_valid`.

### 7. Data quality and status
`row_count_vs_expected`, `ingest_incomplete_flag`, `ingest_dropped_fraction`, `unauthorized_deploy_flag` (**only our servers may set it** — scraperd always writes null and strips it from spool input), `warmup_state` (inactive / warming / ready), `detector_alarms_fired`, `estimator_valid`, `aux_model_valid`, `shed_event_flag`, `rebaseline_event_flag`.

## Label (for training, not scraped — joined from incident records)
- `label` / `failed` / `failure`: 1 at the failure event step, else 0

---
Existing ingestion points to reuse rather than duplicate: `SystemMetricsPoller` (`src/connectors/system_metrics_poller.py`) and SDK `get_process_metrics()` (`sdk/blurredbox_agd/metrics.py`). No standalone scraper module exists yet — this is the field list to build one from.
