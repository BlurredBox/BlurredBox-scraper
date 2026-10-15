"""monitor_v4: detectors fire on real shifts, stay quiet otherwise, and nothing
partner-private reaches the shipped table."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import duckdb
import numpy as np
import pytest

from scraperd import daemon, schema, sdk_hook
from scraperd.monitor import Monitor
from scraperd.writer import TableWriter

W = 300
T0 = 1_785_888_000.0                    # 2026-08-05T00:00:00Z, window-aligned
CFG = {"window_seconds": W, "reference_rows": 300, "min_window_rows": 50,
       "max_window_rows": 2000, "min_ops_rows": 10, "rolling_bars": 100000}

SPEC = """psi_aggregate mmd mahalanobis_distance covariance_delta top_eigenvalue_ratio
pca_reconstruction_error domain_classifier_drift if_anomaly_mean if_anomaly_fraction_high
hstrees_score novelty_score score_mean score_std score_skew score_kurtosis
predictive_entropy positive_decision_rate brier nll ece mce ci_width realised_metric
tr_hit_rate tr_hit_rate_rolling tr_realised_error tr_signal_move_corr tr_signal_move_ic
tr_realised_vol tr_amihud_illiquidity tr_log_volume_ratio tr_spread_proxy
tr_cross_asset_corr tr_cross_asset_corr_rolling tr_turnover tr_signal_autocorr
tr_output_scale_drift inference_latency_p50 inference_latency_p95 inference_latency_p99
cpu_utilization gpu_utilization memory_headroom request_rate timeout_rate exception_rate
retry_rate queue_depth feed_staleness_s host_anomaly_score row_count_vs_expected
ingest_incomplete_flag ingest_dropped_fraction unauthorized_deploy_flag warmup_state
detector_alarms_fired estimator_valid aux_model_valid shed_event_flag
rebaseline_event_flag""".split()


def mon(tmp_path, **kw):
    return Monitor(CFG | {"state_dir": str(tmp_path / "state")} | kw, cpu_count=4, now=T0)


def inputs(rng, n, start, shift=0.0, region=None):
    out = []
    for i in range(n):
        f = {"amount": float(rng.normal(shift, 1)), "age": float(rng.normal(40, 5)),
             "region": region or str(rng.choice(["eu", "us", "apac"]))}
        out.append({"kind": "request", "ts": start + i * (W - 1) / n,
                    "request_id": f"r{start}-{i}", "latency_ms": 10.0 + i % 7,
                    "status": "success", "features": f,
                    "score": float(1 / (1 + np.exp(-f["amount"])))})
    return out


def run(m, events, until):
    return m.step(events, errors=0, watermark=until)


def warm(m, rng, windows=1):
    rows = []
    for k in range(windows):
        rows += run(m, inputs(rng, 300, T0 + k * W), T0 + (k + 1) * W)[0]
    return rows


# ── schema ───────────────────────────────────────────────────────────────────
def test_every_requested_field_is_a_monitor_column():
    missing = [f for f in SPEC if f not in schema.MONITOR_FIELDS]
    assert not missing
    for name in schema.OPS_FIELDS:
        assert f"{name}_valid" in schema.MONITOR_FIELDS


def test_per_feature_stats_are_not_in_the_shipped_table():
    for f in ("feature", "psi", "js_divergence", "ks_statistic", "wasserstein",
              "chi_square", "hellinger", "domain_attribution"):
        assert f not in schema.MONITOR_FIELDS
        assert f in schema.FEATURE_DRIFT_FIELDS


# ── warmup and drift ─────────────────────────────────────────────────────────
def test_warmup_states_and_rebaseline_flag(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(1)
    empty = run(m, [], T0 + W)[0][0]
    assert empty["warmup_state"] == "inactive" and not empty["estimator_valid"]

    half = run(m, inputs(rng, 100, T0 + W), T0 + 2 * W)[0][0]
    assert half["warmup_state"] == "warming"
    assert half["psi_aggregate"] is None and not half["rebaseline_event_flag"]

    done = run(m, inputs(rng, 250, T0 + 2 * W), T0 + 3 * W)[0][0]
    assert done["warmup_state"] == "ready" and done["rebaseline_event_flag"]
    assert done["estimator_valid"] and done["aux_model_valid"]

    nxt = run(m, inputs(rng, 200, T0 + 3 * W), T0 + 4 * W)[0][0]
    assert not nxt["rebaseline_event_flag"]


def test_same_distribution_is_quiet_shifted_distribution_fires(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(2)
    warm(m, rng)
    (same,), local = run(m, inputs(rng, 300, T0 + W), T0 + 2 * W)
    (moved,), _ = run(m, inputs(rng, 300, T0 + 2 * W, shift=2.0, region="mars"),
                      T0 + 3 * W)

    assert same["psi_aggregate"] < 0.2 < moved["psi_aggregate"]
    assert same["domain_classifier_drift"] < 0.65 < 0.9 < moved["domain_classifier_drift"]
    assert same["mahalanobis_distance"] < moved["mahalanobis_distance"]
    assert same["mmd"] < moved["mmd"]
    assert same["if_anomaly_fraction_high"] < moved["if_anomaly_fraction_high"]
    assert same["novelty_score"] < moved["novelty_score"]
    assert same["hstrees_score"] < moved["hstrees_score"]
    assert same["detector_alarms_fired"] < moved["detector_alarms_fired"]
    assert moved["score_mean"] > same["score_mean"]

    by_name = {r["feature"]: r for r in local}
    assert set(by_name) == {"amount", "age", "region"}
    assert by_name["region"]["feature_kind"] == "categorical"
    assert by_name["amount"]["ks_statistic"] is not None
    assert by_name["region"]["ks_statistic"] is None
    assert abs(sum(r["domain_attribution"] for r in local) - 1) < 1e-6


def test_new_categories_are_counted(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(3)
    warm(m, rng)
    _, local = run(m, inputs(rng, 200, T0 + W, region="mars"), T0 + 2 * W)
    region = next(r for r in local if r["feature"] == "region")
    assert region["new_category_rate"] == 1.0
    assert region["cardinality_change"] < 0          # 1 distinct vs 3


def test_feature_names_never_appear_in_a_monitor_row(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(4)
    warm(m, rng)
    (row,), _ = run(m, inputs(rng, 300, T0 + W, shift=1.0), T0 + 2 * W)
    assert set(row) == set(schema.MONITOR_FIELDS)
    values = {str(v) for v in row.values()}
    assert not values & {"amount", "age", "region", "eu", "us", "apac"}


def test_feature_set_change_triggers_rebaseline(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(5)
    warm(m, rng)
    renamed = inputs(rng, 200, T0 + W)
    for e in renamed:
        e["features"] = {"other": 1.0}
    (row,), _ = run(m, renamed, T0 + 2 * W)
    assert row["warmup_state"] == "warming" and not row["estimator_valid"]


def test_reference_survives_a_restart(tmp_path):
    rng = np.random.default_rng(6)
    warm(mon(tmp_path), rng)
    m2 = Monitor(CFG | {"state_dir": str(tmp_path / "state")}, cpu_count=4, now=T0 + W)
    (row,), _ = run(m2, inputs(rng, 100, T0 + W), T0 + 2 * W)
    assert row["warmup_state"] == "ready" and row["psi_aggregate"] is not None
    assert not row["rebaseline_event_flag"]


def test_requested_rebaseline_starts_a_new_reference(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(7)
    warm(m, rng)
    ctl = {"kind": "control", "action": "rebaseline", "ts": T0 + W + 1}
    (row,), _ = run(m, [ctl], T0 + 2 * W)
    assert row["warmup_state"] == "warming"


# ── outputs, calibration, trading ────────────────────────────────────────────
def test_calibration_joins_outcomes_by_request_id(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(8)
    preds, outs = [], []
    for i in range(400):
        p = float(rng.uniform())
        preds.append({"kind": "request", "ts": T0 + i * 0.5, "request_id": f"q{i}",
                      "latency_ms": 5.0, "status": "success", "score": p})
        outs.append({"kind": "outcome", "ts": T0 + W + i * 0.5, "request_id": f"q{i}",
                     "label": int(rng.uniform() < p)})        # calibrated by construction
    (first,), _ = run(m, preds, T0 + W)
    assert first["brier"] is None and first["n_outcomes"] == 0
    assert 0.4 < first["positive_decision_rate"] < 0.6
    assert first["predictive_entropy"] > 0

    (second,), _ = run(m, outs, T0 + 2 * W)               # outcomes arrive later
    assert second["n_outcomes"] == 400
    assert second["ece"] < 0.1 and second["mce"] >= second["ece"]
    assert 0.1 < second["brier"] < 0.25
    assert 0.6 < second["realised_metric"] < 0.9
    assert second["ci_width"] > 0
    assert second["realised_metric_lag_s"] == pytest.approx(W)


def test_trading_signals_and_market(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(9)
    ev = []
    for i in range(200):
        move = float(rng.normal())
        sig = move + float(rng.normal(0, 0.5))               # informative signal
        ev.append({"kind": "request", "ts": T0 + i, "request_id": f"t{i}",
                   "latency_ms": 3.0, "status": "success", "signal": sig, "asset": "BTC"})
        ev.append({"kind": "outcome", "ts": T0 + i + 0.5, "request_id": f"t{i}",
                   "realised_move": move})
    common = np.cumsum(rng.normal(0, 0.01, W))
    for s in range(W):
        for asset, noise in (("BTC", 0.002), ("ETH", 0.002)):
            px = 100 * np.exp(common[s] + rng.normal(0, noise))
            ev.append({"kind": "market", "ts": T0 + s, "asset": asset, "price": px,
                       "volume": 10.0, "bid": px * 0.999, "ask": px * 1.001})
    (row,), _ = run(m, ev, T0 + W)
    assert row["tr_hit_rate"] > 0.7 and row["tr_hit_rate_rolling"] == row["tr_hit_rate"]
    assert row["tr_signal_move_corr"] > 0.7 and row["tr_signal_move_ic"] > 0.7
    assert row["tr_realised_error"] > 0
    assert row["tr_cross_asset_corr"] > 0.5
    assert row["tr_spread_proxy"] == pytest.approx(0.002, rel=1e-3)
    assert row["tr_realised_vol"] > 0 and row["tr_amihud_illiquidity"] > 0
    assert row["tr_turnover"] > 0 and row["tr_signal_autocorr"] is not None
    assert row["realised_metric"] == row["tr_hit_rate"]        # no class labels
    assert row["feed_staleness_s_valid"] and row["feed_staleness_s"] == pytest.approx(1.0)


# ── ops and data quality ─────────────────────────────────────────────────────
def test_ops_are_null_and_invalid_without_data(tmp_path):
    (row,), _ = run(mon(tmp_path), [], T0 + W)
    for name in ("inference_latency_p50", "gpu_utilization", "queue_depth",
                 "feed_staleness_s", "host_anomaly_score", "timeout_rate"):
        assert row[name] is None and row[f"{name}_valid"] is False
    assert row["request_rate"] == 0.0 and row["request_rate_valid"]


def test_ops_rates_and_ticks(tmp_path):
    m = mon(tmp_path)
    ev = [{"kind": "request", "ts": T0 + i, "request_id": f"o{i}",
           "latency_ms": float(i), "status": s, "retry": i % 10 == 0, "queue_depth": 4}
          for i, s in enumerate(["success"] * 80 + ["timeout"] * 10 + ["error"] * 10)]
    ev.append({"kind": "signal", "ts": T0 + 5, "name": "host_anomaly_score", "value": 0.3})
    for s in range(W):
        m.observe_tick(T0 + s, cpu_sum=200.0, gpu=50.0, gpu_ok=True, headroom=0.4)
    (row,), _ = run(m, ev, T0 + W)
    assert row["inference_latency_p50"] == pytest.approx(49.5)
    assert row["inference_latency_p99_valid"]
    assert (row["timeout_rate"], row["exception_rate"], row["retry_rate"]) == (0.1, 0.1, 0.1)
    assert row["request_rate"] == pytest.approx(100 / W)
    assert row["cpu_utilization"] == 50.0                      # 200% of 4 cores
    assert row["gpu_utilization"] == 50.0 and row["memory_headroom"] == 0.4
    assert row["queue_depth"] == 4.0 and row["host_anomaly_score"] == 0.3


def test_gpu_invalid_on_cpu_only_host(tmp_path):
    m = mon(tmp_path)
    m.observe_tick(T0 + 1, cpu_sum=10.0, gpu=0.0, gpu_ok=False, headroom=0.9)
    (row,), _ = run(m, [], T0 + W)
    assert row["cpu_utilization_valid"] and not row["gpu_utilization_valid"]
    assert row["gpu_utilization"] is None


def test_partial_first_window_and_late_events_are_flagged(tmp_path):
    m = Monitor(CFG | {"state_dir": str(tmp_path)}, cpu_count=1, now=T0 + 100)
    (first,), _ = run(m, [], T0 + W)
    assert first["ingest_incomplete_flag"]

    late = {"kind": "request", "ts": T0 + 5, "request_id": "late",
            "latency_ms": 1.0, "status": "success"}
    ok = {"kind": "request", "ts": T0 + W + 5, "request_id": "ok",
          "latency_ms": 1.0, "status": "success"}
    (second,), _ = m.step([late, ok], errors=1, watermark=T0 + 2 * W)
    assert second["ingest_dropped_fraction"] == pytest.approx(2 / 3)
    assert second["ingest_incomplete_flag"]


def test_row_count_vs_expected_and_shedding(tmp_path):
    m = mon(tmp_path, expected_requests_per_window=100, max_window_rows=50)
    rng = np.random.default_rng(10)
    (row,), _ = run(m, inputs(rng, 80, T0), T0 + W)
    assert row["row_count_vs_expected"] == pytest.approx(0.8)
    assert row["shed_event_flag"]                              # 80 > 50 reservoir


def test_unauthorized_deploy_flag_is_never_set(tmp_path):
    m = mon(tmp_path)
    ev = {"kind": "request", "ts": T0, "request_id": "x", "latency_ms": 1.0,
          "status": "success", "unauthorized_deploy_flag": True}
    (row,), _ = run(m, [ev], T0 + W)
    assert row["unauthorized_deploy_flag"] is None


# ── spool, config, writer ────────────────────────────────────────────────────
def _close_spool(spool):
    for p in spool.glob("*.jsonl"):
        p.rename(p.with_name(f"{p.stem.rsplit('-', 1)[0]}-200001010000.jsonl"))


def test_spool_roundtrip_keeps_features_out_of_requests(tmp_path):
    spool = tmp_path / "spool"
    sdk_hook.record(spool, "a1", 12.0, features={"amount": 3.0}, score=0.7,
                    signal=0.1, asset="BTC")
    sdk_hook.record_outcome(spool, "a1", label=1, realised_move=0.2)
    sdk_hook.record_market(spool, "BTC", 100.0, 1.0)
    sdk_hook.record_signal(spool, "host_anomaly_score", 0.2)
    sdk_hook.request_rebaseline(spool)
    with open(next(spool.glob("*.jsonl")), "a") as fh:
        fh.write(json.dumps({"kind": "signal", "name": "x", "value": 1,
                             "unauthorized_deploy_flag": True,
                             "timestamp": datetime.now(timezone.utc).isoformat()}) + "\n")
        fh.write("not json\n")
    _close_spool(spool)

    got = sdk_hook.drain_all(spool)
    assert got["errors"] == 1
    assert [e["kind"] for e in got["events"]] == \
        ["request", "outcome", "market", "signal", "control", "signal"]
    assert all("unauthorized_deploy_flag" not in e for e in got["events"])
    (req,) = got["requests"]
    assert set(req) == set(schema.REQUEST_FIELDS) and "features" not in req
    assert got["events"][0]["features"] == {"amount": 3.0}
    assert not list(spool.glob("*.jsonl"))


def test_pre_v4_spool_lines_still_parse(tmp_path):
    spool = tmp_path / "spool"
    spool.mkdir()
    row = {"request_id": "old", "latency_ms": 1.0, "cpu_usage": 1.0, "memory_usage_mb": 1.0,
           "gpu_usage": 0.0, "gpu_vram_mb": 0.0, "process_id": 1, "status": "success",
           "input_size": 0, "output_size": 0, "prediction": "",
           "timestamp": datetime.now(timezone.utc).isoformat()}
    (spool / "1-200001010000.jsonl").write_text(json.dumps(row) + "\n")
    got = sdk_hook.drain_all(spool)
    assert len(got["requests"]) == 1 and got["events"][0]["kind"] == "request"


@pytest.mark.parametrize("key", ["state_dir", "partner_local_root"])
def test_config_refuses_partner_dirs_inside_output_root(tmp_path, key):
    cfg = {"entity_id": "vm1", "output_root": str(tmp_path / "cold"),
           "spool_dir": str(tmp_path / "spool"),
           "monitor": {"enabled": True, "state_dir": str(tmp_path / "state"),
                       "partner_local_root": str(tmp_path / "local")}}
    cfg["monitor"][key] = str(tmp_path / "cold" / "x")
    p = tmp_path / "c.yaml"
    p.write_text(json.dumps(cfg))
    with pytest.raises(SystemExit, match="inside output_root"):
        daemon.load_config(p)
    cfg["monitor"][key] = str(tmp_path / key)
    p.write_text(json.dumps(cfg))
    assert daemon.load_config(p)["monitor"]["enabled"]


def test_monitor_rows_write_and_read_back(tmp_path):
    m = mon(tmp_path)
    rng = np.random.default_rng(11)
    rows = warm(m, rng)
    more, local = run(m, inputs(rng, 200, T0 + W, shift=1.0), T0 + 2 * W)
    hb = tmp_path / "hb"
    w = TableWriter(tmp_path / "cold", "monitor", schema.MONITOR_SCHEMA, "vm1", hb)
    for r in rows + more:
        w.add(r)
    w.close()
    lw = TableWriter(tmp_path / "local", "feature_drift", schema.FEATURE_DRIFT_SCHEMA, "vm1", hb)
    for r in local:
        lw.add(r)
    lw.close()

    got = duckdb.sql(f"""SELECT count(*), max(psi_aggregate), bool_and(estimator_valid)
        FROM read_parquet('{tmp_path}/cold/monitor/*/*.parquet')""").fetchone()
    assert got[0] == 2 and got[1] > 0 and got[2]
    n = duckdb.sql(f"SELECT count(DISTINCT feature) FROM "
                   f"read_parquet('{tmp_path}/local/feature_drift/*/*.parquet')").fetchone()
    assert n[0] == 3
    assert not any("feature_drift" in d for d, *_ in os.walk(tmp_path / "cold"))
