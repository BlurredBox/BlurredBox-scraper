"""monitor_v4: one row per window of drift, anomaly, output-shape, calibration,
trading, operations and data-quality metrics, computed on the partner host.

Inputs:
  - spool events from sdk_hook.drain_all(): inferences (with features, score,
    signal…), outcomes, market ticks, external signals, control;
  - 1 Hz ticks from the sample loop (cpu, gpu, memory headroom).

Outputs per closed window:
  - one `monitor` row (MONITOR_SCHEMA) — aggregates only, written under
    output_root alongside samples/requests;
  - `feature_drift` rows (FEATURE_DRIFT_SCHEMA) — one per feature, carrying
    feature names, written under partner_local_root and nowhere else.

Raw feature values exist in three places only: the spool, this process's
memory, and state_dir/reference.json (the frozen reference sample). All three
are partner-local by construction; load_config refuses either directory if it
sits inside output_root.

Threading: the sample loop only appends to `self.ticks` (a deque, append is
atomic). Everything else happens inside step(), called from one executor
thread by the flush loop, so no lock is needed.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import Counter, OrderedDict, deque
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from . import detectors as det
from .schema import FEATURE_DRIFT_FIELDS, MONITOR_FIELDS, OPS_FIELDS

log = logging.getLogger("scraperd.monitor")

DEFAULTS: dict = {
    "window_seconds": 300,
    "reference_rows": 2000,       # inputs frozen as the drift reference
    "max_window_rows": 5000,      # per-window reservoir; beyond it = shed
    "min_window_rows": 50,        # fewer inputs -> estimators not valid
    "min_ops_rows": 10,           # fewer requests -> latency/rates not valid
    "expected_requests_per_window": None,   # None: median of recent windows
    "outcome_ttl_seconds": 7 * 86400,
    "max_pending_outcomes": 200_000,
    "hit_rate_rolling_n": 1000,
    "bar_seconds": 60,
    "rolling_bars": 1440,
    "decision_threshold": 0.5,
    "seed": 0,
}

# A detector "fires" when its value exceeds the threshold. psi 0.2 matches the
# upstream DriftTrigger; the ratio-scaled ones are ~1.0 when nothing moved.
DEFAULT_ALARMS: dict[str, float] = {
    "psi_aggregate": 0.2,
    "domain_classifier_drift": 0.75,
    "mahalanobis_distance": 2.0,
    "top_eigenvalue_ratio": 2.0,
    "pca_reconstruction_error": 2.0,
    "if_anomaly_fraction_high": 0.05,
    "novelty_score": 0.05,
    "hstrees_score": 0.3,
    "ece": 0.1,
}

TIMEOUT = {"timeout"}
EXCEPTION = {"error", "exception", "failed"}
SHED = {"shed"}
SIGNALS = {"host_anomaly_score", "queue_depth"}   # accepted via record_signal()


def _dt(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def _num(v) -> float:
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    return math.nan


class Reservoir:
    """Uniform sample of at most `cap` items; `seen` keeps the true count."""

    def __init__(self, cap: int, rng: np.random.Generator):
        self.cap, self.rng, self.items, self.seen = cap, rng, [], 0

    def add(self, x) -> None:
        self.seen += 1
        if len(self.items) < self.cap:
            self.items.append(x)
        else:
            j = int(self.rng.integers(self.seen))
            if j < self.cap:
                self.items[j] = x

    @property
    def shed(self) -> bool:
        return self.seen > self.cap


class Window:
    def __init__(self, idx: int, seconds: int, cap: int, rng: np.random.Generator):
        self.start, self.end = idx * seconds, (idx + 1) * seconds
        R = lambda: Reservoir(cap, rng)  # noqa: E731
        self.inputs, self.scores, self.probs, self.latencies = R(), R(), R(), R()
        self.cal, self.trade, self.lags = R(), R(), R()
        self.signals: dict[str, list] = {}
        self.n_signals = 0
        self.n_inferences = self.n_outcomes = self.n_events = 0
        self.n_requests = self.retries = 0
        self.decisions = [0, 0]                   # positives, total
        self.status: Counter = Counter()
        self.queue: list[float] = []
        self.host_anomaly: list[float] = []
        self.cpu: list[float] = []
        self.gpu: list[float] = []
        self.gpu_ok = False
        self.headroom: list[float] = []
        self.prices: dict[str, list[float]] = {}
        self.quotes: list[float] = []


class Reference:
    """The frozen input distribution every drift number is measured against."""

    def __init__(self, rows: list[dict], seed: int = 0):
        from sklearn.covariance import LedoitWolf
        from sklearn.decomposition import PCA
        from sklearn.ensemble import IsolationForest
        from sklearn.neighbors import LocalOutlierFactor

        if len(rows) < 20:
            raise ValueError(f"reference needs >= 20 rows, got {len(rows)}")
        self.rng = np.random.default_rng(seed)
        names = sorted(set().union(*rows))
        self.num, self.cat = [], []
        for n in names:
            vals = [r.get(n) for r in rows if r.get(n) is not None]
            is_num = vals and all(isinstance(v, (int, float, bool)) for v in vals)
            (self.num if is_num else self.cat).append(n)
        if not names:
            raise ValueError("reference rows carry no features")
        self.names = frozenset(names)
        self.columns = self.num + self.cat              # encode() column order

        self.num_raw = {n: np.array([_num(r.get(n)) for r in rows]) for n in self.num}
        self.cat_raw = {n: [None if r.get(n) is None else str(r.get(n)) for r in rows]
                        for n in self.cat}
        self.edges = {n: det.numeric_edges(v) for n, v in self.num_raw.items()}
        self.freq = {}
        for n, vals in self.cat_raw.items():
            c = Counter(v for v in vals if v is not None)
            tot = sum(c.values()) or 1
            self.freq[n] = {k: v / tot for k, v in c.items()}

        raw = self._raw(rows)
        self.mu = np.nanmean(raw, 0)
        sd = np.nanstd(raw, 0)
        self.mu = np.where(np.isfinite(self.mu), self.mu, 0.0)
        self.sd = np.where(np.isfinite(sd) & (sd > 0), sd, 1.0)
        self.X = self.encode(rows)
        d = self.X.shape[1]

        self.cov = np.atleast_2d(np.cov(self.X, rowvar=False))
        self.precision = LedoitWolf().fit(self.X).precision_
        self.top_eig = float(np.linalg.eigvalsh(self.cov)[-1])
        self.bandwidth = det.median_bandwidth(self.X, self.rng)
        self.pca = PCA(n_components=0.9, random_state=seed).fit(self.X) if d >= 2 else None
        self.pca_ref_err = self._recon(self.X) if self.pca is not None else None

        self.iforest = IsolationForest(n_estimators=100, random_state=seed).fit(self.X)
        self.if_threshold = float(np.quantile(-self.iforest.score_samples(self.X), 0.99))
        self.hst = det.HalfSpaceTrees(seed=seed).fit(self.X)
        self.hst_ref = float(self.hst.score(self.X).mean())
        self.lof = LocalOutlierFactor(n_neighbors=min(20, len(self.X) - 1),
                                      novelty=True, contamination=0.01).fit(self.X)

    # ── encoding: numeric z-scores + categorical reference frequency ────────
    def _raw(self, rows: list[dict]) -> np.ndarray:
        cols = [[_num(r.get(n)) for r in rows] for n in self.num]
        for n in self.cat:
            f = self.freq[n]
            cols.append([math.nan if r.get(n) is None else f.get(str(r.get(n)), 0.0)
                         for r in rows])
        return np.array(cols, dtype=float).T.reshape(len(rows), len(self.columns))

    def encode(self, rows: list[dict]) -> np.ndarray:
        z = (self._raw(rows) - self.mu) / self.sd
        return np.where(np.isfinite(z), z, 0.0)       # missing -> reference mean

    def _recon(self, X: np.ndarray) -> float:
        back = self.pca.inverse_transform(self.pca.transform(X))
        return float(((X - back) ** 2).sum(1).mean())

    # ── per-window measurement ──────────────────────────────────────────────
    def per_feature(self, rows: list[dict]) -> list[dict]:
        out = []
        for n in self.num:
            w = np.array([_num(r.get(n)) for r in rows])
            out.append({"feature": n, "feature_kind": "numeric"}
                       | det.numeric_drift(self.num_raw[n], w, self.edges[n]))
        for n in self.cat:
            w = [None if r.get(n) is None else str(r.get(n)) for r in rows]
            out.append({"feature": n, "feature_kind": "categorical"}
                       | det.categorical_drift(self.cat_raw[n], w, self.rng))
        return out

    def multivariate(self, Xw: np.ndarray) -> dict:
        out = {
            "mmd": det.mmd_rbf(self.X, Xw, self.bandwidth, self.rng),
            "mahalanobis_distance": det.mahalanobis_mean(Xw, self.precision),
            "covariance_delta": det.covariance_delta(self.cov, Xw),
            "top_eigenvalue_ratio": det.top_eigenvalue_ratio(self.top_eig, Xw),
            "pca_reconstruction_error": None,
        }
        if self.pca is not None and self.pca_ref_err:
            out["pca_reconstruction_error"] = self._recon(Xw) / self.pca_ref_err
        return out

    def anomaly(self, Xw: np.ndarray) -> dict:
        s = -self.iforest.score_samples(Xw)
        return {
            "if_anomaly_mean": float(s.mean()),
            "if_anomaly_fraction_high": float(np.mean(s > self.if_threshold)),
            "hstrees_score": 1.0 - float(self.hst.score(Xw).mean()) / self.hst_ref
            if self.hst_ref > 0 else None,
            "novelty_score": float(np.mean(self.lof.predict(Xw) == -1)),
        }

    def domain_classifier(self, Xw: np.ndarray, max_n: int = 2000):
        """Cross-validated AUC of reference-vs-window, and per-feature weight."""
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import roc_auc_score
        from sklearn.model_selection import StratifiedKFold, cross_val_predict

        n = min(len(self.X), len(Xw), max_n)
        if n < 10:
            return None, None
        a = self.X[self.rng.choice(len(self.X), n, replace=False)]
        b = Xw[self.rng.choice(len(Xw), n, replace=False)]
        X = np.vstack([a, b])
        X2 = np.hstack([X, X ** 2])                  # mean and variance shifts
        y = np.r_[np.zeros(n), np.ones(n)]
        clf = LogisticRegression(max_iter=500)
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=0)
        p = cross_val_predict(clf, X2, y, cv=cv, method="predict_proba")[:, 1]
        auc = float(roc_auc_score(y, p))
        coef = np.abs(clf.fit(X2, y).coef_[0])
        d = X.shape[1]
        attr = coef[:d] + coef[d:]
        attr = attr / attr.sum() if attr.sum() > 0 else attr
        return auc, dict(zip(self.columns, attr.tolist()))


class Monitor:
    def __init__(self, cfg: dict | None, cpu_count: int, now: float | None = None):
        c = DEFAULTS | dict(cfg or {})
        self.c = c
        self.alarms = c.get("alarms") or DEFAULT_ALARMS
        self.W = int(c["window_seconds"])
        self.rng = np.random.default_rng(c["seed"])
        self.cpu_count = cpu_count or 1
        self.state_path = Path(c["state_dir"]) / "reference.json" if c.get("state_dir") else None

        now = time.time() if now is None else now
        self.started = now
        self.next_close = int(now // self.W)
        self.windows: dict[int, Window] = {}
        self.ticks: deque = deque()

        self.pending: OrderedDict = OrderedDict()   # request_id -> prediction
        self.hits: deque = deque(maxlen=int(c["hit_rate_rolling_n"]))
        self.bars: dict[str, dict[int, list]] = {}  # asset -> bar -> [close, vol, $vol]
        self.last_tick: dict[str, float] = {}
        self.last_pos: dict[str, float] = {}
        self.counts_hist: deque = deque(maxlen=12)
        self.carry_dropped = 0

        self.ref: Reference | None = None
        self.warm_rows: list[dict] = []
        self.sig_ref: list[float] = []
        self.seen_features = False
        self.fitted_this_window = False
        self._load_state()

    # ── public ──────────────────────────────────────────────────────────────
    def observe_tick(self, ts: float, cpu_sum: float, gpu: float, gpu_ok: bool,
                     headroom: float) -> None:
        """Called by the 1 Hz loop. Append only; folded in by step()."""
        self.ticks.append((ts, cpu_sum, gpu, gpu_ok, headroom))

    def step(self, events: list[dict], errors: int, watermark: float,
             writer_dropped: int = 0) -> tuple[list[dict], list[dict]]:
        """Ingest one drain's events and close every window ending <= watermark.

        `watermark` is the epoch second before which every spool file has been
        consumed; no event older than it can still arrive.
        """
        self.carry_dropped += errors + writer_dropped
        while self.ticks:
            ts, cpu, gpu, gpu_ok, headroom = self.ticks.popleft()
            w = self._window(ts, count_late=False)
            if w is not None:
                w.cpu.append(cpu)
                w.gpu.append(gpu)
                w.gpu_ok |= gpu_ok
                w.headroom.append(headroom)
        for e in sorted(events, key=lambda e: e["ts"]):
            self._ingest(e)
        self._prune_pending(watermark)

        rows, local = [], []
        while (self.next_close + 1) * self.W <= watermark:
            w = self.windows.pop(self.next_close, None) \
                or Window(self.next_close, self.W, self.c["max_window_rows"], self.rng)
            row, feat = self._close(w)
            rows.append(row)
            local.extend(feat)
            self.next_close += 1
        if rows:
            self._save_state()
        return rows, local

    # ── ingest ──────────────────────────────────────────────────────────────
    def _window(self, ts: float, count_late: bool = True) -> Window | None:
        idx = int(ts // self.W)
        if idx < self.next_close:
            if count_late:
                self.carry_dropped += 1          # arrived after its window closed
            return None
        w = self.windows.get(idx)
        if w is None:
            w = self.windows[idx] = Window(idx, self.W, self.c["max_window_rows"], self.rng)
        return w

    def _ingest(self, e: dict) -> None:
        kind, ts = e.get("kind", "request"), e["ts"]
        if kind == "control":
            if e.get("action") == "rebaseline":
                self._rebaseline("requested via sdk_hook.request_rebaseline()")
            return
        w = self._window(ts)
        if w is None:
            return
        w.n_events += 1
        getattr(self, f"_on_{kind}", self._on_unknown)(w, e, ts)

    def _on_unknown(self, w, e, ts):
        log.debug("ignoring spool event kind %r", e.get("kind"))

    def _on_request(self, w: Window, e: dict, ts: float) -> None:
        w.n_requests += 1
        w.n_inferences += 1
        status = str(e.get("status") or "").lower()
        w.status[status] += 1
        if e.get("retry"):
            w.retries += 1
        if e.get("latency_ms") is not None:
            w.latencies.add(float(e["latency_ms"]))
        if e.get("queue_depth") is not None:
            w.queue.append(float(e["queue_depth"]))

        feats = e.get("features")
        if isinstance(feats, dict) and feats:
            self.seen_features = True
            w.inputs.add(feats)
            if self.ref is None and len(self.warm_rows) < self.c["reference_rows"]:
                self.warm_rows.append(feats)

        p = det.to_probs(e.get("score"), e.get("probs"))
        score = e.get("score")
        if score is None and p is not None:
            score = float(p.max())
        if score is not None:
            w.scores.add(float(score))
        if p is not None:
            w.probs.add(p)
        decision = e.get("decision")
        if decision is None and p is not None and p.size == 2:
            decision = p[1] >= self.c["decision_threshold"]
        if decision is not None:
            w.decisions[0] += bool(decision)
            w.decisions[1] += 1

        sig = e.get("signal")
        asset = str(e.get("asset") or "")
        if sig is not None and math.isfinite(float(sig)):
            sig = float(sig)
            w.n_signals += 1
            if w.n_signals <= self.c["max_window_rows"]:
                pos = e.get("position")
                w.signals.setdefault(asset, []).append(
                    (ts, sig, None if pos is None else float(pos)))
            if len(self.sig_ref) < self.c["reference_rows"]:
                self.sig_ref.append(sig)
        else:
            sig = None

        rid = e.get("request_id")
        if rid and (p is not None or sig is not None):
            self.pending[rid] = (ts, None if p is None else p.tolist(), sig, asset)
            self.pending.move_to_end(rid)
            while len(self.pending) > self.c["max_pending_outcomes"]:
                self.pending.popitem(last=False)

    def _on_outcome(self, w: Window, e: dict, ts: float) -> None:
        pred = self.pending.pop(e.get("request_id"), None)
        if pred is None:
            return                     # never seen, expired, or pre-restart
        pts, probs, sig, _asset = pred
        w.n_outcomes += 1
        w.lags.add(ts - pts)
        if probs is not None and e.get("label") is not None:
            w.cal.add((np.asarray(probs), int(e["label"])))
        move = e.get("realised_move")
        if sig is not None and move is not None and math.isfinite(float(move)):
            w.trade.add((sig, float(move)))

    def _on_market(self, w: Window, e: dict, ts: float) -> None:
        asset, price = str(e.get("asset") or ""), e.get("price")
        if price is None or not float(price) > 0:
            return
        price, vol = float(price), float(e.get("volume") or 0.0)
        bar = self.bars.setdefault(asset, {}).setdefault(int(ts // self.c["bar_seconds"]),
                                                         [price, 0.0, 0.0])
        bar[0] = price
        bar[1] += vol
        bar[2] += vol * price
        w.prices.setdefault(asset, []).append(price)
        bid, ask = e.get("bid"), e.get("ask")
        if bid is not None and ask is not None and float(bid) > 0 and float(ask) >= float(bid):
            w.quotes.append((float(ask) - float(bid)) / ((float(ask) + float(bid)) / 2))
        self.last_tick[asset] = max(ts, self.last_tick.get(asset, ts))

    def _on_signal(self, w: Window, e: dict, ts: float) -> None:
        name, v = e.get("name"), e.get("value")
        if name not in SIGNALS or v is None:
            return
        (w.host_anomaly if name == "host_anomaly_score" else w.queue).append(float(v))

    def _prune_pending(self, now: float) -> None:
        cutoff = now - self.c["outcome_ttl_seconds"]
        while self.pending:
            rid, pred = next(iter(self.pending.items()))
            if pred[0] >= cutoff:
                break
            self.pending.popitem(last=False)

    # ── reference ───────────────────────────────────────────────────────────
    def _rebaseline(self, why: str) -> None:
        log.warning("rebaseline: %s — collecting a new reference", why)
        self.ref, self.warm_rows, self.sig_ref = None, [], []

    def _maybe_fit(self) -> None:
        if self.ref is not None or len(self.warm_rows) < self.c["reference_rows"]:
            return
        try:
            self.ref = Reference(self.warm_rows, seed=self.c["seed"])
            self.fitted_this_window = True
            log.info("reference fitted on %d rows, %d features",
                     len(self.warm_rows), len(self.ref.columns))
        except Exception:
            log.exception("reference fit failed — discarding and re-collecting")
            self.warm_rows = []

    def _load_state(self) -> None:
        if not self.state_path or not self.state_path.exists():
            return
        try:
            s = json.loads(self.state_path.read_text())
            self.warm_rows = s.get("reference_rows", [])
            self.sig_ref = s.get("signal_reference", [])
            self.hits.extend(s.get("hits", []))
            self.counts_hist.extend(s.get("counts_hist", []))
            self.seen_features = bool(self.warm_rows)
            if len(self.warm_rows) >= self.c["reference_rows"]:
                self.ref = Reference(self.warm_rows, seed=self.c["seed"])
            log.info("state loaded: %d reference rows", len(self.warm_rows))
        except Exception:
            log.exception("state unreadable — starting a fresh reference")
            self.warm_rows, self.sig_ref = [], []

    def _save_state(self) -> None:
        if not self.state_path:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "reference_rows": self.warm_rows,
            "signal_reference": self.sig_ref,
            "hits": list(self.hits),
            "counts_hist": list(self.counts_hist),
        }))
        os.replace(tmp, self.state_path)

    # ── close ───────────────────────────────────────────────────────────────
    def _close(self, w: Window) -> tuple[dict, list[dict]]:
        row = dict.fromkeys(MONITOR_FIELDS)
        covered = w.end - max(w.start, self.started)
        partial = covered < self.W
        row |= {
            "window_start": _dt(w.start), "timestamp": _dt(w.end),
            "window_seconds": self.W,
            "n_inferences": w.n_inferences, "n_outcomes": w.n_outcomes,
        }
        self.fitted_this_window = False
        local: list[dict] = []
        rows = w.inputs.items

        if self.ref is not None and rows:
            mismatch = np.mean([frozenset(r) != self.ref.names for r in rows])
            if mismatch > 0.5:
                self._rebaseline(f"feature set changed in {mismatch:.0%} of inputs")
        self._maybe_fit()

        estimator_ok = self.ref is not None and len(rows) >= self.c["min_window_rows"]
        aux_ok = False
        if estimator_ok:
            try:
                X = self.ref.encode(rows)
                local = self._drift(row, rows, X)
                aux_ok = self._aux(row, local, X)
            except Exception:
                log.exception("drift estimators failed for window %s", row["window_start"])
                estimator_ok = False

        self._output(row, w)
        self._calibration(row, w)
        self._trading(row, w)
        self._ops(row, w, covered)
        self._quality(row, w, partial)
        row |= {
            "warmup_state": "ready" if self.ref is not None
            else ("warming" if self.seen_features else "inactive"),
            "estimator_valid": estimator_ok,
            "aux_model_valid": aux_ok,
            "shed_event_flag": w.inputs.shed or w.scores.shed or w.latencies.shed
            or w.n_signals > self.c["max_window_rows"] or any(w.status[s] for s in SHED),
            "rebaseline_event_flag": self.fitted_this_window,
            "unauthorized_deploy_flag": None,         # server-only; never set here
        }
        row["detector_alarms_fired"] = sum(
            1 for k, t in self.alarms.items() if row.get(k) is not None and row[k] > t)
        for r in local:
            r |= {"window_start": row["window_start"], "timestamp": row["timestamp"]}
        local = [{k: r.get(k) for k in FEATURE_DRIFT_FIELDS} for r in local]
        return row, local

    def _drift(self, row: dict, rows: list[dict], X: np.ndarray) -> list[dict]:
        local = self.ref.per_feature(rows)
        psis = [r["psi"] for r in local if r.get("psi") is not None]
        row["psi_aggregate"] = float(np.mean(psis)) if psis else None
        row |= self.ref.multivariate(X)
        return local

    def _aux(self, row: dict, local: list[dict], X: np.ndarray) -> bool:
        """Fitted auxiliary models. Their failure invalidates only themselves."""
        try:
            row |= self.ref.anomaly(X)
            auc, attr = self.ref.domain_classifier(X)
            row["domain_classifier_drift"] = auc
            for r in local:
                r["domain_attribution"] = (attr or {}).get(r["feature"])
            return auc is not None
        except Exception:
            log.exception("auxiliary models failed")
            return False

    def _output(self, row: dict, w: Window) -> None:
        row |= det.score_shape(w.scores.items)
        row["predictive_entropy"] = det.predictive_entropy(w.probs.items)
        if w.decisions[1]:
            row["positive_decision_rate"] = w.decisions[0] / w.decisions[1]

    def _calibration(self, row: dict, w: Window) -> None:
        if w.cal.items:
            probs, labels = zip(*w.cal.items)
            row |= det.calibration(list(probs), list(labels), self.rng)
        elif w.trade.items:
            # No class labels: the lagged metric is the realised hit rate.
            h = det.hits(*zip(*w.trade.items))
            if h:
                row["realised_metric"] = float(np.mean(h))
                row["ci_width"] = det.bootstrap_ci_width(h, self.rng)
        if w.lags.items:
            row["realised_metric_lag_s"] = float(np.median(w.lags.items))

    def _trading(self, row: dict, w: Window) -> None:
        if w.trade.items:
            s, m = zip(*w.trade.items)
            row |= det.signal_vs_move(s, m)
            self.hits.extend(det.hits(s, m))
        if self.hits:
            row["tr_hit_rate_rolling"] = float(np.mean(self.hits))

        turns, acs, all_sig = [], [], []
        for asset, seq in w.signals.items():
            seq.sort(key=lambda t: t[0])
            sig = [s for _, s, _ in seq]
            pos = [s if p is None else p for _, s, p in seq]
            if asset in self.last_pos:
                pos = [self.last_pos[asset]] + pos
            if len(pos) >= 2:
                turns.append(float(np.mean(np.abs(np.diff(pos)))))
            self.last_pos[asset] = pos[-1]
            ac = det.lag1_autocorr(sig)
            if ac is not None:
                acs.append(ac)
            all_sig += sig
        row["tr_turnover"] = float(np.mean(turns)) if turns else None
        row["tr_signal_autocorr"] = float(np.mean(acs)) if acs else None
        ref_sd = float(np.std(self.sig_ref)) if len(self.sig_ref) >= self.c["reference_rows"] else 0
        if len(all_sig) >= 2 and ref_sd > 0 and np.std(all_sig) > 0:
            row["tr_output_scale_drift"] = math.log(float(np.std(all_sig)) / ref_sd)

        self._market(row, w)

    def _market(self, row: dict, w: Window) -> None:
        bs = self.c["bar_seconds"]
        lo, hi = int(w.start // bs), int(w.end // bs)
        vols, amis, lvr = [], [], []
        win_ret, all_ret = {}, {}
        for asset, bars in self.bars.items():
            keys = sorted(bars)
            closes = [bars[b][0] for b in keys]
            r = det.log_returns(closes)
            all_ret[asset] = dict(zip(keys[1:], r))
            in_w = [i for i, b in enumerate(keys) if lo <= b < hi]
            if not in_w:
                continue
            idx = [i for i in in_w if i >= 1]          # bars with a prior close
            rw = np.array([r[i - 1] for i in idx])
            win_ret[asset] = {keys[i]: r[i - 1] for i in idx}
            if rw.size >= 2:
                vols.append(float(rw.std()))
            a = det.amihud(rw, [bars[keys[i]][2] for i in idx])
            if a is not None:
                amis.append(a)
            prior = [bars[b][1] for b in keys if b < lo]
            now_v = np.mean([bars[keys[i]][1] for i in in_w])
            if prior and np.mean(prior) > 0 and now_v > 0:
                lvr.append(math.log(now_v / np.mean(prior)))
        row["tr_realised_vol"] = float(np.mean(vols)) if vols else None
        row["tr_amihud_illiquidity"] = float(np.mean(amis)) if amis else None
        row["tr_log_volume_ratio"] = float(np.mean(lvr)) if lvr else None
        if w.quotes:
            row["tr_spread_proxy"] = float(np.mean(w.quotes))
        else:
            rolls = [s for s in map(det.roll_spread, w.prices.values()) if s is not None]
            row["tr_spread_proxy"] = float(np.mean(rolls)) if rolls else None
        row["tr_cross_asset_corr"] = det.mean_pairwise_corr(_aligned(win_ret))
        row["tr_cross_asset_corr_rolling"] = det.mean_pairwise_corr(_aligned(all_ret))

        keep_from = hi - int(self.c["rolling_bars"])
        for bars in self.bars.values():
            for b in [b for b in bars if b < keep_from]:
                del bars[b]

    def _ops(self, row: dict, w: Window, covered: float) -> None:
        def put(name, value, valid):
            ok = bool(valid) and value is not None
            row[name], row[f"{name}_valid"] = (det.as_float(value) if ok else None), ok

        enough = w.latencies.seen >= self.c["min_ops_rows"]
        lat = np.array(w.latencies.items) if w.latencies.items else np.array([0.0])
        for q in (50, 95, 99):
            put(f"inference_latency_p{q}", np.percentile(lat, q), enough)
        put("cpu_utilization",
            np.mean(w.cpu) / self.cpu_count if w.cpu else None, w.cpu)
        put("gpu_utilization", np.mean(w.gpu) if w.gpu else None, w.gpu_ok)
        put("memory_headroom", min(w.headroom) if w.headroom else None, w.headroom)
        put("request_rate", w.n_requests / covered if covered > 0 else None, covered > 0)
        n, ok = w.n_requests, w.n_requests >= self.c["min_ops_rows"]
        put("timeout_rate", sum(w.status[s] for s in TIMEOUT) / max(n, 1), ok)
        put("exception_rate", sum(w.status[s] for s in EXCEPTION) / max(n, 1), ok)
        put("retry_rate", w.retries / max(n, 1), ok)
        put("queue_depth", np.mean(w.queue) if w.queue else None, w.queue)
        put("feed_staleness_s",
            w.end - min(self.last_tick.values()) if self.last_tick else None,
            self.last_tick)
        put("host_anomaly_score",
            np.mean(w.host_anomaly) if w.host_anomaly else None, w.host_anomaly)
        assert all(f"{n}_valid" in row for n in OPS_FIELDS)

    def _quality(self, row: dict, w: Window, partial: bool) -> None:
        expected = self.c.get("expected_requests_per_window")
        if expected is None and len(self.counts_hist) >= 3:
            expected = float(np.median(self.counts_hist))
        if expected:
            frac = (w.end - max(w.start, self.started)) / self.W
            row["row_count_vs_expected"] = w.n_inferences / (expected * frac)
        if not partial:
            self.counts_hist.append(w.n_inferences)

        dropped, self.carry_dropped = self.carry_dropped, 0
        total = dropped + w.n_events
        row["ingest_dropped_fraction"] = dropped / total if total else 0.0
        ratio = row["row_count_vs_expected"]
        row["ingest_incomplete_flag"] = bool(
            partial or dropped or (ratio is not None and ratio < 0.5))


def _aligned(returns: dict[str, dict[int, float]]) -> np.ndarray:
    """bars x assets matrix over the bars every asset traded in."""
    if len(returns) < 2:
        return np.empty((0, 0))
    common = sorted(set.intersection(*(set(r) for r in returns.values())))
    return np.array([[r[b] for r in returns.values()] for b in common]).reshape(
        len(common), len(returns))
