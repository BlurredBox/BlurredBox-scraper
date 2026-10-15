"""Pure statistics for the monitor_v4 table. Arrays in, floats out, no state.

Every function returns None rather than a number it cannot honestly compute
(too few rows, zero variance, no labels). monitor.py turns None into a null
column; nothing here ever substitutes 0.0 for "unknown".

Scale conventions, chosen so the aggregates that leave the partner are
comparable across partners with different feature counts and units:
  - numeric inputs are standardised by the *reference* mean/std first;
  - mahalanobis_distance is divided by sqrt(d), so ~1.0 means "no shift";
  - top_eigenvalue_ratio and pca_reconstruction_error are window/reference
    ratios, so ~1.0 means "no shift";
  - domain_classifier_drift is a cross-validated AUC, so 0.5 means "no shift".
"""

from __future__ import annotations

import math
from collections import Counter

import numpy as np
from scipy import stats
from scipy.spatial.distance import jensenshannon

EPS = 1e-4
N_BINS = 10            # must-collect.md: psi_score n_bins=10


def _finite(x) -> np.ndarray:
    a = np.asarray(x, dtype=float)
    return a[np.isfinite(a)]


def as_float(x) -> float | None:
    """float, or None for nan/inf — parquet gets a null, not a NaN."""
    if x is None:
        return None
    x = float(x)
    return x if math.isfinite(x) else None


# ── 1. per-feature drift (partner-local) ─────────────────────────────────────
def _proportions(counts: np.ndarray) -> np.ndarray:
    p = counts.astype(float) + EPS
    return p / p.sum()


def _binned(ref_p: np.ndarray, win_p: np.ndarray, n_win: int, win_counts) -> dict:
    psi = float(np.sum((win_p - ref_p) * np.log(win_p / ref_p)))
    js = float(jensenshannon(ref_p, win_p, base=2) ** 2)
    expected = ref_p * n_win
    chi2 = float(np.sum((np.asarray(win_counts) - expected) ** 2 / expected))
    hell = float(np.sqrt(0.5 * np.sum((np.sqrt(ref_p) - np.sqrt(win_p)) ** 2)))
    return {"psi": psi, "js_divergence": js, "chi_square": chi2, "hellinger": hell}


def numeric_edges(ref) -> np.ndarray | None:
    """Reference-quantile bin edges, open at both ends. None if constant."""
    r = _finite(ref)
    if r.size < N_BINS:
        return None
    edges = np.unique(np.quantile(r, np.linspace(0, 1, N_BINS + 1)[1:-1]))
    return edges if edges.size else None


def numeric_drift(ref, win, edges: np.ndarray | None) -> dict:
    r, w = _finite(ref), _finite(win)
    out = dict.fromkeys(("psi", "js_divergence", "ks_statistic", "wasserstein",
                         "chi_square", "hellinger"))
    if r.size < 2 or w.size < 2:
        return out
    out["ks_statistic"] = float(stats.ks_2samp(r, w).statistic)
    sd = r.std()
    if sd > 0:
        out["wasserstein"] = float(stats.wasserstein_distance(r, w) / sd)
    if edges is not None:
        rc = np.bincount(np.searchsorted(edges, r, side="right"), minlength=edges.size + 1)
        wc = np.bincount(np.searchsorted(edges, w, side="right"), minlength=edges.size + 1)
        out |= _binned(_proportions(rc), _proportions(wc), w.size, wc)
    return out


def categorical_drift(ref: list, win: list, rng: np.random.Generator) -> dict:
    out = dict.fromkeys(("psi", "js_divergence", "chi_square", "hellinger",
                         "new_category_rate", "cardinality_change"))
    r = [v for v in ref if v is not None]
    w = [v for v in win if v is not None]
    if not r or not w:
        return out
    rc, wc = Counter(r), Counter(w)
    cats = sorted(set(rc) | set(wc), key=str)
    rv = np.array([rc.get(c, 0) for c in cats])
    wv = np.array([wc.get(c, 0) for c in cats])
    out |= _binned(_proportions(rv), _proportions(wv), len(w), wv)
    out["new_category_rate"] = sum(n for c, n in wc.items() if c not in rc) / len(w)
    # Distinct counts grow with n, so compare against an equal-size reference draw.
    k = min(len(w), len(r))
    ref_draw = rng.choice(np.array(r, dtype=object), size=k, replace=False)
    ref_distinct = len(set(ref_draw.tolist()))
    out["cardinality_change"] = len(wc) / ref_distinct - 1.0
    return out


# ── 1b. multivariate drift (aggregates leave the partner) ────────────────────
def mmd_rbf(ref_z: np.ndarray, win_z: np.ndarray, bandwidth: float,
            rng: np.random.Generator, m: int = 500) -> float | None:
    """Unbiased MMD^2 with an RBF kernel, clipped at 0."""
    if len(ref_z) < 2 or len(win_z) < 2:
        return None
    x = ref_z[rng.choice(len(ref_z), min(m, len(ref_z)), replace=False)]
    y = win_z[rng.choice(len(win_z), min(m, len(win_z)), replace=False)]

    def k(a, b):
        d2 = (a * a).sum(1)[:, None] + (b * b).sum(1)[None, :] - 2 * a @ b.T
        return np.exp(-np.maximum(d2, 0) / (2 * bandwidth ** 2))

    kxx, kyy, kxy = k(x, x), k(y, y), k(x, y)
    n, mm = len(x), len(y)
    v = ((kxx.sum() - np.trace(kxx)) / (n * (n - 1))
         + (kyy.sum() - np.trace(kyy)) / (mm * (mm - 1))
         - 2 * kxy.mean())
    return max(0.0, float(v))


def median_bandwidth(ref_z: np.ndarray, rng: np.random.Generator, m: int = 500) -> float:
    x = ref_z[rng.choice(len(ref_z), min(m, len(ref_z)), replace=False)]
    d2 = (x * x).sum(1)[:, None] + (x * x).sum(1)[None, :] - 2 * x @ x.T
    d = np.sqrt(np.maximum(d2[np.triu_indices(len(x), 1)], 0))
    med = float(np.median(d)) if d.size else 1.0
    return med if med > 0 else 1.0


def mahalanobis_mean(win_z: np.ndarray, precision: np.ndarray) -> float | None:
    if len(win_z) == 0:
        return None
    d2 = np.einsum("ij,jk,ik->i", win_z, precision, win_z)
    return float(np.sqrt(np.maximum(d2, 0)).mean() / math.sqrt(win_z.shape[1]))


def covariance_delta(ref_cov: np.ndarray, win_z: np.ndarray) -> float | None:
    """RMS difference of the two covariance matrices in reference-z units."""
    if len(win_z) < 3:
        return None
    wc = np.atleast_2d(np.cov(win_z, rowvar=False))
    return float(np.sqrt(np.mean((wc - ref_cov) ** 2)))


def top_eigenvalue_ratio(ref_top: float, win_z: np.ndarray) -> float | None:
    if len(win_z) < 3 or ref_top <= 0:
        return None
    wc = np.atleast_2d(np.cov(win_z, rowvar=False))
    return float(np.linalg.eigvalsh(wc)[-1] / ref_top)


# ── 2. anomaly detectors ─────────────────────────────────────────────────────
class HalfSpaceTrees:
    """Half-space trees (Tan, Ting & Liu 2011), batch mass profile.

    Inputs are scaled to [0,1] by the reference range; each tree splits a
    random dimension at the midpoint of its current work range. The score of a
    point is sum(mass * 2**depth) at the node where it stops (leaf, or mass
    under `size_limit`). Low score = sparse region = anomalous.
    """

    def __init__(self, n_trees: int = 25, depth: int = 10, seed: int = 0):
        self.n_trees, self.depth = n_trees, depth
        self.rng = np.random.default_rng(seed)

    def fit(self, x: np.ndarray) -> "HalfSpaceTrees":
        self.lo = x.min(0)
        span = x.max(0) - self.lo
        self.span = np.where(span > 0, span, 1.0)
        u = self._scale(x)
        d = x.shape[1]
        n_nodes = 2 ** self.depth - 1
        self.size_limit = max(1.0, 0.1 * len(x))
        self.dims, self.splits, self.mass = [], [], []
        for _ in range(self.n_trees):
            dims = self.rng.integers(0, d, n_nodes)
            splits = np.empty(n_nodes)
            # Random work range per tree, then midpoints down the tree.
            s = self.rng.uniform(0, 1, d)
            lo_r = s - 2 * np.maximum(s, 1 - s)
            hi_r = s + 2 * np.maximum(s, 1 - s)
            lo, hi = np.tile(lo_r, (2 ** self.depth * 2, 1)), np.tile(hi_r, (2 ** self.depth * 2, 1))
            for node in range(n_nodes):
                q = dims[node]
                mid = (lo[node, q] + hi[node, q]) / 2
                splits[node] = mid
                left, right = 2 * node + 1, 2 * node + 2
                lo[left], hi[left] = lo[node], hi[node]
                lo[right], hi[right] = lo[node], hi[node]
                hi[left, q] = mid
                lo[right, q] = mid
            self.dims.append(dims)
            self.splits.append(splits)
            self.mass.append(self._mass(u, dims, splits))
        return self

    def _scale(self, x):
        return (x - self.lo) / self.span

    def _paths(self, u, dims, splits):
        node = np.zeros(len(u), dtype=int)
        path = [node]
        for _ in range(self.depth):
            go_right = u[np.arange(len(u)), dims[np.minimum(node, len(dims) - 1)]] \
                > splits[np.minimum(node, len(splits) - 1)]
            node = 2 * node + 1 + go_right
            path.append(node)
        return path

    def _mass(self, u, dims, splits):
        mass = np.zeros(2 ** (self.depth + 1) - 1)
        for node in self._paths(u, dims, splits):
            mass += np.bincount(node, minlength=mass.size)[: mass.size]
        return mass

    def score(self, x: np.ndarray) -> np.ndarray:
        u = self._scale(x)
        total = np.zeros(len(u))
        for dims, splits, mass in zip(self.dims, self.splits, self.mass):
            done = np.zeros(len(u), dtype=bool)
            for k, node in enumerate(self._paths(u, dims, splits)):
                m = mass[node]
                stop = ~done & ((m <= self.size_limit) | (k == self.depth))
                total[stop] += m[stop] * 2.0 ** k
                done |= stop
        return total


# ── 3. output shape ──────────────────────────────────────────────────────────
def score_shape(scores) -> dict:
    s = _finite(scores)
    keys = ("score_mean", "score_std", "score_skew", "score_kurtosis",
            "score_p05", "score_p25", "score_p50", "score_p75", "score_p95")
    out = dict.fromkeys(keys)
    if s.size == 0:
        return out
    out["score_mean"], out["score_std"] = float(s.mean()), float(s.std())
    if s.size >= 3 and s.std() > 0:
        out["score_skew"] = float(stats.skew(s))
        out["score_kurtosis"] = float(stats.kurtosis(s))      # excess
    q = np.quantile(s, [0.05, 0.25, 0.5, 0.75, 0.95])
    for k, v in zip(keys[4:], q):
        out[k] = float(v)
    return out


def to_probs(score=None, probs=None) -> np.ndarray | None:
    """One prediction as a class-probability vector, or None if it isn't one."""
    if probs is not None:
        p = np.asarray(probs, dtype=float)
        if p.ndim == 1 and p.size >= 2 and np.all(p >= 0) and abs(p.sum() - 1) < 1e-3:
            return p / p.sum()
        return None
    if score is not None and 0.0 <= float(score) <= 1.0:
        return np.array([1.0 - float(score), float(score)])
    return None


def predictive_entropy(prob_rows: list[np.ndarray]) -> float | None:
    if not prob_rows:
        return None
    return float(np.mean([-(p * np.log(np.clip(p, 1e-12, 1))).sum() for p in prob_rows]))


# ── 4. calibration ───────────────────────────────────────────────────────────
def calibration(prob_rows: list[np.ndarray], labels: list[int],
                rng: np.random.Generator, n_bins: int = 10, n_boot: int = 200) -> dict:
    out = dict.fromkeys(("brier", "nll", "ece", "mce", "ci_width", "realised_metric"))
    pairs = [(p, int(y)) for p, y in zip(prob_rows, labels) if 0 <= int(y) < p.size]
    if not pairs:
        return out
    brier, nll, conf, correct = [], [], [], []
    for p, y in pairs:
        onehot = np.zeros(p.size)
        onehot[y] = 1
        # Binary: the conventional (p - y)^2. Multiclass: the sum over classes.
        sq = (p - onehot) ** 2
        brier.append(float(sq[1] if p.size == 2 else sq.sum()))
        nll.append(-math.log(max(p[y], 1e-12)))
        conf.append(float(p.max()))
        correct.append(float(np.argmax(p) == y))
    conf, correct = np.array(conf), np.array(correct)
    out["brier"], out["nll"] = float(np.mean(brier)), float(np.mean(nll))
    idx = np.minimum((conf * n_bins).astype(int), n_bins - 1)
    gaps = [abs(correct[idx == b].mean() - conf[idx == b].mean())
            for b in range(n_bins) if np.any(idx == b)]
    weights = [np.mean(idx == b) for b in range(n_bins) if np.any(idx == b)]
    out["ece"] = float(np.dot(gaps, weights))
    out["mce"] = float(max(gaps))
    out["realised_metric"] = float(correct.mean())
    out["ci_width"] = bootstrap_ci_width(correct, rng, n_boot)
    return out


def bootstrap_ci_width(x, rng: np.random.Generator, n_boot: int = 200) -> float | None:
    """Width of the 95% percentile-bootstrap CI of the mean."""
    a = _finite(x)
    if a.size < 2:
        return None
    means = a[rng.integers(0, a.size, (n_boot, a.size))].mean(1)
    lo, hi = np.quantile(means, [0.025, 0.975])
    return float(hi - lo)


# ── 5. trading ───────────────────────────────────────────────────────────────
def signal_vs_move(signals, moves) -> dict:
    s, m = np.asarray(signals, float), np.asarray(moves, float)
    keep = np.isfinite(s) & np.isfinite(m)
    s, m = s[keep], m[keep]
    out = dict.fromkeys(("tr_hit_rate", "tr_realised_error",
                         "tr_signal_move_corr", "tr_signal_move_ic"))
    if s.size == 0:
        return out
    nz = s != 0
    if nz.any():
        out["tr_hit_rate"] = float(np.mean(np.sign(s[nz]) == np.sign(m[nz])))
    out["tr_realised_error"] = float(np.mean(np.abs(s - m)))
    if s.size >= 3 and s.std() > 0 and m.std() > 0:
        out["tr_signal_move_corr"] = float(np.corrcoef(s, m)[0, 1])
        out["tr_signal_move_ic"] = as_float(stats.spearmanr(s, m).statistic)
    return out


def hits(signals, moves) -> list[float]:
    """Per-pair hit (1/0), zero signals excluded — feeds the rolling hit rate."""
    return [float(np.sign(s) == np.sign(m)) for s, m in zip(signals, moves)
            if s != 0 and math.isfinite(s) and math.isfinite(m)]


def log_returns(closes) -> np.ndarray:
    c = np.asarray(closes, float)
    c = c[np.isfinite(c) & (c > 0)]
    return np.diff(np.log(c)) if c.size >= 2 else np.array([])


def amihud(returns, dollar_volumes) -> float | None:
    r, dv = np.asarray(returns, float), np.asarray(dollar_volumes, float)
    keep = dv > 0
    return float(np.mean(np.abs(r[keep]) / dv[keep])) if keep.any() else None


def roll_spread(prices) -> float | None:
    """Roll (1984) effective spread from tick prices, as a fraction of price."""
    p = _finite(prices)
    if p.size < 4:
        return None
    dp = np.diff(p)
    c = np.cov(dp[1:], dp[:-1])[0, 1]
    return float(2 * math.sqrt(-c) / p.mean()) if c < 0 else 0.0


def mean_pairwise_corr(matrix: np.ndarray) -> float | None:
    """matrix: bars x assets of aligned returns. Mean off-diagonal Pearson."""
    if matrix.ndim != 2 or matrix.shape[1] < 2 or matrix.shape[0] < 3:
        return None
    sd = matrix.std(0)
    keep = sd > 0
    if keep.sum() < 2:
        return None
    c = np.corrcoef(matrix[:, keep], rowvar=False)
    return as_float(c[np.triu_indices_from(c, 1)].mean())


def lag1_autocorr(x) -> float | None:
    a = _finite(x)
    if a.size < 3 or a[1:].std() == 0 or a[:-1].std() == 0:
        return None
    return float(np.corrcoef(a[1:], a[:-1])[0, 1])
