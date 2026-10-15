"""Per-request rows — the only source of latency_ms.

Two halves on purpose. `record()` runs inside the *model* process and appends one
JSON line to a per-minute spool file. `drain()` runs inside scraperd and turns
closed spool files into `requests` rows. A file is the transport because the two
are separate processes and ZeroMQ is deferred (file-layout.md §6); a spool file
is only consumed once its minute has passed, so no reader ever sees a partial
line and a restart cannot double-ingest.

monitor_v4 rides the same spool. Every line carries a `kind`: "request" (the
default, so pre-v4 spool lines still parse), "outcome", "market", "signal" or
"control". Features, scores and signals stay on this host: drain_all() hands
them to monitor.py, and only REQUEST_FIELDS ever reach the requests table.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from .schema import CORE_V1, REQUEST_FIELDS, SERVER_ONLY_FIELDS

log = logging.getLogger("scraperd.sdk_hook")

_ZERO = {"cpu_usage": 0.0, "memory_usage_mb": 0.0, "gpu_usage": 0.0, "gpu_vram_mb": 0.0}


def _resources() -> dict:
    """The four resource fields of the d=5, sampled now, in this process."""
    try:
        from blurredbox_agd.metrics import get_process_metrics

        m = get_process_metrics()
        return {k: float(m[k]) for k in _ZERO} | {"process_id": m["process_id"]}
    except Exception:
        pass
    try:
        import psutil

        p = psutil.Process()
        return _ZERO | {
            "cpu_usage": float(p.cpu_percent(interval=None)),
            "memory_usage_mb": p.memory_info().rss / (1024 * 1024),
            "process_id": p.pid,
        }
    except Exception:
        return _ZERO | {"process_id": os.getpid()}


def _append(spool_dir: str | Path, row: dict) -> None:
    d = Path(spool_dir)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{os.getpid()}-{datetime.now(timezone.utc):%Y%m%d%H%M}.jsonl"
    with open(path, "a") as fh:          # O_APPEND: one line, one atomic write
        fh.write(json.dumps(row) + "\n")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def record(
    spool_dir: str | Path,
    request_id: str,
    latency_ms: float,
    status: str = "success",
    input_size: int = 0,
    output_size: int = 0,
    prediction: str = "",
    *,
    features: dict | None = None,
    score: float | None = None,
    probs: list[float] | None = None,
    decision: bool | None = None,
    signal: float | None = None,
    asset: str | None = None,
    position: float | None = None,
    retry: bool = False,
    queue_depth: float | None = None,
) -> None:
    """Call at the end of one inference, with that request's own wall-clock.

    latency_ms must be the measured duration of *this* request. Never pass a
    rolling average, and never let a poll sample supply it — invariant 1.

    The keyword-only arguments feed monitor_v4 and never reach the requests
    table. `features` is {name: value} — numbers are numeric features, strings
    categorical; names stay on this host. `score` is the model's scalar output
    (a probability in [0,1] for binary models), `probs` the class-probability
    vector for multiclass. `signal` is a trading signal read as a predicted
    move, `position` the resulting position if it differs from the signal.
    `status` "timeout", "error" and "shed" drive the ops rates.
    """
    row = _resources() | {
        "kind": "request",
        "request_id": request_id,
        "latency_ms": float(latency_ms),
        "status": status,
        "input_size": int(input_size),
        "output_size": int(output_size),
        "prediction": str(prediction),
        "timestamp": _now(),
    }
    extra = {"features": features, "score": score, "probs": probs,
             "decision": decision, "signal": signal, "asset": asset,
             "position": position, "queue_depth": queue_depth}
    row |= {k: v for k, v in extra.items() if v is not None}
    if retry:
        row["retry"] = True
    _append(spool_dir, row)


def record_outcome(
    spool_dir: str | Path,
    request_id: str,
    label: int | None = None,
    realised_move: float | None = None,
) -> None:
    """The real outcome of an earlier request, whenever it becomes known.

    `label` is the true class index (calibration, realised_metric);
    `realised_move` the move the signal was predicting (tr_* performance).
    """
    _append(spool_dir, {"kind": "outcome", "request_id": request_id,
                        "label": label, "realised_move": realised_move,
                        "timestamp": _now()})


def record_market(
    spool_dir: str | Path,
    asset: str,
    price: float,
    volume: float = 0.0,
    bid: float | None = None,
    ask: float | None = None,
) -> None:
    """One market tick. Drives realised vol, liquidity, spread, cross-asset
    correlation and feed_staleness_s."""
    _append(spool_dir, {"kind": "market", "asset": asset, "price": price,
                        "volume": volume, "bid": bid, "ask": ask,
                        "timestamp": _now()})


def record_signal(spool_dir: str | Path, name: str, value: float) -> None:
    """An externally computed value: `host_anomaly_score` (System A) or
    `queue_depth`. Other names are ignored by the monitor."""
    _append(spool_dir, {"kind": "signal", "name": name, "value": value,
                        "timestamp": _now()})


def request_rebaseline(spool_dir: str | Path) -> None:
    """Discard the drift reference and collect a new one, e.g. after a
    deliberate model or feature change."""
    _append(spool_dir, {"kind": "control", "action": "rebaseline", "timestamp": _now()})


def drain(spool_dir: str | Path) -> list[dict]:
    """Consume every closed spool file; return only the `requests` rows."""
    return drain_all(spool_dir)["requests"]


def drain_all(spool_dir: str | Path) -> dict:
    """Consume every spool file whose minute has closed. Deletes what it reads.

    Returns {"requests": table rows, "events": every parsed event with an
    epoch `ts`, "errors": lines dropped, "watermark": epoch second before
    which all spool files have now been consumed}.
    """
    d = Path(spool_dir)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    out: dict = {"requests": [], "events": [], "errors": 0,
                 "watermark": now.timestamp()}
    if not d.is_dir():
        return out
    now_minute = f"{now:%Y%m%d%H%M}"
    for path in sorted(d.glob("*.jsonl")):
        if path.stem.rsplit("-", 1)[-1] >= now_minute:
            continue                      # still being appended to
        for line in path.read_text().splitlines():
            try:
                r = json.loads(line)
                for k in SERVER_ONLY_FIELDS & r.keys():
                    log.warning("stripping server-only field %s from spool line", k)
                    del r[k]
                r["timestamp"] = datetime.fromisoformat(r["timestamp"])
                r["ts"] = r["timestamp"].timestamp()
                if r.setdefault("kind", "request") == "request":
                    missing = [k for k in CORE_V1 if r.get(k) is None]
                    if missing:
                        log.warning("dropping request row missing %s", missing)
                        out["errors"] += 1
                        continue
                    out["requests"].append({k: r.get(k) for k in REQUEST_FIELDS})
                out["events"].append(r)
            except Exception as exc:
                log.warning("bad spool line in %s: %s", path.name, exc)
                out["errors"] += 1
        path.unlink()
    return out
