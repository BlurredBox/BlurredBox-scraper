"""Per-request rows — the only source of latency_ms.

Two halves on purpose. `record()` runs inside the *model* process and appends one
JSON line to a per-minute spool file. `drain()` runs inside scraperd and turns
closed spool files into `requests` rows. A file is the transport because the two
are separate processes and ZeroMQ is deferred (file-layout.md §6); a spool file
is only consumed once its minute has passed, so no reader ever sees a partial
line and a restart cannot double-ingest.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from .schema import CORE_V1, REQUEST_FIELDS

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


def record(
    spool_dir: str | Path,
    request_id: str,
    latency_ms: float,
    status: str = "success",
    input_size: int = 0,
    output_size: int = 0,
    prediction: str = "",
) -> None:
    """Call at the end of one inference, with that request's own wall-clock.

    latency_ms must be the measured duration of *this* request. Never pass a
    rolling average, and never let a poll sample supply it — invariant 1.
    """
    row = _resources() | {
        "request_id": request_id,
        "latency_ms": float(latency_ms),
        "status": status,
        "input_size": int(input_size),
        "output_size": int(output_size),
        "prediction": str(prediction),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    d = Path(spool_dir)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{os.getpid()}-{datetime.now(timezone.utc):%Y%m%d%H%M}.jsonl"
    with open(path, "a") as fh:          # O_APPEND: one line, one atomic write
        fh.write(json.dumps(row) + "\n")


def drain(spool_dir: str | Path) -> list[dict]:
    """Consume every spool file whose minute has closed. Deletes what it reads."""
    d = Path(spool_dir)
    if not d.is_dir():
        return []
    now_minute = f"{datetime.now(timezone.utc):%Y%m%d%H%M}"
    rows: list[dict] = []
    for path in sorted(d.glob("*.jsonl")):
        if path.stem.rsplit("-", 1)[-1] >= now_minute:
            continue                      # still being appended to
        for line in path.read_text().splitlines():
            try:
                r = json.loads(line)
                r["timestamp"] = datetime.fromisoformat(r["timestamp"])
                missing = [k for k in CORE_V1 if r.get(k) is None]
                if missing:
                    log.warning("dropping request row missing %s", missing)
                    continue
                rows.append({k: r.get(k) for k in REQUEST_FIELDS})
            except Exception as exc:
                log.warning("bad spool line in %s: %s", path.name, exc)
        path.unlink()
    return rows
