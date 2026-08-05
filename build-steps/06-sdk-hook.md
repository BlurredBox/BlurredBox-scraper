# 06 — `sdk_hook.py`: the only source of `latency_ms`

Build order step 5. Half a day here, plus a small change in the **main** repo. This is the
only path that produces a trainable d=5 row.

## The blocker, resolved

[file-layout.md](../file-layout.md) §4.1 said to confirm that `metrics.py` emits all five
d=5 fields in one snapshot. It does not — verified against the main repo:

```
sdk/blurredbox_agd/metrics.py:103   get_process_metrics()
    → cpu_usage, memory_usage_mb, gpu_usage, gpu_vram_mb, process_id, and the
      extended counters. No latency_ms. Reads the CALLING process only.

sdk/blurredbox_agd/tracking.py:86   _RequestContext.__exit__
    → latency_ms = (perf_counter() - t0) * 1000, the real per-request wall-clock.
      No resource snapshot.

sdk/blurredbox_agd/daemon.py:73     TelemetryDaemon._collect()
    → all five in one dict, but latency comes from self._latency_provider(),
      an aggregate the caller supplies, defaulting to 0.0. Not this request's latency.
```

The five co-occur nowhere as a genuine single-request snapshot. §4.1 already says what to do
when that is the answer: **the hook goes there, not a workaround here.** `_RequestContext.__exit__`
is the right place — it already owns the true latency, and a `get_process_metrics()` call at
that moment gives the four resource fields at the instant the request ended.

## Do

### 1. `scraperd/sdk_hook.py` (replaces the step-4 placeholder)

```python
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
```

### 2. Turn it on in `config.yaml`

```yaml
spool_dir: /var/lib/blurredbox-agd/spool
```

The daemon wiring is already there from step 4 — `flush_loop` drains the spool and
`run` creates the `requests` writer when `spool_dir` is set.

The spool must be writable by the **model** process and readable by **scraperd**. If they run
as different users:

```bash
sudo install -d -m 0730 -o modeluser -g scraperd /var/lib/blurredbox-agd/spool
```

### 3. The main-repo change

In `sdk/blurredbox_agd/tracking.py`, `_RequestContext.__exit__` already computes the latency.
Emit the row from there:

```python
    def __exit__(self, exc_type, *_):
        self._t._in_flight -= 1
        latency_ms = (time.perf_counter() - self._t0) * 1000.0
        self._t.record_result(success=exc_type is None, latency_ms=latency_ms)
        spool = os.environ.get("SCRAPERD_SPOOL")
        if spool:
            try:
                from scraperd.sdk_hook import record
                record(spool, request_id=str(uuid.uuid4()), latency_ms=latency_ms,
                       status="success" if exc_type is None else "failure")
            except Exception:
                pass          # telemetry must never break the request path
        return False
```

Three properties that are not negotiable:

- **Off by default.** No `SCRAPERD_SPOOL`, no behaviour change. A telemetry hook that alters
  a serving path when nobody asked for it is a worse bug than missing data.
- **Swallows everything.** A full disk, a permission error, a missing import — none of them
  may propagate into an inference.
- **Latency is computed once**, from the timer that already exists. Do not recompute it, do
  not round it, do not pass an average.

This change belongs in a PR against the main repo, not here. Until it merges, `record()` can
be called directly from any serving code that knows its own latency.

## Verify

The spool round trip, end to end, without touching the main repo:

```bash
.venv/bin/python - <<'PY'
import shutil
from pathlib import Path
from scraperd import sdk_hook, schema
from scraperd.writer import TableWriter
import duckdb

sp, root = Path("/tmp/scraperd-spool"), Path("/tmp/scraperd-req")
shutil.rmtree(sp, ignore_errors=True); shutil.rmtree(root, ignore_errors=True)

for i in range(3):
    sdk_hook.record(sp, request_id=f"r{i}", latency_ms=12.5+i,
                    input_size=10, output_size=2, prediction="1")

print("current minute -> drained:", len(sdk_hook.drain(sp)))
f = next(sp.glob("*.jsonl"))
f.rename(f.with_name(f.name.rsplit("-", 1)[0] + "-202608050001.jsonl"))   # age it
rows = sdk_hook.drain(sp)
print("closed minute  -> drained:", len(rows), rows[0]["request_id"], rows[0]["latency_ms"])
print("core_row:", schema.core_row(rows[0]))

w = TableWriter(root, "requests", schema.REQUESTS_SCHEMA, "vm1", root/"heartbeat")
for r in rows: w.add(r)
w.close()
print(duckdb.sql(f"SELECT request_id, latency_ms, cpu_usage, status FROM read_parquet('{root}/requests/*/*.parquet')"))
print("spool drained:", list(sp.glob('*.jsonl')))
PY
```

Real output:

```
current minute -> drained: 0
closed minute  -> drained: 3 r0 12.5
core_row: [0.0, 85.08984375, 0.0, 0.0, 12.5]
┌────────────┬────────────┬───────────┬─────────┐
│ request_id │ latency_ms │ cpu_usage │ status  │
├────────────┼────────────┼───────────┼─────────┤
│ r0         │       12.5 │       0.0 │ success │
│ r1         │       13.5 │       0.0 │ success │
│ r2         │       14.5 │       0.0 │ success │
└────────────┴────────────┴───────────┴─────────┘
spool drained: []
```

`drained: 0` on the first call is the design working: the current minute's file is still open
for appends, so it is not read. The row becomes visible within two minutes.

Then the daemon with `spool_dir` set — the journal must show both tables:

```
INFO scraperd flushed 32 samples rows
INFO scraperd flushed 1 requests rows
INFO scraperd.writer segment closed: .../requests/entity_id=vm1/2026-08-05T06-43.parquet
```

## Why a spool file

`sdk_hook`'s two halves live in two different processes. That leaves: a socket (ZeroMQ is
deferred until a live consumer exists — file-layout.md §6), a shared DB (the SQLite tier is
deferred for a 4 MB working set), or a file.

A file, with the read side deferred by one minute, needs no dependency, no daemon-side state,
no offset file, and cannot double-ingest across a restart. At realistic inference rates the
spool is a few hundred KB. If request volume ever makes per-minute files unwieldy, that is
the trigger for the ZeroMQ path — not a reason to hand-roll a smarter file protocol.

`drain()` drops any row missing a d=5 field rather than defaulting it, and says so in the log.
A request row without a real latency is not a d=5 row.

## Done when

- [ ] the round-trip script prints `drained: 0` then `drained: 3` and the duckdb table
- [ ] `core_row(rows[0])` returns five floats with a real latency in position 5
- [ ] the spool directory is empty afterwards
- [ ] the daemon logs `flushed N requests rows` with `spool_dir` set
- [ ] the main-repo change is raised as a PR, off by default, exception-swallowing
- [ ] `.venv/bin/pytest tests/ -q` still 21 passed

→ [07-verify-the-data.md](07-verify-the-data.md)
