# 04 — `collect.py`, `daemon.py`, `config.yaml`

Build order step 3. The smallest thing that writes real rows. One day. At the end of this
step `scraperd --config config.yaml` samples live processes and leaves parquet on disk.

## Do

### 1. `scraperd/collect.py`

```python
"""1 Hz per-process sampling. Returns plain dicts. Emits no latency.

Per-process counters come from psutil directly — the SDK's get_process_metrics()
only ever reads the *calling* process, so it cannot sample a watched target.
Host-level GPU/freqtrade/disk prefer the main repo's SystemMetricsPoller where
importable (invariant 8) and fall back to pynvml + psutil otherwise.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

import psutil

log = logging.getLogger("scraperd.collect")

_HOST: dict = {"poller": None, "nvml": None, "ready": False}

_HOST_DEFAULTS = {
    "gpu_usage": 0.0,
    "gpu_vram_mb": 0.0,
    "gpu_temperature": 0.0,
    "freqtrade_cpu": 0.0,
    "freqtrade_memory_mb": 0.0,
}


def init_host(agd_repo: str | None = None) -> None:
    """Pick the host-metrics backend once, at startup."""
    if _HOST["ready"]:
        return
    _HOST["ready"] = True
    if agd_repo:
        sys.path.insert(0, str(agd_repo))
        try:
            from src.connectors.system_metrics_poller import SystemMetricsPoller

            _HOST["poller"] = SystemMetricsPoller()
            log.info("host metrics: SystemMetricsPoller from %s", agd_repo)
            return
        except Exception as exc:
            log.info("SystemMetricsPoller unavailable (%s) — falling back", exc)
    try:
        import pynvml

        pynvml.nvmlInit()
        _HOST["nvml"] = pynvml
        log.info("host metrics: pynvml direct")
    except Exception as exc:
        log.info("no GPU backend (%s) — gpu_* will be 0.0", exc)


def host_metrics() -> dict:
    """Host-wide fields shared by every sample in a tick."""
    out = dict(_HOST_DEFAULTS)
    out["disk_usage_pct"] = psutil.disk_usage("/").percent

    poller = _HOST["poller"]
    if poller is not None:
        try:
            t = poller.to_agd_telemetry()
            # t["latency_ms"] is how long the poll itself took, NOT inference
            # latency. Dropped here on purpose — invariant 1. Never copy it
            # into a sample row.
            for k in _HOST_DEFAULTS:
                out[k] = float(t.get(k) or 0.0)
        except Exception as exc:
            log.warning("poller failed, zeros this tick: %s", exc)
        return out

    nvml = _HOST["nvml"]
    if nvml is not None:
        try:
            h = nvml.nvmlDeviceGetHandleByIndex(0)
            out["gpu_usage"] = float(nvml.nvmlDeviceGetUtilizationRates(h).gpu)
            out["gpu_vram_mb"] = nvml.nvmlDeviceGetMemoryInfo(h).used / (1024 * 1024)
            out["gpu_temperature"] = float(nvml.nvmlDeviceGetTemperature(h, 0))
        except Exception as exc:
            log.warning("nvml failed, zeros this tick: %s", exc)
    return out


def resolve(patterns: list[str], pids: list[int], own_pid: int) -> dict[int, psutil.Process]:
    """pid -> Process. Objects are cached by the caller: cpu_percent(None) is a
    delta against the previous call on the *same* object, so recreating them
    every tick reports 0.0 forever."""
    found: dict[int, psutil.Process] = {}
    for p in psutil.process_iter(["pid", "name", "cmdline"]):
        pid = p.info["pid"]
        if pid == own_pid:
            continue
        if pid in pids:
            found[pid] = p
            continue
        hay = " ".join(p.info["cmdline"] or []) or (p.info["name"] or "")
        if any(pat in hay for pat in patterns):
            found[pid] = p
    return found


def _num_fds(proc: psutil.Process) -> int:
    try:
        return proc.num_fds()
    except Exception:
        return 0


def _num_connections(proc: psutil.Process) -> int:
    try:
        fn = getattr(proc, "net_connections", None) or proc.connections
        return len(fn(kind="inet"))
    except Exception:
        return 0


def _io(proc: psutil.Process) -> tuple[int, int]:
    try:
        io = proc.io_counters()
        return int(io.read_bytes), int(io.write_bytes)
    except Exception:
        return 0, 0


def sample(proc: psutil.Process, host: dict) -> dict:
    """One `samples` row. Raises psutil.NoSuchProcess if the target died."""
    with proc.oneshot():
        ctx = proc.num_ctx_switches()
        row = {
            "process_id": proc.pid,
            "cpu_usage": float(proc.cpu_percent(interval=None)),
            "memory_usage_mb": proc.memory_info().rss / (1024 * 1024),
            "memory_pct": float(proc.memory_percent()),
            "num_threads": proc.num_threads(),
            "num_fds": _num_fds(proc),
            "num_connections": _num_connections(proc),
            "ctx_switches": ctx.voluntary + ctx.involuntary,
            "timestamp": datetime.now(timezone.utc),
        }
    row["io_read_bytes"], row["io_write_bytes"] = _io(proc)
    row.update(host)
    return row
```

### 2. `scraperd/daemon.py`

```python
"""asyncio loop: sample at cadence, flush on interval, SIGTERM -> flush -> close."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import psutil
import yaml

from . import __version__, collect, schema, sdk_hook
from .writer import TableWriter

log = logging.getLogger("scraperd")


def load_config(path: str | Path) -> dict:
    cfg = yaml.safe_load(Path(path).read_text())
    cfg.setdefault("cadence_seconds", 1)
    cfg.setdefault("flush_seconds", 60)
    cfg.setdefault("segment_seconds", 900)
    cfg.setdefault("max_disk_pct", 90.0)
    cfg.setdefault("watch", {})
    cfg["watch"].setdefault("patterns", [])
    cfg["watch"].setdefault("pids", [])
    cfg["watch"].setdefault("refresh_seconds", 60)
    if not cfg.get("entity_id"):
        raise SystemExit("config: entity_id is required (it is the host — invariant 5)")
    return cfg


def write_manifest(cfg: dict, root: Path) -> Path:
    """Run-constant metadata. One 400-byte file instead of 7.8M repeats."""
    m = {k: "" for k in schema.MANIFEST_FIELDS}
    m.update(cfg.get("manifest") or {})
    m.update({
        "entity_id": cfg["entity_id"],
        "schema_version": schema.SCHEMA_VERSION,
        "run_started_at": datetime.now(timezone.utc).isoformat(),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": psutil.cpu_count(),
        "total_memory_mb": round(psutil.virtual_memory().total / (1024 * 1024), 1),
        "version": cfg.get("manifest", {}).get("version") or __version__,
    })
    unknown = set(m) - set(schema.MANIFEST_FIELDS)
    if unknown:
        raise SystemExit(f"config manifest: unknown key(s) {sorted(unknown)}")
    root.mkdir(parents=True, exist_ok=True)
    path = root / "manifest.json"
    path.write_text(json.dumps(m, indent=2, sort_keys=True) + "\n")
    log.info("manifest: %s", path)
    return path


async def sample_loop(cfg: dict, writer: TableWriter, stop: asyncio.Event) -> None:
    watch, own = cfg["watch"], os.getpid()
    procs = collect.resolve(watch["patterns"], watch["pids"], own)
    log.info("watching %d process(es): %s", len(procs), sorted(procs))
    last_refresh = 0.0
    loop = asyncio.get_running_loop()

    while not stop.is_set():
        now = loop.time()
        if now - last_refresh >= watch["refresh_seconds"]:
            fresh = collect.resolve(watch["patterns"], watch["pids"], own)
            for pid, p in fresh.items():
                procs.setdefault(pid, p)  # keep existing objects: cpu_percent delta
            for pid in [p for p in procs if p not in fresh]:
                log.warning("process %d gone", pid)
                procs.pop(pid)
            last_refresh = now

        host = collect.host_metrics()
        for pid in list(procs):
            try:
                writer.add(collect.sample(procs[pid], host))
            except psutil.Error as exc:
                log.warning("sample %d failed: %s", pid, exc)
                procs.pop(pid, None)

        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg["cadence_seconds"])
        except asyncio.TimeoutError:
            pass


async def flush_loop(cfg: dict, writers: dict[str, TableWriter], stop: asyncio.Event) -> None:
    spool = cfg.get("spool_dir")
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg["flush_seconds"])
        except asyncio.TimeoutError:
            pass
        if spool and "requests" in writers:
            for row in sdk_hook.drain(spool):
                writers["requests"].add(row)
        for name, w in writers.items():
            log.info("flushed %d %s rows", w.flush(), name)


async def run(cfg: dict) -> None:
    root = Path(cfg["output_root"])
    heartbeat = Path(cfg.get("heartbeat") or root / "heartbeat")
    write_manifest(cfg, root)
    collect.init_host(cfg.get("agd_repo"))

    def mk(table, tbl_schema):
        return TableWriter(
            root=root, table=table, schema=tbl_schema,
            entity_id=cfg["entity_id"], heartbeat=heartbeat,
            max_disk_pct=cfg["max_disk_pct"], segment_seconds=cfg["segment_seconds"],
        )

    writers = {"samples": mk("samples", schema.SAMPLES_SCHEMA)}
    if cfg.get("spool_dir"):
        writers["requests"] = mk("requests", schema.REQUESTS_SCHEMA)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    try:
        await asyncio.gather(
            sample_loop(cfg, writers["samples"], stop),
            flush_loop(cfg, writers, stop),
        )
    finally:
        for w in writers.values():
            w.close()           # flush + footer + rename; nothing left as .part
        log.info("stopped cleanly (dropped %d rows total)",
                 sum(w.dropped for w in writers.values()))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="scraperd")
    ap.add_argument("--config", default=os.environ.get("SCRAPERD_CONFIG", "config.yaml"))
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )
    asyncio.run(run(load_config(args.config)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

`daemon.py` imports `sdk_hook`, which does not exist until step 6. Create the placeholder now
so this step runs — step 6 replaces it wholesale:

```bash
cat > scraperd/sdk_hook.py <<'EOF'
"""Placeholder until 06-sdk-hook.md. No producer exists yet."""
def drain(spool_dir):
    return []
EOF
```

### 3. `config.yaml`

```yaml
# Host identity. One entity_id per HOST, not per process — invariant 5.
entity_id: vm1

output_root: /var/lib/blurredbox-agd/cold
heartbeat: /var/lib/blurredbox-agd/cold/heartbeat
# spool_dir: /var/lib/blurredbox-agd/spool   # uncomment at step 6

watch:
  # Substring match against the full cmdline, then the process name.
  patterns: ["freqtrade", "uvicorn"]
  pids: []                 # explicit pids, merged with pattern matches
  refresh_seconds: 60      # rescan for new/dead processes

cadence_seconds: 1         # the 1 Hz poll
flush_seconds: 60          # buffer drain; bounds memory, not crash loss
segment_seconds: 900       # parquet footer interval; bounds crash loss — see D1
max_disk_pct: 90           # refuse to write above this, loudly

# Optional. Path to the BlurredBox-AGD checkout; enables SystemMetricsPoller
# for host-level GPU/freqtrade fields (invariant 8). Omit to use pynvml directly.
# agd_repo: /home/you/Desktop/BlurredBox-AGD

# Run-constant metadata, copied verbatim into manifest.json. Keys must be a
# subset of schema.MANIFEST_FIELDS or startup fails.
manifest:
  model_id: ""
  model_version_id: ""
  slug: ""
  tenant_id: ""
  operator_id: ""
  environment: production
  artifact_id: ""
```

## Verify

Run against a throwaway output root and a pattern that matches something real:

```bash
mkdir -p /tmp/scraperd-smoke
sed -e 's#/var/lib/blurredbox-agd/cold#/tmp/scraperd-smoke/cold#' \
    -e 's#patterns: .*#patterns: ["python3"]#' \
    -e 's#flush_seconds: 60#flush_seconds: 5#' \
    -e 's#segment_seconds: 900#segment_seconds: 3600#' config.yaml > /tmp/smoke.yaml

timeout -s INT 12 .venv/bin/scraperd --config /tmp/smoke.yaml
```

Real output from this exact sequence:

```
2026-08-05 10:43:32,... INFO scraperd manifest: /tmp/scraperd-smoke/cold/manifest.json
2026-08-05 10:43:32,... INFO scraperd.collect no GPU backend (No module named 'pynvml') — gpu_* will be 0.0
2026-08-05 10:43:32,... INFO scraperd watching 30 process(es): [2207, 2263, ...]
2026-08-05 10:43:37,... INFO scraperd flushed 150 samples rows
2026-08-05 10:43:42,... INFO scraperd flushed 150 samples rows
2026-08-05 10:43:44,... INFO scraperd flushed 32 samples rows
2026-08-05 10:43:44,... INFO scraperd.writer segment closed: /tmp/scraperd-smoke/cold/samples/entity_id=vm1/2026-08-05T06-43.parquet
2026-08-05 10:43:44,... INFO scraperd stopped cleanly (dropped 0 rows total)
```

Three things to check in that output: the manifest line, `watching N process(es)` with N > 0,
and **`stopped cleanly`** — that last line is the SIGINT path flushing and writing footers.
If you see no `segment closed` line, the shutdown path is broken and every crash will cost a
full segment.

Then read it back:

```bash
.venv/bin/python -c "
import duckdb
q = \"read_parquet('/tmp/scraperd-smoke/cold/samples/*/*.parquet', hive_partitioning=1)\"
print(duckdb.sql(f'SELECT entity_id, count(*) n, count(DISTINCT process_id) pids, min(timestamp) t0, max(timestamp) t1 FROM {q} GROUP BY 1'))"
```

```
┌───────────┬───────┬───────┬───────────────────────────────┬───────────────────────────────┐
│ entity_id │   n   │ pids  │              t0               │              t1               │
├───────────┼───────┼───────┼───────────────────────────────┼───────────────────────────────┤
│ vm1       │   660 │    30 │ 2026-08-05 10:40:21.751734+04 │ 2026-08-05 10:40:43.237515+04 │
└───────────┴───────┴───────┴───────────────────────────────┴───────────────────────────────┘
```

`entity_id` is a column you never wrote — hive partitioning supplies it from the directory name.

## The traps in this step

**`to_agd_telemetry()` hands you a `latency_ms`. Discard it.**
`src/connectors/system_metrics_poller.py:190` computes `latency_ms` as how long the *poll*
took. Copying it into a sample row would put a plausible-looking float in a column that is
supposed to be impossible to fill from a poll — which is invariant 1 failing quietly rather
than loudly. `collect.host_metrics()` copies only the five keys in `_HOST_DEFAULTS`, and the
comment says why. Do not widen that loop to `t.items()`.

**`get_process_metrics()` reads the caller, not the target.**
`sdk/blurredbox_agd/metrics.py:103` calls a cached `_get_process()` bound to the current
process. Invariant 8 says reuse it — but it is structurally unable to sample a watched pid,
so per-process fields come from psutil here. The poller is still reused for the host-level
fields it does cover. This is decision D6.

**Cache the `psutil.Process` objects.**
`cpu_percent(interval=None)` returns CPU time consumed *since the previous call on that same
object*. A fresh `psutil.Process(pid)` every tick returns `0.0` forever. `sample_loop` keeps
the dict across ticks and `procs.setdefault` on refresh so an existing object is never
replaced. First sample after startup is `0.0` by construction; that is correct, not a bug.

**Permissions.** `io_counters()` and `num_fds()` on another user's process raise
`AccessDenied`, and the helpers return `0` rather than killing the loop. If your `io_read_bytes`
column is all zeros, scraperd is running as the wrong user — see [05-deploy.md](05-deploy.md).

**Segment filenames are UTC.** `2026-08-05T06-43.parquet` was written at 10:43 local. The
timestamps inside are tz-aware UTC. Do not "fix" this to local time; a 90-day run crossing a
DST boundary with local filenames produces two segments with the same name.

## Done when

- [ ] `timeout -s INT 12 .venv/bin/scraperd --config /tmp/smoke.yaml` ends with `stopped cleanly`
- [ ] a `segment closed:` line appeared
- [ ] duckdb returns one row per (pid × second) and `entity_id` = your config value
- [ ] `manifest.json` exists and has your `environment` / `model_id` values
- [ ] `stat -c %y /tmp/scraperd-smoke/cold/heartbeat` is recent
- [ ] `.venv/bin/pytest tests/ -q` still 21 passed

→ [05-deploy.md](05-deploy.md)
