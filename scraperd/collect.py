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


def host_metrics(skip: frozenset[str] = frozenset()) -> dict:
    """Host-wide fields shared by every sample in a tick.

    Names in `skip` are neither sampled nor emitted; the column stays in the
    parquet schema and reads back null.
    """
    out = {k: v for k, v in _HOST_DEFAULTS.items() if k not in skip}
    if "disk_usage_pct" not in skip:
        out["disk_usage_pct"] = psutil.disk_usage("/").percent
    if not out:
        return out

    poller = _HOST["poller"]
    if poller is not None:
        try:
            t = poller.to_agd_telemetry()
            # t["latency_ms"] is how long the poll itself took, NOT inference
            # latency. Dropped here on purpose — invariant 1. Never copy it
            # into a sample row.
            for k in _HOST_DEFAULTS:
                if k in out:
                    out[k] = float(t.get(k) or 0.0)
        except Exception as exc:
            log.warning("poller failed, zeros this tick: %s", exc)
        return out

    nvml = _HOST["nvml"]
    if nvml is not None and {"gpu_usage", "gpu_vram_mb", "gpu_temperature"} & set(out):
        try:
            h = nvml.nvmlDeviceGetHandleByIndex(0)
            if "gpu_usage" in out:
                out["gpu_usage"] = float(nvml.nvmlDeviceGetUtilizationRates(h).gpu)
            if "gpu_vram_mb" in out:
                out["gpu_vram_mb"] = nvml.nvmlDeviceGetMemoryInfo(h).used / (1024 * 1024)
            if "gpu_temperature" in out:
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


# Per-process fields that may be switched off in config (`skip_fields`). The
# mandatory rest — process_id, cpu_usage, memory_usage_mb, timestamp — are the
# reason the daemon exists; two of them are CORE_V1.
_OPTIONAL = {
    "memory_pct": lambda p: float(p.memory_percent()),
    "num_threads": lambda p: p.num_threads(),
    "num_fds": _num_fds,
    "num_connections": _num_connections,
    "ctx_switches": lambda p: sum(p.num_ctx_switches()),
}


def sample(proc: psutil.Process, host: dict, skip: frozenset[str] = frozenset()) -> dict:
    """One `samples` row. Raises psutil.NoSuchProcess if the target died.

    Fields in `skip` are not read at all — the syscall is what costs, not the
    column — and are left out of the row, which parquet writes as null.
    """
    with proc.oneshot():
        row = {
            "process_id": proc.pid,
            "cpu_usage": float(proc.cpu_percent(interval=None)),
            "memory_usage_mb": proc.memory_info().rss / (1024 * 1024),
            "timestamp": datetime.now(timezone.utc),
        }
        for name, fn in _OPTIONAL.items():
            if name not in skip:
                row[name] = fn(proc)
    # One io_counters() call yields both, so it is worth making only if a
    # consumer wants at least one of them.
    if {"io_read_bytes", "io_write_bytes"} - skip:
        read, write = _io(proc)
        if "io_read_bytes" not in skip:
            row["io_read_bytes"] = read
        if "io_write_bytes" not in skip:
            row["io_write_bytes"] = write
    row.update(host)
    return row
