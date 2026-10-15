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


def gpu_available() -> bool:
    """True when gpu_* fields are measured rather than defaulted to 0.0."""
    return _HOST["poller"] is not None or _HOST["nvml"] is not None


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
