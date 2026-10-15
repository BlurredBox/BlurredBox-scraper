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
    mon = cfg.get("monitor") or {}
    if mon.get("enabled"):
        if not cfg.get("spool_dir"):
            raise SystemExit("config: monitor needs spool_dir — it reads the SDK spool")
        out = Path(cfg["output_root"]).resolve()
        for key in ("state_dir", "partner_local_root"):
            if not mon.get(key):
                raise SystemExit(f"config: monitor.{key} is required")
            p = Path(mon[key]).resolve()
            if p == out or out in p.parents:
                raise SystemExit(
                    f"config: monitor.{key} ({p}) is inside output_root. It holds "
                    "feature names/values, which never leave the partner host.")
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


async def sample_loop(cfg: dict, writer: TableWriter, stop: asyncio.Event,
                      monitor=None) -> None:
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
        cpu_sum = 0.0
        for pid in list(procs):
            try:
                row = collect.sample(procs[pid], host)
                writer.add(row)
                cpu_sum += row["cpu_usage"]
            except psutil.Error as exc:
                log.warning("sample %d failed: %s", pid, exc)
                procs.pop(pid, None)
        if monitor is not None:
            vm = psutil.virtual_memory()
            monitor.observe_tick(
                datetime.now(timezone.utc).timestamp(), cpu_sum, host["gpu_usage"],
                collect.gpu_available(), vm.available / vm.total)

        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg["cadence_seconds"])
        except asyncio.TimeoutError:
            pass


async def flush_loop(cfg: dict, writers: dict[str, TableWriter], stop: asyncio.Event,
                     monitor=None) -> None:
    spool = cfg.get("spool_dir")
    loop = asyncio.get_running_loop()
    seen_dropped = 0
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=cfg["flush_seconds"])
        except asyncio.TimeoutError:
            pass
        if spool and "requests" in writers:
            got = sdk_hook.drain_all(spool)
            for row in got["requests"]:
                writers["requests"].add(row)
            if monitor is not None:
                dropped = sum(w.dropped for w in writers.values())
                # Off the event loop: drift + detectors take a second or two.
                # A monitor bug must not stop samples/requests from landing.
                try:
                    rows, local = await loop.run_in_executor(
                        None, monitor.step, got["events"], got["errors"],
                        got["watermark"], dropped - seen_dropped)
                except Exception:
                    log.exception("monitor step failed; core tables unaffected")
                    rows, local = [], []
                seen_dropped = dropped
                for row in rows:
                    writers["monitor"].add(row)
                for row in local:
                    writers["feature_drift"].add(row)
        for name, w in writers.items():
            log.info("flushed %d %s rows", w.flush(), name)


async def run(cfg: dict) -> None:
    root = Path(cfg["output_root"])
    heartbeat = Path(cfg.get("heartbeat") or root / "heartbeat")
    write_manifest(cfg, root)
    collect.init_host(cfg.get("agd_repo"))

    def mk(table, tbl_schema, at=root):
        return TableWriter(
            root=at, table=table, schema=tbl_schema,
            entity_id=cfg["entity_id"], heartbeat=heartbeat,
            max_disk_pct=cfg["max_disk_pct"], segment_seconds=cfg["segment_seconds"],
        )

    writers = {"samples": mk("samples", schema.SAMPLES_SCHEMA)}
    if cfg.get("spool_dir"):
        writers["requests"] = mk("requests", schema.REQUESTS_SCHEMA)

    monitor = None
    mon_cfg = cfg.get("monitor") or {}
    if mon_cfg.get("enabled"):
        from .monitor import Monitor   # numpy/scipy/sklearn only when enabled

        monitor = Monitor(mon_cfg, psutil.cpu_count() or 1)
        writers["monitor"] = mk("monitor", schema.MONITOR_SCHEMA)
        # Carries feature names: partner-local root, never output_root.
        writers["feature_drift"] = mk("feature_drift", schema.FEATURE_DRIFT_SCHEMA,
                                      at=Path(mon_cfg["partner_local_root"]))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    try:
        await asyncio.gather(
            sample_loop(cfg, writers["samples"], stop, monitor),
            flush_loop(cfg, writers, stop, monitor),
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
