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
    # `or {}`, not setdefault, throughout: a YAML key written with no value
    # (`watch:`) is present-and-None, so setdefault leaves the None in place.
    cfg = yaml.safe_load(Path(path).read_text()) or {}
    cfg.setdefault("cadence_seconds", 1)
    cfg.setdefault("flush_seconds", 60)
    cfg.setdefault("segment_seconds", 900)
    cfg.setdefault("request_segment_seconds", 60)
    cfg.setdefault("max_disk_pct", 90.0)
    cfg["watch"] = cfg.get("watch") or {}
    cfg["watch"].setdefault("patterns", [])
    cfg["watch"].setdefault("pids", [])
    cfg["watch"].setdefault("refresh_seconds", 60)
    if not cfg.get("entity_id"):
        raise SystemExit("config: entity_id is required (it is the host — invariant 5)")
    cfg["skip_fields"] = frozenset(cfg.get("skip_fields") or [])
    unknown = cfg["skip_fields"] - set(schema.SAMPLE_FIELDS)
    if unknown:
        raise SystemExit(f"config skip_fields: not a samples field: {sorted(unknown)}")
    # What makes a poll row a poll row. Not a CORE_V1 check: samples never feeds
    # core_row (invariant 3), so gpu_* here may legitimately be switched off on a
    # CPU-only host — the d=5 gpu fields come from sdk_hook, not from this table.
    locked = cfg["skip_fields"] & {
        "process_id", "timestamp", "cpu_usage", "memory_usage_mb",
    }
    if locked:
        raise SystemExit(f"config skip_fields: cannot skip {sorted(locked)}")
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
        "version": (cfg.get("manifest") or {}).get("version") or __version__,
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

        host = collect.host_metrics(cfg["skip_fields"])
        for pid in list(procs):
            try:
                writer.add(collect.sample(procs[pid], host, cfg["skip_fields"]))
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

    def mk(table, tbl_schema, segment_seconds):
        return TableWriter(
            root=root, table=table, schema=tbl_schema,
            entity_id=cfg["entity_id"], heartbeat=heartbeat,
            max_disk_pct=cfg["max_disk_pct"], segment_seconds=segment_seconds,
        )

    writers = {"samples": mk("samples", schema.SAMPLES_SCHEMA, cfg["segment_seconds"])}
    if cfg.get("spool_dir"):
        # drain() unlinks the spool file, so a request row lives only in RAM + an
        # unfootered .part until the segment closes. Short segments bound that.
        writers["requests"] = mk("requests", schema.REQUESTS_SCHEMA,
                                 cfg["request_segment_seconds"])

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
