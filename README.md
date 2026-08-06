# BlurredBox-AGD Scraper

A general-purpose telemetry scraper for model-serving processes. Point it at any process on
the host — whatever model is running there — and it samples per-process CPU, memory, GPU, and
I/O counters at 1 Hz, captures per-inference metrics where the process reports them, and
writes both to Parquet on local disk.

## Purpose

Produce a persistent, portable record of how a model process behaves under load. This is the collection
point: long unattended runs (sized here for 90 days, ~1 GB of Parquet) covering the full
field inventory in [must-collect.md](must-collect.md) — resource counters, request metrics,
and the identifiers needed to slice them per host and per process.

Output is deliberately consumer-agnostic. `duckdb` or `polars` reads the tree directly, so
downstream users — monitoring, drift analysis, capacity work, anyone else granted access —
each take the columns they need without the scraper knowing or caring who they are.

## What it collects

Two tables, Hive-partitioned by `entity_id` (the host), one Parquet file per day:

```
/var/lib/blurredbox-agd/cold/
├── manifest.json          # run-constant metadata (model_id, tenant_id, …)
├── heartbeat              # mtime touched on every flush
├── samples/entity_id=vm1/2026-08-04.parquet    # 1 Hz poll
└── requests/entity_id=vm1/2026-08-04.parquet   # per-inference
```

- **`samples`** — the 1 Hz poll: cpu, memory, GPU usage/VRAM/temp, threads, fds, io bytes,
  connections, ctx switches, disk.
- **`requests`** — one row per inference: `cpu_usage`, `memory_usage_mb`, `gpu_usage`,
  `gpu_vram_mb`, `latency_ms` captured in a single snapshot, plus request id, input/output
  size, status, prediction.
- **`manifest.json`** — the fields constant for the whole run (`entity_id`, `model_id`,
  `tenant_id`, `environment`, `schema_version`, host facts) in one 400-byte file instead of
  repeated across millions of rows.

`latency_ms` appears only on `requests` — it's request wall-clock, and the poller has no
latency to emit. The two streams stay two tables so nothing joins a request's latency onto a
nearby poll sample and produces a row that never existed as measured.

Read it:

```python
duckdb.sql("SELECT * FROM 'cold/requests/*/*.parquet'")
```

## How it runs

A single `scraperd` process per host under systemd (`Restart=always`), configured by one
`config.yaml`: host identity, which processes to watch, cadence, output root. Where the
existing `SystemMetricsPoller` / `get_process_metrics()` are importable it wraps them rather
than forking; otherwise it falls back to `psutil` + `pynvml` directly, so it runs against a
process regardless of what SDK that process uses.

`skip_fields:` switches off fields not worth their syscall on a given host — the `gpu_*` three
on a CPU-only box, `num_connections` for a process in another network namespace. The parquet
schema is unaffected: a skipped field is never read and reads back null, so every host's files
still glob together.

Buffers in memory with a 60s row-group flush, which bounds memory. Crash loss is bounded by the
segment interval instead — a parquet file is unreadable until its footer is written at segment
close: 900s for `samples`, 60s for `requests` (`request_segment_seconds`), since request rows are
the only ones carrying `latency_ms`. Every flush touches a heartbeat file, and a cron check alerts when it goes
stale — for an unattended run the realistic failure isn't corruption or schema drift, it's the
daemon dying in week 3 and nobody noticing until week 12.

## Docs

- [must-collect.md](must-collect.md) — field inventory, source of truth for names
- [file-layout.md](file-layout.md) — what gets built, what's deferred, and build order
- [architecture.md](architecture.md) — target state (C probe, ZeroMQ, plugin registry), *not* the build plan

## Open before the run starts

See [file-layout.md](file-layout.md) §5:

1. **Population**: how many hosts, how many processes each.
2. **Disk**: one day of real Parquet × 90, checked against actual free space.
