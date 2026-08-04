# BlurredBox-AGD Scraper — Architecture Design

## 0. Design constraints

- Must collect **every field** in the inventory doc (core telemetry, extended resource, drift/stats, model perf, fairness, policy/decision, action_taker I/O, metadata) — and be extensible for fields that don't exist yet.
- Must **not** be the thing that decides what the GRU/IsolationForest sees. The scraper produces a superset; a thin adapter downstream (or a filter stage inside the scraper) projects out the fixed d=5 view for the serving model. This is the fix for the d=32 mistake: training-time features and prod-time features diverge because someone let the raw scraper output leak into the training set. The scraper owns collection; a versioned schema contract owns what each consumer is allowed to see.
- Reuse existing ingestion points rather than duplicate them: `SystemMetricsPoller` and the SDK's `get_process_metrics()`. The scraper wraps/extends these, it doesn't fork them.
- Python for orchestration, plugin ecosystem, and anything where a mature library already does the job well (psutil, pynvml, asyncpg, pyzmq). C only where Python's per-sample overhead would itself distort the metric it's measuring, or where you need syscall/kernel-level access Python can't get cleanly (high-frequency `/proc` sampling, fd counting, io accounting, ctx switches at low latency).

## 1. High-level architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              scraperd (Python)               │
                    │  asyncio event loop, one process per host    │
                    │                                               │
  ┌───────────┐     │  ┌──────────────┐   ┌──────────────────┐    │
  │ C sampler  │◄───►│  │  Collector    │──►│  Schema Registry  │    │
  │ (libagd_   │ shm │  │  Registry     │   │  (pydantic models) │    │
  │  probe.so) │     │  │  (plugins)    │   └────────┬──────────┘    │
  └───────────┘     │  └──────────────┘            │               │
                    │                                ▼               │
                    │                     ┌────────────────────┐    │
                    │                     │  Projection Layer   │    │
                    │                     │  (d=5 view, drift    │    │
                    │                     │   view, fairness      │    │
                    │                     │   view, raw view)     │    │
                    │                     └────────┬───────────┘    │
                    │                                ▼               │
                    │                     ┌────────────────────┐    │
                    │                     │ Local ring buffer /  │    │
                    │                     │ WAL (disk-backed)    │    │
                    │                     └────────┬───────────┘    │
                    └──────────────────────────────┼─────────────────┘
                                                     ▼
                                    ZeroMQ CURVE-secured push
                                    (reuse existing IPC pattern)
                                                     ▼
                                    Ingest API → Postgres / Redis
                                    → GRU / IsolationForest / drift /
                                      fairness / policy engine
```

Everything left of the ZeroMQ boundary is the scraper. It's one daemon (`scraperd`), not N separate scripts, so there's a single process lifecycle, a single config, and a single place schema drift gets caught.

## 2. Why C shows up, specifically

Don't reach for C by default — only these three spots earn it:

1. **High-frequency per-process sampling** (`num_threads`, `num_fds`, `ctx_switches`, `io_read_bytes`/`io_write_bytes`, `num_connections`) at sub-second cadence. `psutil` is fine at 1–5s intervals; below that, Python's own overhead (GC, GIL, object churn) starts perturbing the CPU/memory numbers you're trying to measure. A small C sampler (`libagd_probe.so`) reads `/proc/[pid]/stat`, `/proc/[pid]/io`, `/proc/[pid]/fd`, `/proc/[pid]/status` directly, writes fixed-width structs into a POSIX shared-memory ring buffer, and Python just reads that memory — no subprocess spawn per sample, no syscall overhead in the hot path.
2. **GPU low-level counters** where `pynvml` (NVML Python bindings) doesn't expose what's needed (rare, but `gpu_temperature`/`gpu_vram_mb` under sustained sampling can benefit from calling NVML's C API directly via a thin wrapper instead of the Python binding's per-call overhead). In practice `pynvml` covers 95% of this — only add the C path if profiling shows it's needed.
3. **ECDSA signing for `action_taker` / `enclave_signature`** — reuse whatever crypto BlurredBox's C daemon already does rather than re-implementing signing in Python (`cryptography` package would work fine here too; C only wins if you want bit-identical signature code shared with the daemon's proto, avoiding a second implementation to keep in sync).

Everything else (drift stats, fairness deltas, model perf fields, metadata, policy engine outputs, ZeroMQ transport) is Python — there's no per-sample overhead problem, and the libraries are strictly better than anything hand-rolled in C.

## 3. Collector plugin registry

Each field group in the inventory becomes a **collector plugin** implementing a common interface:

```python
class Collector(Protocol):
    name: str
    cadence_s: float                 # how often it runs
    schema: type[BaseModel]          # pydantic model for its output rows

    async def collect(self, ctx: CollectorContext) -> list[BaseModel]: ...
```

Registry entries (one per section of the inventory doc):

| collector | backing lib | cadence | feeds |
|---|---|---|---|
| `core_telemetry` | psutil + pynvml + C shm ring | 1s | GRU/IsolationForest (d=5) |
| `extended_resource` | psutil, C shm ring | 1s | extended metrics store |
| `drift_stats` | numpy/scipy (Welford, PSI) | 5s (rolling window) | drift_forecaster, policy triggers |
| `model_perf` | pulled from SDK client / DB writes on inference | event-driven | monitoring API |
| `fairness` | pulled at labeling time (`outcome_label`) | event-driven | fairness service |
| `policy_signals` | computed from policy engine outputs, not sampled | event-driven | policy/adaptive triggers |
| `action_taker_io` | proto-based, subscribes to the daemon's IPC | event-driven | action_taker proto |
| `metadata` | injected into every row by the framework, not a collector | always | all consumers |

Adding a new field later = adding a collector (or extending an existing pydantic model) and registering it — no change to the transport, projection, or storage layers. This is the actual answer to "collect all these and more in the future": the extensibility point is the plugin registry + pydantic schema, not the wire format.

## 4. Sampling & scheduling

- Single `asyncio` event loop in `scraperd`. Each collector gets its own `asyncio.create_task` loop at its declared cadence — no shared blocking calls.
- Event-driven collectors (`model_perf`, `fairness`, `action_taker_io`) subscribe to whatever internal bus already emits them (reuse existing SDK client hooks / ZeroMQ subscribe socket) rather than polling.
- Sampled collectors (`core_telemetry`, `extended_resource`, `drift_stats`) run on a `PeriodicTask` wrapper with jitter to avoid thundering-herd sampling across many processes on one host.
- The C sampler runs as its own lightweight loop (or is triggered by a signal/timerfd) independent of the Python GIL, writing into shared memory; Python's `core_telemetry`/`extended_resource` collectors just read the latest ring buffer slot each tick — this decouples sampling frequency from Python scheduling jitter.

## 5. Schema contract & projection layer

- All rows are `pydantic` models, versioned (`schema_version` field). This is what stops a d=32-style mismatch: the GRU/IsolationForest consumer requests the `d=5` **view name**, not "whatever the scraper happens to emit," and the projection layer is the only place allowed to construct that view.
- Projection layer = a small set of named views (`core_v1` → the fixed 5 fields, `drift_v1`, `fairness_v1`, `raw_v1` = everything). Each view has its own pydantic model with an explicit field list, so it fails loudly (validation error) if a required field is missing rather than silently drifting.
- `raw_payload` (JSONB) stays as an escape hatch for the model_perf table but is never the input to a trained model — same principle.

## 6. Local storage: SQLite write buffer → Parquet archive (finalized)

Decision for the 3-month unattended run (data stays on the scraped host until manually moved out, no ingest API dependency during collection):

### 6.1 Tier 1 — SQLite write buffer

- One SQLite DB, **WAL journal mode**, one writer connection per process.
- One table per collector group (`core_telemetry`, `extended_resource`, `drift_stats`, `model_perf`, `fairness`, `policy_signals`, `action_taker_io`) — not one wide table. Matches the projection views in §5.
- Batched transactional inserts every 1–5s (never per-row commits at 1Hz over 90 days).
- Index `(entity_id, timestamp)` per table.
- SQLite only ever holds a rolling **~6–12 hours** of data (last export window + one safety window kept even after export). It is not the long-term store — it is a durable, crash-safe, queryable-while-live buffer between the collectors and the archive.

### 6.2 Tier 2 — Parquet archive, exported every 6 hours

- A scheduled job runs every 6h, per collector table:
  1. Query SQLite for rows with `timestamp` in `[window_start, window_end)`.
  2. Write to a **new** Parquet file for that window (`duckdb.sql("COPY (...) TO 'file.parquet' (FORMAT PARQUET)")` — 5 lines, no new infra).
  3. Verify (row count / checksum match between query and written file).
  4. Only then `DELETE` those rows from SQLite; periodic `VACUUM` (SQLite doesn't reclaim space from deletes automatically).
- **One Parquet file per collector table per 6h window per entity** — not one file per table for the whole run, not one giant file. ~4 files/day/table over 90 days ≈ 360 files/table total — trivial for the filesystem and for `duckdb`/`polars` to glob as a single logical table with predicate pushdown. Only worth compacting into daily files later if the export cadence is ever tightened well below 6h.
- Idempotent/resumable: windows are named by their boundaries (`2026-08-04T00-06.parquet`), so a restarted job can tell which windows are already exported (file exists + row count matches) and skip them rather than double-export or leave a gap.
- Rationale for the split (why not write Parquet directly, why not one huge file): Parquet is columnar/immutable, built for batch writes from a complete table, not streaming appends — writing it directly would just move the buffering problem into unprotected application memory and lose SQLite's crash safety. One huge file makes the entire 3-month dataset a single point of failure on any write error or disk corruption; many small per-window files bound the blast radius to one window and keep the export job cheap and resumable.

### 6.3 On-disk layout

```
/var/lib/blurredbox-agd/cold/
├── core_telemetry/
│   └── entity_id=vm1/
│       ├── 2026-08-04T00-06.parquet
│       ├── 2026-08-04T06-12.parquet
│       └── ...
├── extended_resource/   └── entity_id=vm1/...
├── drift_stats/         └── entity_id=vm1/...
├── model_perf/          └── entity_id=vm1/...
├── fairness/            └── entity_id=vm1/...
├── policy_signals/      └── entity_id=vm1/...
└── action_taker_io/     └── entity_id=vm1/...
```

- Partition by collector table first, then entity, then window file — matches how the data will actually be queried later, and lets the training side glob a subtree as one table with zero merge code.
- Put this tree on a separate disk/partition from the SQLite WAL if at all possible — over 3 months this directory *is* the deliverable, and it shouldn't compete for I/O with the live write buffer or share a failure domain with it.
- No automatic off-box transfer in this design (data sits on the scraped host by design). Before committing, size it: per-window Parquet size × 4/day × ~90 days, checked against actual disk on the host, to catch a disk-capacity problem in week 1 instead of month 2.
- Safety guard for a fully unattended run: the export job should refuse to prune SQLite rows if the target partition's disk is above a threshold (e.g. 90% full) — SQLite then keeps growing and is a visible, alertable signal, instead of collection silently degrading.

### 6.4 Delivery to the rest of the stack (separate from local storage)

- Independent of the local SQLite→Parquet archive above: any collector output that also needs to reach the live ingest API (model_perf, fairness, policy/action_taker signals feeding the running governance stack) still goes over the existing ZeroMQ CURVE-secured IPC — `scraperd` is just another CURVE client pushing to the same socket the SDK's `metrics.py` uses, no new transport.
- Backpressure there is separate from the archive: if the ZeroMQ push queue backs up, retry with a bounded local retry queue distinct from the SQLite archive tier, replayed oldest-first on reconnect.

## 7. Process layout / deployment

- One `scraperd` per host (VM), running as a systemd service, same pattern as the rest of the Docker Compose stack (or as its own container with `--pid=host` / `--privileged` access to `/proc` if containerized — this is the one place containerization needs real thought, since `/proc/[pid]` visibility across container boundaries is the usual gotcha).
- C sampler ships as a `.so` loaded via `ctypes`/`cffi`, built once per target (Rocky Linux 10, matching the rest of the stack), not a separate daemon — keeps the deployment surface to "one systemd unit."
- Config: single YAML per host declaring which collectors are enabled and their cadence overrides — lets you run a stripped-down collector set on the training VM (no fairness/policy collectors needed there) vs. the full set on the hosting VM.

## 8. What this buys vs. what it costs

**Buys:** one extensible collection point instead of N ad hoc scripts, a schema contract that makes the d=32 mistake structurally hard to repeat, reuse of existing ZeroMQ/SDK infrastructure instead of a second transport, and a C hot path only where Python's overhead would actually corrupt the measurement.

**Costs:** the C sampler is now a piece of native code you maintain and rebuild per target OS/kernel — worth it only for the handful of fields where sub-second, low-overhead sampling genuinely matters (per-process resource counters). Everything else earns its place in Python precisely because that cost isn't worth paying there.