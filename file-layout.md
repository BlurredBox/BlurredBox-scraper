# BlurredBox-AGD Scraper — File Layout

**Status of the other two docs:** [must-collect.md](must-collect.md) remains the source of truth for field names. [architecture.md](architecture.md) is now a *target state*, not a build plan — this layout deliberately does not implement it, and §6 lists what triggers each deferred piece. Read §5 before arguing with that.

## 0. What this is sized for

1 Hz × 90 days = 7.8M rows per process. Twenty float columns is ~1.2 GB raw and ~60–120 MB as Parquet, since smooth telemetry series compress hard. Across a handful of processes the entire deliverable is on the order of one gigabyte.

That number drives every decision below. A 6-hour SQLite buffer at this rate holds ~21,600 rows — about 4 MB. Machinery to protect 4 MB is not worth writing, testing, or debugging at 3am in week 7.

## 1. Tree

```
BlurredBox-scraper/
├── pyproject.toml              # deps + `scraperd` console script
├── config.yaml                 # host identity, what to sample, where it lands
├── README.md                   # run it, check it's alive, read the output
├── architecture.md             # target state — NOT what this builds
├── must-collect.md             # field source of truth
├── file-layout.md              # this file
│
├── scraperd/
│   ├── __init__.py             # version only
│   ├── schema.py               # field lists + CORE_V1 + the one array builder
│   ├── collect.py              # 1 Hz psutil/pynvml sampling
│   ├── sdk_hook.py             # per-request rows (the only source of latency_ms) + monitor events
│   ├── detectors.py            # monitor_v4: pure drift/anomaly/calibration/trading statistics
│   ├── monitor.py              # monitor_v4: windows, reference, outcome joins, market bars
│   ├── writer.py               # parquet row-group writer, daily rotation
│   └── daemon.py               # asyncio loop, heartbeat, disk check, SIGTERM flush
│
├── deploy/
│   ├── scraperd.service        # systemd unit, Restart=always
│   └── heartbeat-check.sh      # cron: alert if heartbeat goes stale
│
└── tests/
    ├── test_schema.py          # CORE_V1 is frozen; every must-collect field declared
    ├── test_writer.py          # rotation, crash-resume, readback
    └── test_monitor.py         # detectors fire on shifts; nothing partner-private ships
```

16 entries, 12 of them code or config. Roughly 300–400 lines.

## 2. What each file does

| file | responsibility |
|---|---|
| `pyproject.toml` | deps: `psutil`, `pynvml`, `pyarrow`, `pyyaml`. Dev: `pytest`, `duckdb`. No pydantic, no zmq, no protobuf, no scipy — nothing here needs them yet. Console script `scraperd = scraperd.daemon:main`. |
| `config.yaml` | `entity_id` (the host — see §4.2), process match patterns or explicit pids, cadence, output root, flush interval, heartbeat path, disk-full threshold, and the run-constant metadata block that goes into `manifest.json`. One file, no env layering, no settings framework. |
| `scraperd/schema.py` | `CORE_V1` — a module-level frozen tuple of the five serving fields. `PROCESS_FIELDS` and `REQUEST_FIELDS` — the names from must-collect. `core_row(d)` builds the d=5 array and raises on any missing key. **This function is the only way a model input gets constructed.** Also the `pyarrow` schemas for both tables. |
| `scraperd/collect.py` | One 1 Hz pass per watched process. Wraps `SystemMetricsPoller` / the SDK's `get_process_metrics()` when the main repo is importable; falls back to direct `psutil` + `pynvml` otherwise. Returns plain dicts. No latency — see §4.1. |
| `scraperd/sdk_hook.py` | Receives per-request metrics from `sdk/blurredbox_agd/metrics.py`, where all five d=5 fields already co-occur, and hands them to the writer tagged `source="request"`. This is the only path that produces a trainable row. |
| `scraperd/writer.py` | `pyarrow.ParquetWriter` per table per day. Buffers in memory, writes a row group every 60s, fsyncs and closes on date rollover and on SIGTERM. Worst-case crash loss is one flush interval — 60 seconds out of 90 days. Touches the heartbeat on every flush. Refuses to write and logs loudly above the disk threshold. |
| `scraperd/daemon.py` | asyncio loop: schedule `collect` at cadence, drain `sdk_hook`, drive `writer` flushes, write `manifest.json` at startup, handle SIGTERM → flush → close. Everything else is a function call from here. |
| `scraperd/detectors.py` | monitor_v4 statistics as pure functions: per-feature drift, MMD, Mahalanobis, half-space trees, score shape, calibration, trading. Return None, never 0.0, when a number can't be computed honestly. |
| `scraperd/monitor.py` | monitor_v4 state: per-window reservoirs, the frozen reference (persisted to `state_dir`), prediction→outcome joins by `request_id`, market bars. Runs in an executor thread from the flush loop. Needs the `[monitor]` extra (numpy, scipy, scikit-learn) — the first deps beyond §2's list, and only when `monitor.enabled`. |
| `deploy/scraperd.service` | systemd, `Restart=always`, config path from env. |
| `deploy/heartbeat-check.sh` | Cron entry: if the heartbeat file's mtime is older than N minutes, alert. Ten lines. **This is the single highest-value file in the repo** — see §5.1. |
| `tests/test_schema.py` | Asserts `CORE_V1` still has exactly those five names in that order, that `core_row` raises on a missing key, and that every field name in must-collect.md is declared somewhere in `schema.py`. The last one turns "we forgot to collect a field" into a CI failure. |
| `tests/test_writer.py` | Daily rotation, resume onto an existing day's file after a kill, and a `duckdb` readback that the written rows match what went in. |

## 3. On-disk output

```
/var/lib/blurredbox-agd/cold/
├── manifest.json                              # run-constant metadata, written once
├── heartbeat                                  # mtime touched every flush
├── samples/
│   └── entity_id=vm1/2026-08-04.parquet
└── requests/
    └── entity_id=vm1/2026-08-04.parquet
```

**Two tables, not seven.**

`samples` — the 1 Hz poll: `process_id`, `cpu_usage`, `memory_usage_mb`, `memory_pct`, `gpu_usage`, `gpu_vram_mb`, `gpu_temperature`, `num_threads`, `num_fds`, `io_read_bytes`, `io_write_bytes`, `num_connections`, `ctx_switches`, `disk_usage_pct`, `freqtrade_cpu`, `freqtrade_memory_mb`, `timestamp`. No `latency_ms`.

`requests` — one row per inference, from the SDK: the complete d=5 (`cpu_usage`, `memory_usage_mb`, `gpu_usage`, `gpu_vram_mb`, `latency_ms`) captured in a single snapshot, plus `request_id`, `process_id`, `input_size`, `output_size`, `status`, `prediction`, `timestamp`.

`monitor` (optional) — one row per window of the monitor_v4 metrics; see must-collect.md. Its sibling `feature_drift` table carries feature names and is written under `monitor.partner_local_root`, never here.

`manifest.json` — `entity_id`, `model_id`, `model_version_id`, `slug`, `tenant_id`, `operator_id`, `environment`, `artifact_id`, `version`, `schema_version`, run start time, host facts. These are constant for the whole run; they belong in one 400-byte file, not repeated across 7.8M rows.

Read it with `duckdb.sql("SELECT * FROM 'cold/requests/*/*.parquet'")`. Hive partitioning gives you `entity_id` as a column for free.

## 4. The decisions that actually matter

### 4.1 `latency_ms` never comes from a poll row

`latency_ms` is request wall-clock, attached at inference time in `sdk/blurredbox_agd/metrics.py`. The 1 Hz poller has no latency to emit.

Joining a request's latency to the nearest poll sample manufactures a training row that never existed at serving time — the d=32 failure mode wearing a different hat. So the two streams stay two tables, `core_row()` reads from `requests` only, and `samples` exists for background series and anomaly work where latency is irrelevant.

Yes, this means most poll rows are never d=5 training rows. That's correct: a row without a real latency isn't a d=5 row.

**Confirm in week 1:** if `metrics.py` doesn't already emit all five fields in one snapshot, that's a hook to add there, not a workaround here.

### 4.2 `entity_id` is the host; process identity is `process_id`

must-collect defines `entity_id` as "host/VM/process" and architecture §6.3 partitions as `entity_id=vm1`. Taking the latter as the resolution: one `entity_id` per host, set in config, and `process_id` as a column. Keeps partition cardinality flat and keeps per-entity splits leak-free at the host boundary, which is the boundary that matters for leakage.

### 4.3 The d=5 guard is a tuple and a test, not a layer

The d=32 mistake happened because raw scraper output reached a model. What prevents that is: one frozen tuple, one function that builds the array from it and raises on a missing key, one test asserting the tuple hasn't changed, and nothing else in the codebase that constructs a model input.

A separate projection module with a `VIEWS` registry catches the same bug. It is not worth the extra module until a third consumer of this data exists.

### 4.4 Parquet directly, no SQLite tier

architecture §6.2 argues Parquet is built for batch writes, not streaming appends, and that writing it directly moves the buffering problem into unprotected memory. True in general; the numbers say otherwise here. `ParquetWriter` appends row groups incrementally, and a 60s flush bounds crash loss to 60 seconds out of 7.8M rows.

The SQLite tier buys crash safety on a 4 MB working set, plus queryable-while-live — which `duckdb` over yesterday's file already gives you, since today's partial file is readable after any flush.

### 4.5 No pydantic

Two tables with fixed columns. `pyarrow` schemas already reject wrong types at write time, which is the same validation with fewer dependencies.

## 5. What the design was missing entirely

### 5.1 A liveness signal

For an unattended 90-day run, the realistic failure is not disk corruption or schema drift. It is that the daemon died in week 3 and nobody noticed until week 12. Three design documents specified a disk-full guard and no heartbeat.

`writer.py` touches a heartbeat file on every flush; `deploy/heartbeat-check.sh` runs from cron and alerts when it goes stale. Ten lines, and it prevents the only failure that loses the whole run.

### 5.2 Nobody has counted the labels

The training label comes from incident records — 1 at the failure step, else 0. If the infrastructure is even moderately healthy, 90 days may produce a handful of genuine failure events. 7.8M rows and twelve positives is not a training set; it is a class-imbalance problem that clean Parquet does not fix.

**Do this before starting the run:** count failure events in the trailing 90 days of existing incident records. If the answer is single digits, the run needs a different design — deliberate fault injection, a broader definition of "failure," or a different target variable entirely. That question decides whether the collection is worth doing at all, and no version of these documents has asked it.

### 5.3 Nobody has sized the population

How many hosts, how many processes each? Two VMs and six processes is a small-data problem. Write the number in `README.md` and multiply it out against actual free disk in week 1, not month 2.

## 6. Deferred, with triggers

Each of these has a home in architecture.md and a reason not to exist yet. The trigger is what makes it exist.

| deferred | trigger |
|---|---|
| SQLite write buffer | you actually lose data you needed, or the flush interval has to drop below ~10s |
| Event collectors (`model_perf`, `fairness`, `policy_signals`, `action_taker_io`) | one at a time, when that bus emits real traffic. Writing four collectors against streams with no producers is the most expensive code there is, because nothing tells you it's wrong |
| `drift_stats` collector | superseded for model inputs by monitor_v4 (`psi_aggregate` etc.), which must run on the partner host because feature values never leave it. Welford stats over the telemetry series remain deferred |
| `sink.py` / ZeroMQ | when a live consumer exists. The stated 3-month goal is the Parquet tree itself |
| `native/` C probe | when profiling shows psutil perturbs the measurement at your cadence. At 1 Hz it won't; architecture §2 concedes this |
| Plugin registry / entry points | when someone outside the team writes a collector |
| `signing.py` | when scraperd produces a signature rather than recording one |
| Projection module | when a third consumer of the data exists |

## 7. Build order

1. `schema.py` + `tests/test_schema.py` — pin `CORE_V1` and the field-coverage gate first. Half a day.
2. `writer.py` + `tests/test_writer.py` — rotation and crash-resume.
3. `collect.py` + `daemon.py` + `config.yaml` — smallest thing that writes real rows.
4. `deploy/` — systemd unit and the heartbeat cron. **Do not start the 90-day run before this exists.**
5. `sdk_hook.py` — needs the `metrics.py` confirmation from §4.1.

Then, in week 1 of the run: the label count from §5.2, the disk projection from §5.3, and one `duckdb` query against the first day's files to confirm the data is what you think it is.