# 004 — Launch blockers (not code defects)

**Severity: blocking.** None of these are bugs. All of them decide whether the 90-day run
produces a usable dataset.
**Fixes:** [04](../fix-steps/04-narrow-the-watch-patterns.md),
[05](../fix-steps/05-wire-sdk-hook-into-serving.md),
[06](../fix-steps/06-answer-the-two-blocking-questions.md),
[07](../fix-steps/07-install-on-the-target-host.md)

## Status — 2026-08-06

| | state | where |
|---|---|---|
| B1 | **fixed** | `patterns: ["freqtrade trade"]` — 36 → 12 processes, verified by a daemon run |
| B2 | **fixed** | emit wired into `RequestTrackingMiddleware` (AGD); `record()` made stdlib-only, which is what had been silently breaking it |
| B3 | **answered** | [ANSWERS.md](../ANSWERS.md) — 0 organic failures, 1,149 injected, injection already runs |
| B4 | **fixed** | `gpu_*` in `skip_fields`; d=5 is effectively d=3 on a CPU-only host — the design question stands |
| B5 | **fixed** | poller import was `ModuleNotFoundError: httpx`, not a missing `agd_repo`; see below |
| B6 | **fixed** | `num_connections` in `skip_fields` |
| B7 | **open** | `manifest.*` still `""` — run-constant, must be filled before the run starts |
| — | **open** | nothing installed on a target host yet ([07](../fix-steps/07-install-on-the-target-host.md)) |

Two things turned out to be different from what is written below:

- **B2 was not "nothing calls `record()`".** `sdk/blurredbox_agd/tracking.py` already had the call,
  in `_RequestContext.__exit__`, gated on `SCRAPERD_SPOOL`. Nothing called `track_request()`, so it
  was dead code — and underneath that, `sdk_hook.record()` imported `.schema`, which imports
  pyarrow, which the serving image does not have. Every call site swallows `ImportError`, so the
  failure was invisible. The import is now function-local to `drain()`; the producer half is
  stdlib-only, guarded by `tests/test_sdk_hook.py::test_record_needs_no_pyarrow`.
- **B5 was not just a commented-out `agd_repo`.** Uncommenting it changed nothing: the import goes
  through `src/connectors/__init__.py`, which pulls `FreqtradeConnector` → `httpx`. `init_host()`
  now stubs the parent packages and loads the poller module directly. The poller then reports
  "Multiple Freqtrade candidates detected (37)" and resolves a 0.32 MB `entrypoint.chaos-entity.sh`
  wrapper, so `freqtrade_cpu` / `freqtrade_memory_mb` are in `skip_fields` too — the per-process
  numbers are already in `samples`.

Measurements below are from **this dev box**, 2026-08-06. The daemon runs on whichever host the
watched processes live on (psutil reads `/proc`; it cannot scrape a remote host), so if that is
a different machine, B1/B4/B5/B6 must be re-measured there. B2 and B3 hold everywhere.

---

## B1 — `watch.patterns` over-matches by 18×

`patterns: ["freqtrade", "uvicorn"]` in [config.yaml:10](../config.yaml#L10) is a substring match
against the full cmdline. On this host it selected **36 processes**:

```console
$ pgrep -af freqtrade | head -3
2489 /bin/bash /freqtrade/entrypoint.chaos-entity.sh      <- shell wrapper, 0.3 MB RSS
2503 /bin/bash /freqtrade/entrypoint.chaos-entity.sh
2819 /usr/local/bin/python3.14 .../freqtrade trade --config ft_config.json ...   <- the real one
```

34 of the 36 are `entrypoint.chaos-entity.sh` bash wrappers sitting at 0.3 MB RSS and 0.0% CPU.
Two are the actual freqtrade python processes. Zero `uvicorn` processes are running at all.

Consequence, at 1 Hz over 90 days:

| | processes | rows | parquet |
|---|---|---|---|
| as configured | 36 | 280 M | ~1.5–13 GB |
| real targets only | 2 | 15.5 M | ~0.1–0.7 GB |

The size range is wide because the only measurement so far used 5-second segments (46 B/row
compressed); 900-second segments compress far better. Measure a real day before trusting either
end. The design target in [CLAUDE.md](../CLAUDE.md) is ~1 GB, which the 36-process configuration
misses by up to 13×.

## B2 — Nothing calls `sdk_hook.record()`

[scraperd/sdk_hook.py](../scraperd/sdk_hook.py) is complete and verified working — spool →
`drain()` → parquet → `core_row()` all execute correctly when something calls `record()`.
Nothing does.

No caller means no `requests` table, which means no `latency_ms`, which means **no d=5 row is
ever produced by the run**. 90 days of `samples` with no requests table is 280 M rows of
resource counters and zero training rows.

This is the single blocker that decides whether the run has a point. Open question 3 in
[file-layout.md](../file-layout.md) §5 — whether `sdk/blurredbox_agd/metrics.py` emits all five
d=5 fields in one snapshot — is still unanswered and gates it.

## B3 — The label-count question was never answered

`build-steps/ANSWERS.md` held the two blocking pre-run questions and was deleted in `c0c203f`
("clean") with both still blank. They did not stop being blocking.

The training label is joined from incident records: 1 at the failure step, else 0. If the
infrastructure is even moderately healthy, 90 days may yield single-digit positives. 280 M rows
and twelve positives is a class-imbalance problem, not a training set — and it is only
discoverable **before** the run, by counting.

## B4 — No GPU on this host

```console
$ nvidia-smi -L
/bin/bash: line 1: nvidia-smi: command not found
```

`gpu_usage`, `gpu_vram_mb`, `gpu_temperature` are constant 0.0 (confirmed: 504/504 sample rows).
Two of the five `CORE_V1` fields are dead, so d=5 is effectively d=3 for anything collected here.

Not a defect — the fallback behaves exactly as designed. It is a design question: if the target
host has no GPU, is d=5 the right contract for this fleet, or is it d=3 plus two columns that
will teach a model nothing?

## B5 — `agd_repo` is commented out

[config.yaml:31](../config.yaml#L31). Without it `SystemMetricsPoller` never loads, so
`freqtrade_cpu` and `freqtrade_memory_mb` are 0.0 for the whole run (confirmed: 504/504 rows).
Invariant 8 says reuse the poller where the main repo is importable — right now it isn't
pointed at one.

## B6 — `num_connections` is structurally 0 for containerised targets

0 on all 504 rows, including the real freqtrade processes, which certainly hold sockets. The
targets run in a container with its own network namespace; psutil matches socket inodes against
the **host's** `/proc/net/*`, finds nothing, returns an empty list.

Not fixable from here, and it costs a `/proc/net` scan per process per second to keep producing
zeros. Add it to `skip_fields` on any host where the targets are containerised.

Also empty: `manifest.model_id`, `model_version_id`, `slug`, `tenant_id`, `operator_id`,
`artifact_id` — all `""` in [config.yaml](../config.yaml). They are run-constant metadata, so
filling them in after the run starts means the manifest describes the run incorrectly.
