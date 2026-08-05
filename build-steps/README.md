# build-steps — follow these in order, end with a running daemon

Nine documents. Each is one sitting, each ends with a command that either passes or
doesn't. Do not skip ahead: step N's verification is step N+1's precondition.

| # | file | you end with |
|---|---|---|
| 0 | [00-prerequisites.md](00-prerequisites.md) | toolchain checked, the three blocking questions logged |
| 1 | [01-bootstrap.md](01-bootstrap.md) | `pyproject.toml`, venv, `scraperd --help` runs |
| 2 | [02-schema.md](02-schema.md) | `schema.py` + 13 passing tests — the d=5 guard |
| 3 | [03-writer.md](03-writer.md) | `writer.py` + 8 passing tests — parquet on disk |
| 4 | [04-collect-daemon.md](04-collect-daemon.md) | `scraperd --config config.yaml` writing real rows |
| 5 | [05-deploy.md](05-deploy.md) | systemd unit + heartbeat cron. **Gate before any long run.** |
| 6 | [06-sdk-hook.md](06-sdk-hook.md) | `requests` table with real `latency_ms` |
| 7 | [07-verify-the-data.md](07-verify-the-data.md) | duckdb queries proving the tree is what you think |
| 8 | [08-week-one.md](08-week-one.md) | label count, population, disk projection |

Steps 1–5 are the working product. Step 6 needs an SDK change in the main repo. Steps 7–8
are the checks that decide whether the collected data is worth anything.

## How each step is written

- **Do** — the exact files to create, complete, no ellipses. Type or paste them whole.
- **Verify** — a command and the output it must produce.
- **Done when** — a checklist. If a box won't tick, stop; don't proceed.

Code in these docs was written and run against Python 3.12.3 / pyarrow 25.0.0 /
psutil 7.2.2 / duckdb 1.4.3 before being pasted here. The verification outputs are real,
not illustrative.

## Which doc wins

Per [CLAUDE.md](../CLAUDE.md): [must-collect.md](../must-collect.md) owns field names,
[file-layout.md](../file-layout.md) owns the build plan, [architecture.md](../architecture.md)
is target state and is **not** being built. These steps implement file-layout.md.

## Decisions these steps make that the design docs did not

Six. Each is a place the design docs were silent or physically impossible, resolved here
with the reasoning, in the same spirit as file-layout.md §4.

**D1 — Segments, not one file per day.**
file-layout.md §3 shows `2026-08-04.parquet`; §4.4 promises crash loss bounded to 60s.
Those cannot both hold: a Parquet file has a single footer written at `close()`, and until
that footer lands the file is unreadable — not truncated, *unreadable*. Keeping one writer
open all day means a crash in hour 20 loses all 20 hours, not 60 seconds.

So segments close on a timer: `segment_seconds`, default 900. Filenames become
`2026-08-04T23-55.parquet` (UTC segment start) in the same directory, so
`cold/samples/*/*.parquet` and hive partitioning are unchanged, and the date is still in
the name for cheap filtering. This is architecture.md §6.2's own naming convention at a
tighter cadence. Worst-case loss is one segment; set `segment_seconds: 60` if you want the
60s bound and can accept 1440 files/day/table.

**D2 — `NAME.parquet.part` until the footer lands.**
A crash otherwise leaves a headless file that makes `read_parquet('*/*.parquet')` fail for
the *whole table*, not just that file. Segments are written as `.part` and renamed on
close, so an orphan is inert and visible. One `os.replace`.

**D3 — `nvidia-ml-py`, not `pynvml`.**
Same `import pynvml`, but the `pynvml` distribution now emits
`FutureWarning: The pynvml package is deprecated` on every import. Over 90 days of restarts
that is noise in the journal for nothing. No code change.

**D4 — `pytz` in dev extras.**
Timestamps are `timestamp[us, tz=UTC]`. duckdb needs `pytz` to materialize a TIMESTAMPTZ
into a Python object; without it the README's `duckdb.sql("SELECT * FROM ...")` raises
`Required module 'pytz' failed to import`. Dev/read-side only, not a daemon dependency.

**D5 — Per-minute JSONL spool for request rows.**
`sdk_hook` has two halves in two different *processes* — the model process produces, the
daemon consumes — and ZeroMQ is deferred (file-layout.md §6). A spool file is the transport:
the producer appends one JSON line per request to `spool/<pid>-<YYYYmmddHHMM>.jsonl`, the
daemon only consumes files whose minute has closed. No partial lines, no offset bookkeeping,
restart-safe, no dependency.

**D6 — `get_process_metrics()` cannot sample a watched process.**
Invariant 8 says reuse it. It reads the *calling* process
(`sdk/blurredbox_agd/metrics.py:103`, via a cached `_get_process()`), so scraperd watching
some other pid cannot use it at all. Per-process fields therefore come from psutil directly;
`SystemMetricsPoller` is still reused, for the host-level GPU/freqtrade/disk fields where it
does apply. See [04-collect-daemon.md](04-collect-daemon.md) for the one field of its output
that must be discarded.

## Definition of done

```
scraperd/{__init__,schema,collect,sdk_hook,writer,daemon}.py    697 lines
tests/{test_schema,test_writer}.py                              232 lines, 21 tests
deploy/{scraperd.service,heartbeat-check.sh}
pyproject.toml, config.yaml
```

`systemctl status scraperd` active, heartbeat mtime under a minute old, and a duckdb query
over `cold/samples/*/*.parquet` returning rows for every watched pid.

**On the line budget.** CLAUDE.md sizes this at 300–400 lines and warns that well past it
means something deferred crept in. 697 is past it and nothing deferred crept in: no SQLite,
no ZeroMQ, no plugin registry, no projection module, no pydantic. The gap is roughly 200
lines of docstring and comment — `schema.py` is 165 lines of which 60 are field names, one
per line, and the DEFERRED_FIELDS block alone is 45 names. Strip comments and blank lines
and the executable body is around 420. The budget's *purpose* — catch deferred machinery
sneaking in — is satisfied; the number is not. Worth knowing before you assume you took a
wrong turn.
