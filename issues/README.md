# Issues

Found during a pre-launch verification pass on 2026-08-06, against `c0c203f` on branch `fix`.
Every issue here was **reproduced by running the code**, not read off the source. Each file
records the repro command and its actual output, so none of this has to be re-derived.

Fixes live in [../fix-steps/](../fix-steps/), numbered in the order they should be done.

| # | issue | severity | fix step |
|---|---|---|---|
| [001](001-request-rows-lost-on-hard-kill.md) | Hard kill destroys up to 15 min of `requests` rows — deleted from the spool, never footered in parquet | **high** | [01](../fix-steps/01-bound-the-request-loss-window.md) |
| [002](002-heartbeat-green-while-writes-refused.md) | Heartbeat stays fresh while the disk guard is dropping every row — the cron alarm never fires | **high** | [02](../fix-steps/02-heartbeat-must-mean-writing.md) |
| [003](003-empty-watch-key-crashes-startup.md) | `watch:` with no value gives an `AttributeError` traceback instead of a config error | low | [03](../fix-steps/03-config-error-not-traceback.md) |
| [004](004-launch-blockers.md) | Six non-code blockers between here and a valid 90-day run | **blocking** | [04](../fix-steps/04-narrow-the-watch-patterns.md)–[07](../fix-steps/07-install-on-the-target-host.md) |

## What is already verified working

Not assumed — executed on this host:

- `samples`: 36 processes at 1 Hz, 504 rows, real `cpu_usage` / `memory_usage_mb` / `io_*` / `num_fds`
- `requests`: `record()` → spool → `drain()` → parquet → `core_row()` → `[0.0, 42.8, 0.0, 0.0, 12.5]`
- SIGTERM → flush → footer → rename; no orphaned `.part` left behind
- daily + `segment_seconds` rotation, restart collision naming (`...-1.parquet`), disk guard, heartbeat
- `check.py` runs green end to end; 27/27 tests pass

The daemon works. Issues 001 and 002 are durability and monitoring gaps that only show up in
the failure cases a 90-day unattended run will eventually hit. 004 is everything that is not
code.
