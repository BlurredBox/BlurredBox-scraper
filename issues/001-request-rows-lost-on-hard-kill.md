# 001 — Hard kill destroys up to 15 minutes of `requests` rows

**Severity: high.** Affects the scarce table, not the cheap one.
**Fix:** [../fix-steps/01-bound-the-request-loss-window.md](../fix-steps/01-bound-the-request-loss-window.md)

## What happens

`drain()` deletes each spool file as soon as it reads it — [scraperd/sdk_hook.py:99](../scraperd/sdk_hook.py#L99).
The rows it returns go into the writer's in-memory buffer, then into an **open** parquet segment.
A parquet file is unreadable until its footer is written, and the footer is only written on
rotate or `close()` — i.e. after `segment_seconds`, which is **900** in
[config.yaml:26](../config.yaml#L26).

So between drain and the next segment close there is a window where the rows exist in exactly
one place: the RAM of a process, plus a `.part` file no reader will ever open.

```
record()  ->  spool/*.jsonl        durable on disk
drain()   ->  spool file UNLINKED  <-- only copy destroyed here
          ->  writer._rows         RAM
          ->  NAME.parquet.part    no footer; reader glob skips it
close()   ->  NAME.parquet         durable again      <-- up to 900s later
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                  everything in this gap dies on SIGKILL
```

## Reproduction

Five request rows spooled, daemon started, confirmed drained, then `kill -9`:

```console
$ .venv/bin/python -m scraperd.daemon --config smoke4.yaml & PID=$!
$ sleep 6; grep -c "flushed 5 requests" k.log
1
$ kill -9 $PID

$ ls smoke4/spool
                                     # empty — drain() deleted it
$ find smoke4/cold/requests -type f
smoke4/cold/requests/entity_id=vm1/2026-08-06T12-16.parquet.part
```

Spool empty, on-disk file is `.part`. The five rows are gone from both sides and are not
recoverable. `check.py` will not even notice — its stale-`.part` assert only fires after 24h.

## Why it matters here specifically

- SIGTERM is **safe** — systemd's normal stop path flushes, footers and renames. Verified.
- This only bites on `kill -9`, OOM-kill, power loss, or a hung stop hitting `TimeoutStopSec=90`.
- Over 90 unattended days, at least one of those happens. This host currently sits at
  **1 GB free of 14 GB RAM**, so the OOM killer is a live risk, not a hypothetical.
- `samples` loses 15 minutes too, but `samples` is 86400 rows/day/process — the loss is noise.
  `requests` rows are the only rows carrying `latency_ms`, which makes them the only d=5 rows
  in the entire dataset. Losing 15 minutes of those is losing training data you cannot
  regenerate.

## Note on the docs

[CLAUDE.md](../CLAUDE.md) invariant 7 says a 60s flush "bounds crash loss to 60 seconds".
That is not what the code does, and [scraperd/writer.py:1-8](../scraperd/writer.py#L1-L8) already
says so correctly: the crash-loss window is one *segment*, not one flush. The code is right and
the invariant text is stale. Whichever fix is chosen should leave the two agreeing.
