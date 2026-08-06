# 01 — Bound the request-row loss window

Fixes [001](../issues/001-request-rows-lost-on-hard-kill.md). Two options; take A unless you
have a reason not to.

---

## Option A — give `requests` its own segment interval (recommended)

The loss window equals `segment_seconds`. `samples` wants 900s because it writes 86400
rows/day/process and small files are wasteful. `requests` writes orders of magnitude fewer rows,
so short segments cost almost nothing there.

**1. [config.yaml](../config.yaml)** — add below `segment_seconds`:

```yaml
segment_seconds: 900              # samples: the 1 Hz firehose
request_segment_seconds: 60       # requests: scarce rows, small files, 60s exposure
```

**2. [scraperd/daemon.py](../scraperd/daemon.py)** — default it in `load_config`, next to the
other `setdefault` calls (~line 29):

```python
cfg.setdefault("request_segment_seconds", 60)
```

**3. [scraperd/daemon.py](../scraperd/daemon.py)** — `mk()` in `run()` (~line 129) takes the
interval as an argument instead of always reading `cfg["segment_seconds"]`:

```python
def mk(table, tbl_schema, segment_seconds):
    return TableWriter(
        root=root, table=table, schema=tbl_schema,
        entity_id=cfg["entity_id"], heartbeat=heartbeat,
        max_disk_pct=cfg["max_disk_pct"], segment_seconds=segment_seconds,
    )

writers = {"samples": mk("samples", schema.SAMPLES_SCHEMA, cfg["segment_seconds"])}
if cfg.get("spool_dir"):
    writers["requests"] = mk("requests", schema.REQUESTS_SCHEMA,
                             cfg["request_segment_seconds"])
```

Cuts exposure from 900s to 60s. Does not eliminate it.

### Verify

```bash
# spool 5 rows, run, confirm drained, then SIGKILL after the segment should have closed
.venv/bin/python -m scraperd.daemon --config test.yaml & PID=$!
sleep 70; kill -9 $PID
find cold/requests -name '*.parquet'        # a real footered file, not only .part
```

---

## Option B — unlink the spool file only after the segment closes

Eliminates the window instead of shrinking it. ~12 lines, and a second state to get wrong.

`drain()` renames each consumed file to `*.jsonl.done` instead of unlinking; `TableWriter.close()`
/ `_rotate()` reports a successful footer; the daemon then deletes the `.done` files that were
drained into that segment. On restart, `*.done` files older than the newest closed segment are
deleted; anything else is re-ingested.

Take B only if 60 seconds of request loss is genuinely unacceptable. It adds a
crash-during-rename case that Option A does not have, and it protects rows that Option A already
narrows to a one-minute window.

---

## Either way: fix the stale invariant

[CLAUDE.md](../CLAUDE.md) invariant 7 says the 60s flush "bounds crash loss to 60 seconds".
It doesn't — the flush drains the buffer into an open segment, and the footer is what makes data
readable. [scraperd/writer.py:1-8](../scraperd/writer.py#L1-L8) already documents this correctly.
Reword the invariant to say the crash-loss window is one **segment**, so the next person doesn't
trust the wrong number.
