# 05 — Wire `sdk_hook.record()` into the serving process

Fixes [B2](../issues/004-launch-blockers.md#b2--nothing-calls-sdk_hookrecord). Without this the
run produces **zero d=5 rows** in 90 days.

## The state of it

[scraperd/sdk_hook.py](../scraperd/sdk_hook.py) is finished and verified. Executed end to end:

```console
$ # 5 rows via record(), then the daemon drained them
INFO scraperd flushed 5 requests rows

request_id | latency_ms | cpu_usage | memory_usage_mb | gpu_usage | status
req-0      |       12.5 |       0.0 |       42.828125 |       0.0 | success
...
core_row: [[0.0, 42.828125, 0.0, 0.0, 12.5], ...]
```

Spool → `drain()` → parquet → `core_row()` all work. The only missing piece is a caller.

## 1. Answer the blocking question first

[file-layout.md](../file-layout.md) §5, open question 3: does `sdk/blurredbox_agd/metrics.py`
emit all five d=5 fields **in a single snapshot**?

```bash
grep -n "def get_process_metrics" -A 40 <agd-repo>/sdk/blurredbox_agd/metrics.py
```

- **Yes** → `record()`'s `_resources()` picks them up automatically via its
  `from blurredbox_agd.metrics import get_process_metrics` path.
- **No** → that is a hook to add **there**, not a workaround here. `_resources()` already falls
  back to raw psutil for `cpu_usage` / `memory_usage_mb`, which is why `gpu_usage` came back 0.0
  in the run above.

## 2. Add the call site

At the end of one inference, in the serving process, with that request's own measured wall-clock:

```python
import time
from scraperd import sdk_hook

start = time.perf_counter()
prediction = model.predict(payload)
latency_ms = (time.perf_counter() - start) * 1000.0

sdk_hook.record(
    spool_dir="/var/lib/blurredbox-agd/spool",   # must match config.yaml spool_dir
    request_id=request_id,
    latency_ms=latency_ms,
    status="success",
    input_size=len(payload),
    output_size=len(prediction),
    prediction=str(prediction),
)
```

Wrap it so a telemetry failure never fails an inference, and record the failure path too
(`status="error"`) — a run where only successful requests are logged has a biased latency
distribution.

**`latency_ms` must be this request's own duration.** Never a rolling average, never borrowed
from a poll sample. Invariant 1 in [CLAUDE.md](../CLAUDE.md): joining a nearby poll sample's
numbers onto a request manufactures a training row that never existed at serving time.

## 3. Check the process boundary

`record()` runs in the **serving** process; `drain()` runs in **scraperd**. They only share the
spool directory, so:

- the serving process needs write permission on `spool_dir`
- scraperd needs read + unlink on it
- if they run as different users, set the directory group-writable, or run scraperd as the
  serving user (which [07](07-install-on-the-target-host.md) needs anyway for `/proc` access)

## 4. Verify

```bash
ls /var/lib/blurredbox-agd/spool/                 # *.jsonl appearing as requests are served
journalctl -u scraperd | grep "requests rows"     # "flushed N requests rows", N > 0
.venv/bin/python check.py --config /etc/scraperd/config.yaml
```

`check.py` section 6 prints real `core_row()` output once the table exists. Until then it prints
"no requests table yet" and exits 0 — that message is the signal this step is not done.
