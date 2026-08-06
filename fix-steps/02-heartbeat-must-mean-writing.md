# 02 — Make the heartbeat mean "writing", not "running"

Fixes [002](../issues/002-heartbeat-green-while-writes-refused.md). Four lines.

## The change

[scraperd/writer.py:53-68](../scraperd/writer.py#L53-L68) — move the heartbeat touch to **after**
the disk check, and skip it when the disk guard fires:

```python
    def flush(self) -> int:
        """Drain the buffer into the current segment. Returns rows written."""
        pct = self._disk_pct()
        if pct >= self.max_disk_pct:
            self.dropped += len(self._rows)
            log.error(
                "DISK %.1f%% >= threshold %.1f%% — REFUSING TO WRITE. "
                "Dropped %d rows this flush, %d total. Heartbeat NOT touched — "
                "the cron check will alert. Free space or the run is over.",
                pct, self.max_disk_pct, len(self._rows), self.dropped,
            )
            self._rows = []
            return 0

        # Touched only on a flush that could actually write. A heartbeat that
        # stays fresh through a full disk is worse than none — it reassures
        # while the run dies.
        self._touch_heartbeat()
        if not self._rows:
            return 0
        rows, self._rows = self._rows, []
        ...
```

Note the disk check now runs before the empty-buffer early return, so an idle writer on a full
disk also stops touching the heartbeat. That is intended: the disk is full either way.

## Consequence

`SCRAPERD_MAX_AGE_S` in [deploy/heartbeat-check.sh](../deploy/heartbeat-check.sh) is 300s and
`flush_seconds` is 60, so the alarm fires within ~5 minutes of the disk crossing the threshold
— while there is still room to act, instead of after the run is gone.

## Verify

```bash
# threshold below current usage => every flush must refuse
sed 's/max_disk_pct: 90/max_disk_pct: 1/' config.yaml > full.yaml
.venv/bin/python -m scraperd.daemon --config full.yaml & PID=$!
sleep 5; kill -TERM $PID
```

Expect: `REFUSING TO WRITE` in the log, and **no heartbeat file created** (or an mtime that
stops advancing if one already existed). Then:

```bash
SCRAPERD_HEARTBEAT=cold/heartbeat SCRAPERD_MAX_AGE_S=1 deploy/heartbeat-check.sh
echo $?      # 1, with "MISSING" or "STALE" on stderr
```

Before the fix that script exits 0 and says nothing, which is the whole bug.

## Worth adding at the same time

`TableWriter.dropped` is only reported once, in the daemon's shutdown log line. Nothing surfaces
it during a run. Logging it on each refused flush (as above) is enough — `check.py` already
counts rows independently, so a gap shows up there too.
