# 002 — Heartbeat stays fresh while every row is being dropped

**Severity: high.** Defeats the one monitor the run has.
**Fix:** [../fix-steps/02-heartbeat-must-mean-writing.md](../fix-steps/02-heartbeat-must-mean-writing.md)

## What happens

`flush()` touches the heartbeat **before** it checks the disk —
[scraperd/writer.py:53-68](../scraperd/writer.py#L53-L68):

```python
def flush(self) -> int:
    self._touch_heartbeat()          # <-- line 55, unconditional
    if not self._rows:
        return 0
    rows, self._rows = self._rows, []

    pct = self._disk_pct()
    if pct >= self.max_disk_pct:     # <-- line 61, disk full
        self.dropped += len(rows)
        log.error("DISK ... REFUSING TO WRITE ...")
        return 0                     # rows already discarded from the buffer
```

Once the partition crosses `max_disk_pct` (90), every flush drops its rows, logs loudly to
journald — and touches the heartbeat anyway. [deploy/heartbeat-check.sh](../deploy/heartbeat-check.sh)
sees a fresh mtime and stays silent. Cron never mails anyone.

The daemon is alive, the alarm is green, and no data is being written. It stays that way until
somebody reads journald by hand.

## Why it matters

Invariants 6 and 9 in [CLAUDE.md](../CLAUDE.md) are each fine alone and don't connect:

- **9** — refuse to write above the threshold, and log loudly. Implemented.
- **6** — the heartbeat exists because "the realistic failure is the daemon dying in week 3 and
  nobody noticing until week 12". Implemented.

But invariant 6's whole premise is that nobody is watching the logs. A disk-full at day 40 is
exactly the week-3-death scenario wearing a different hat, and it is the one failure mode the
heartbeat is blind to. `log.error` into an unread journal is not a monitor.

The fix is that the heartbeat should mean **"I am writing data"**, not **"my process is
scheduled"**. A liveness check that stays green through total data loss is worse than none —
it is actively reassuring while the run dies.

## Related

The dropped rows are removed from the buffer at line 58 *before* the disk check at line 61, so
they are discarded, not retried. That is deliberate (`self.dropped` counts them) and correct —
retrying into a full disk just grows the buffer until the process OOMs. Only the heartbeat
ordering is wrong.
