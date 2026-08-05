# 07 — Verify the data is what you think it is

One hour, on day 1 of the run and again at week 1. Every query here has been run against real
output. The point is not that the daemon is up — step 5 covered that — it is that the columns
contain what you assume they contain.

Save this as `check.py` and run it weekly.

## Do

### 1. Coverage: is every watched process actually in there

```python
import duckdb
S = "read_parquet('/var/lib/blurredbox-agd/cold/samples/*/*.parquet', hive_partitioning=1)"

duckdb.sql(f"""
SELECT entity_id, process_id, count(*) n,
       min(timestamp) first_seen, max(timestamp) last_seen
FROM {S} GROUP BY 1,2 ORDER BY n DESC""").show()
```

A process you expected to see and don't means the `watch.patterns` substring never matched.
Check with `ps -ef | grep <pattern>` against the **full cmdline** — that is what `resolve()`
matches on.

### 2. Gaps: did the 1 Hz cadence hold

```python
duckdb.sql(f"""
WITH d AS (
  SELECT process_id,
         epoch(timestamp) - epoch(lag(timestamp) OVER (PARTITION BY process_id ORDER BY timestamp)) gap
  FROM {S})
SELECT process_id, count(*) n, round(max(gap),1) max_gap_s, round(avg(gap),2) avg_gap_s
FROM d GROUP BY 1 ORDER BY max_gap_s DESC NULLS LAST LIMIT 10""").show()
```

```
┌────────────┬───────┬───────────┐
│ process_id │   n   │ max_gap_s │
├────────────┼───────┼───────────┤
│       4007 │    22 │       1.0 │
```

`avg_gap_s` ≈ 1.0 is the cadence holding. A `max_gap_s` of hundreds is a restart — cross-check
against `journalctl -u scraperd | grep -c "watching"`. A `max_gap_s` that creeps above 1.0 as
process count grows means one tick's sampling now takes longer than the cadence; raise
`cadence_seconds` rather than letting the loop slip silently.

### 3. Dead columns: which fields are all zero

```python
duckdb.sql(f"""
SELECT count(*) n,
       count(*) FILTER (WHERE io_read_bytes = 0)  zero_io,
       count(*) FILTER (WHERE gpu_usage = 0)      zero_gpu,
       count(*) FILTER (WHERE num_fds = 0)        zero_fds,
       count(*) FILTER (WHERE cpu_usage = 0)      zero_cpu
FROM {S}""").show()
```

```
┌─────────┬──────────┬───────┐
│ zero_io │ zero_gpu │   n   │
├─────────┼──────────┼───────┤
│      44 │      660 │   660 │
```

Read that honestly:

| all-zero column | meaning |
|---|---|
| `gpu_usage`, `gpu_vram_mb`, `gpu_temperature` | expected on a CPU-only host. On a GPU host it means no NVML — check the startup log for `no GPU backend` |
| `io_read_bytes`, `num_fds` | **permissions.** scraperd is not running as a user that can read `/proc/<pid>/io`. See the `User=` comment in [05-deploy.md](05-deploy.md) |
| `cpu_usage` on every row for a pid | the `psutil.Process` object is being recreated each tick, so the delta is always zero. Only possible if `sample_loop`'s caching was changed |
| `freqtrade_*` | no `agd_repo` configured, or no freqtrade running |

A column that is structurally zero is not a bug you have to fix — it is a fact you have to
know before someone trains on it.

### 4. Size projection: does 90 days fit

```python
duckdb.sql("""
SELECT round(sum(total_compressed_size)/1e6, 2) mb
FROM parquet_metadata('/var/lib/blurredbox-agd/cold/samples/*/*.parquet')""").show()
```

```
┌────────┐
│   kb   │
│   18.3 │
└────────┘
```

Multiply one full day by 90 and compare against `df -h`. Do this on day 1, not month 2. Write
the number into `ANSWERS.md` Q2.

### 5. Invariants, as queries

```python
# 1. samples has no latency column, at all
assert "latency_ms" not in duckdb.sql(f"SELECT * FROM {S} LIMIT 0").columns

# 2. requests always has a real latency
R = "read_parquet('/var/lib/blurredbox-agd/cold/requests/*/*.parquet', hive_partitioning=1)"
assert duckdb.sql(f"SELECT count(*) FROM {R} WHERE latency_ms IS NULL OR latency_ms <= 0").fetchone()[0] == 0

# 3. one entity_id, and it is the host
assert duckdb.sql(f"SELECT count(DISTINCT entity_id) FROM {S}").fetchone()[0] == 1

# 4. no orphan .part files older than a day (each one is a hard kill)
import glob, os, time
stale = [p for p in glob.glob('/var/lib/blurredbox-agd/cold/*/*/*.part')
         if time.time() - os.path.getmtime(p) > 86400]
print("stale .part files:", stale)
```

### 6. The d=5 path, once

The only supported way to build a model input:

```python
from scraperd.schema import core_row
rows = duckdb.sql(f"SELECT * FROM {R} LIMIT 5").df().to_dict("records")
X = [core_row(r) for r in rows]
```

If that raises, the row is not a d=5 row and must not be trained on. Never assemble the vector
by hand, never source it from `samples`, and never join a request's latency onto a nearby poll
sample — invariants 1 and 3.

## Done when

- [ ] every expected pid appears with a plausible row count
- [ ] `avg_gap_s` ≈ 1.0
- [ ] you can explain every all-zero column, and permission-caused zeros are fixed
- [ ] one measured day × 90 fits on the partition, written into `ANSWERS.md`
- [ ] all four invariant assertions pass
- [ ] `core_row()` builds a vector from a real `requests` row

→ [08-week-one.md](08-week-one.md)
