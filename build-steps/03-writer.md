# 03 — `writer.py`: segments, heartbeat, disk guard

Build order step 2. Half a day. Parquet on disk, the liveness signal, and the refusal to
degrade silently on a full disk (invariants 6 and 9).

**Read decision D1 and D2 in [README.md](README.md) before this file.** The writer does not
produce one file per day, and the reason is not a preference.

## Do

### 1. `scraperd/writer.py`

```python
"""Parquet segment writer: time-bounded segments, heartbeat, disk guard.

A parquet file is unreadable until its footer is written at close(), so the
crash-loss window is one *segment*, not one flush. Segments therefore close on
a timer (`segment_seconds`) and are written as `NAME.parquet.part`, renamed to
`NAME.parquet` only once the footer lands. A crash leaves a `.part` behind that
the reader glob ignores instead of poisoning the whole table.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("scraperd.writer")


class TableWriter:
    def __init__(
        self,
        root: str | Path,
        table: str,
        schema: pa.Schema,
        entity_id: str,
        heartbeat: str | Path,
        max_disk_pct: float = 90.0,
        segment_seconds: int = 900,
        compression: str = "zstd",
    ) -> None:
        self.dir = Path(root) / table / f"entity_id={entity_id}"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.schema = schema
        self.heartbeat = Path(heartbeat)
        self.max_disk_pct = max_disk_pct
        self.segment_seconds = segment_seconds
        self.compression = compression
        self.dropped = 0
        self._rows: list[dict] = []
        self._w: pq.ParquetWriter | None = None
        self._part: Path | None = None
        self._final: Path | None = None
        self._seg_start = None

    # ── public ────────────────────────────────────────────────────────
    def add(self, row: dict) -> None:
        self._rows.append(row)

    def flush(self) -> int:
        """Drain the buffer into the current segment. Returns rows written."""
        self._touch_heartbeat()
        if not self._rows:
            return 0
        rows, self._rows = self._rows, []

        pct = self._disk_pct()
        if pct >= self.max_disk_pct:
            self.dropped += len(rows)
            log.error(
                "DISK %.1f%% >= threshold %.1f%% — REFUSING TO WRITE. "
                "Dropped %d rows this flush, %d total. Free space or the run is over.",
                pct, self.max_disk_pct, len(rows), self.dropped,
            )
            return 0

        written, batch = 0, []
        for row in rows:
            if self._needs_rotate(row["timestamp"]):
                written += self._write(batch)
                batch = []
                self._rotate(row["timestamp"])
            batch.append(row)
        return written + self._write(batch)

    def close(self) -> None:
        """Write the footer and publish the segment. Idempotent."""
        self.flush()
        if self._w is None:
            return
        self._w.close()
        os.replace(self._part, self._final)
        log.info("segment closed: %s", self._final)
        self._w = self._part = self._final = self._seg_start = None

    # ── internals ─────────────────────────────────────────────────────
    def _touch_heartbeat(self) -> None:
        self.heartbeat.parent.mkdir(parents=True, exist_ok=True)
        self.heartbeat.touch()

    def _disk_pct(self) -> float:
        u = shutil.disk_usage(self.dir)
        return 100.0 * u.used / u.total

    def _needs_rotate(self, ts) -> bool:
        if self._w is None:
            return True
        return (
            ts.date() != self._seg_start.date()
            or (ts - self._seg_start).total_seconds() >= self.segment_seconds
        )

    def _rotate(self, ts) -> None:
        if self._w is not None:
            self._w.close()
            os.replace(self._part, self._final)
        self._seg_start = ts
        self._final = self._segment_path(ts)
        self._part = self._final.with_name(self._final.name + ".part")
        self._w = pq.ParquetWriter(self._part, self.schema, compression=self.compression)

    def _segment_path(self, ts) -> Path:
        stem = ts.strftime("%Y-%m-%dT%H-%M")
        p = self.dir / f"{stem}.parquet"
        n = 0
        while p.exists() or p.with_name(p.name + ".part").exists():
            n += 1
            p = self.dir / f"{stem}-{n}.parquet"
        return p

    def _write(self, batch: list[dict]) -> int:
        if not batch:
            return 0
        self._w.write_table(pa.Table.from_pylist(batch, schema=self.schema))
        return len(batch)
```

### 2. `tests/test_writer.py`

```python
"""Segment rotation, crash resume, disk guard, duckdb readback."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from scraperd import schema
from scraperd.writer import TableWriter

T0 = datetime(2026, 8, 4, 23, 55, 0, tzinfo=timezone.utc)


def sample(ts, pid=1234):
    return {
        "process_id": pid,
        "cpu_usage": 10.0, "memory_usage_mb": 512.0, "memory_pct": 3.0,
        "gpu_usage": 0.0, "gpu_vram_mb": 0.0, "gpu_temperature": 0.0,
        "num_threads": 8, "num_fds": 42,
        "io_read_bytes": 100, "io_write_bytes": 200,
        "num_connections": 3, "ctx_switches": 77,
        "disk_usage_pct": 41.0, "freqtrade_cpu": 0.0, "freqtrade_memory_mb": 0.0,
        "timestamp": ts,
    }


def mk(tmp_path, **kw):
    return TableWriter(
        root=tmp_path / "cold", table="samples", schema=schema.SAMPLES_SCHEMA,
        entity_id="vm1", heartbeat=tmp_path / "cold" / "heartbeat", **kw,
    )


def files(tmp_path):
    return sorted(p.name for p in (tmp_path / "cold/samples/entity_id=vm1").glob("*.parquet"))


def test_heartbeat_touched_even_with_no_rows(tmp_path):
    w = mk(tmp_path)
    hb = tmp_path / "cold" / "heartbeat"
    assert w.flush() == 0
    assert hb.exists()
    before = hb.stat().st_mtime_ns
    w.flush()
    assert hb.stat().st_mtime_ns >= before


def test_segment_rotates_on_time_and_on_midnight(tmp_path):
    w = mk(tmp_path, segment_seconds=300)
    for i in range(0, 1200, 60):          # 23:55 → 00:14 next day, 1 row/min
        w.add(sample(T0 + timedelta(seconds=i)))
    w.close()
    names = files(tmp_path)
    assert names == [
        "2026-08-04T23-55.parquet",       # 23:55–23:59
        "2026-08-05T00-00.parquet",       # midnight forces a new segment
        "2026-08-05T00-05.parquet",
        "2026-08-05T00-10.parquet",
    ]


def test_crash_leaves_a_part_file_that_the_glob_ignores(tmp_path):
    w = mk(tmp_path, segment_seconds=3600)
    for i in range(10):
        w.add(sample(T0 + timedelta(seconds=i)))
    w.flush()                              # rows on disk, footer NOT yet written
    del w                                  # SIGKILL: no close(), no footer

    assert files(tmp_path) == []           # nothing published
    parts = list((tmp_path / "cold/samples/entity_id=vm1").glob("*.part"))
    assert len(parts) == 1                 # the orphan is visible and inert


def test_resume_after_crash_writes_a_new_segment_beside_the_orphan(tmp_path):
    w = mk(tmp_path, segment_seconds=3600)
    w.add(sample(T0))
    w.flush()
    del w                                  # crash

    w2 = mk(tmp_path, segment_seconds=3600)
    for i in range(1, 6):
        w2.add(sample(T0 + timedelta(seconds=i)))
    w2.close()

    assert files(tmp_path) == ["2026-08-04T23-55-1.parquet"]
    rows = duckdb.sql(
        f"SELECT * FROM read_parquet('{tmp_path}/cold/samples/*/*.parquet', hive_partitioning=1)"
    ).fetchall()
    assert len(rows) == 5                  # loss is bounded to the open segment


def test_readback_matches_what_went_in(tmp_path):
    w = mk(tmp_path, segment_seconds=3600)
    for i in range(120):
        w.add(sample(T0 + timedelta(seconds=i), pid=1000 + i))
    w.close()

    got = duckdb.sql(
        f"""SELECT entity_id, count(*) n, min(process_id) lo, max(process_id) hi,
                   min(timestamp) t0, avg(cpu_usage) cpu
            FROM read_parquet('{tmp_path}/cold/samples/*/*.parquet', hive_partitioning=1)
            GROUP BY entity_id"""
    ).fetchone()
    assert got[0] == "vm1"                 # entity_id free from hive partitioning
    assert (got[1], got[2], got[3]) == (120, 1000, 1119)
    assert got[4] == T0                    # tz-aware, compared as an instant
    assert got[5] == pytest.approx(10.0)


def test_wrong_type_is_rejected_at_write_time(tmp_path):
    w = mk(tmp_path)
    bad = sample(T0) | {"num_threads": "eight"}
    w.add(bad)
    with pytest.raises(Exception):
        w.flush()


def test_refuses_to_write_above_the_disk_threshold(tmp_path, caplog):
    w = mk(tmp_path, max_disk_pct=0.0)
    w.add(sample(T0))
    with caplog.at_level("ERROR"):
        assert w.flush() == 0
    assert w.dropped == 1
    assert "REFUSING TO WRITE" in caplog.text
    assert files(tmp_path) == []


def test_a_stray_latency_never_reaches_the_samples_file(tmp_path):
    """The schema is the filter: an extra key is dropped, not smuggled in."""
    w = mk(tmp_path)
    w.add(sample(T0) | {"latency_ms": 12.0})
    w.close()
    cols = duckdb.sql(
        f"SELECT * FROM read_parquet('{tmp_path}/cold/samples/*/*.parquet')"
    ).columns
    assert "latency_ms" not in cols
```

## Verify

```bash
.venv/bin/pytest tests/ -q
```

```
.....................                                                    [100%]
21 passed in 0.16s
```

Thirteen from step 2, eight here.

## What each test is actually defending

| test | the failure it prevents |
|---|---|
| `heartbeat_touched_even_with_no_rows` | daemon alive but idle looks dead to cron; the heartbeat must beat on empty flushes too |
| `segment_rotates_on_time_and_on_midnight` | one 90-day file, or a segment that silently straddles a date boundary |
| `crash_leaves_a_part_file_that_the_glob_ignores` | one headless file making `read_parquet('*/*.parquet')` fail for the entire table |
| `resume_after_crash…` | restart clobbering the previous run's segment, or refusing to start |
| `readback_matches_what_went_in` | the columnar round trip quietly mangling values; also proves hive partitioning yields `entity_id` |
| `wrong_type_is_rejected_at_write_time` | the reason there is no pydantic — pyarrow already rejects it |
| `refuses_to_write_above_the_disk_threshold` | silent degradation on a full disk, which loses the run (invariant 9) |
| `a_stray_latency_never_reaches_the_samples_file` | invariant 1, enforced by the schema rather than by discipline |

Note what the last one showed: `pa.Table.from_pylist(rows, schema=...)` **drops** unknown
keys, it does not raise. That is the safe direction — a stray `latency_ms` cannot reach
`samples` — but it means the writer will not tell you about a typo'd field name either. That
is what the step-2 coverage gate is for.

## Knobs and their consequences

| knob | default | what moving it costs |
|---|---|---|
| `segment_seconds` | 900 | 96 files/day/table, ≤15 min crash loss. Set 60 for a 60s bound and 1440 files/day. Set 3600 for 24 files/day and an hour of exposure. |
| `flush_seconds` (step 4) | 60 | buffer residency, ~4 MB working set. Below ~10s, revisit the SQLite trigger in file-layout.md §6. |
| `max_disk_pct` | 90 | rows are dropped, loudly, above this. Lower it if the partition is shared. |
| `compression` | `zstd` | `snappy` is faster and ~30% larger. Telemetry series compress hard either way. |

## Done when

- [ ] `.venv/bin/pytest tests/ -q` → 21 passed
- [ ] you understand why segment files are named `2026-08-04T23-55.parquet` (D1)
- [ ] `grep -n "REFUSING TO WRITE" scraperd/writer.py` finds the loud path

→ [04-collect-daemon.md](04-collect-daemon.md)
