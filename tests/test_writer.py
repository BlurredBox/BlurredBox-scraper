"""Segment rotation, crash resume, disk guard, duckdb readback."""

from __future__ import annotations

import time
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


def test_idle_segment_closes_on_the_wall_clock(tmp_path):
    """No second row means no rotate — the footer must land on the timer anyway."""
    w = mk(tmp_path, segment_seconds=0.5)
    w.add(sample(T0))
    w.flush()
    assert files(tmp_path) == []           # segment still open, still a .part
    time.sleep(0.6)
    w.flush()                              # no new rows; timer alone closes it
    assert files(tmp_path) == ["2026-08-04T23-55.parquet"]


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
