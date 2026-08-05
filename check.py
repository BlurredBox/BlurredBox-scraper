#!/usr/bin/env python3
"""Weekly data check — build-steps/07-verify-the-data.md, as a script.

    python check.py [--config config.yaml]

Prints coverage, cadence gaps, all-zero columns and a 90-day size projection,
then asserts the four invariants. Exits non-zero if an invariant fails; the
descriptive sections are printed for a human to read, not asserted.
"""

from __future__ import annotations

import argparse
import glob
import os
import shutil
import sys
import time

import duckdb
import yaml


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    root = yaml.safe_load(open(ap.parse_args().config))["output_root"]

    S = f"read_parquet('{root}/samples/*/*.parquet', hive_partitioning=1)"
    R = f"read_parquet('{root}/requests/*/*.parquet', hive_partitioning=1)"

    print("\n== 1. coverage: every watched process present ==")
    duckdb.sql(f"""
    SELECT entity_id, process_id, count(*) n,
           min(timestamp) first_seen, max(timestamp) last_seen
    FROM {S} GROUP BY 1,2 ORDER BY n DESC""").show()
    print("a missing pid means watch.patterns never matched its full cmdline")

    print("\n== 2. gaps: did the 1 Hz cadence hold ==")
    duckdb.sql(f"""
    WITH d AS (
      SELECT process_id,
             epoch(timestamp) - epoch(lag(timestamp)
               OVER (PARTITION BY process_id ORDER BY timestamp)) gap
      FROM {S})
    SELECT process_id, count(*) n, round(max(gap),1) max_gap_s, round(avg(gap),2) avg_gap_s
    FROM d GROUP BY 1 ORDER BY max_gap_s DESC NULLS LAST LIMIT 10""").show()
    print("avg_gap_s ~1.0 is the cadence holding; a max_gap_s of hundreds is a restart")

    print("\n== 3. dead columns: which fields are all zero ==")
    duckdb.sql(f"""
    SELECT count(*) n,
           count(*) FILTER (WHERE io_read_bytes = 0)  zero_io,
           count(*) FILTER (WHERE gpu_usage = 0)      zero_gpu,
           count(*) FILTER (WHERE num_fds = 0)        zero_fds,
           count(*) FILTER (WHERE cpu_usage = 0)      zero_cpu
    FROM {S}""").show()
    print("zero io/fds = permissions (User= in deploy). zero gpu = CPU-only host or no NVML.")

    print("\n== 4. size projection: does 90 days fit ==")
    mb, first, last = duckdb.sql(f"""
    SELECT (SELECT sum(total_compressed_size)/1e6
            FROM parquet_metadata('{root}/samples/*/*.parquet')),
           (SELECT epoch(min(timestamp)) FROM {S}),
           (SELECT epoch(max(timestamp)) FROM {S})""").fetchone()
    days = max((last - first) / 86400, 1 / 1440)  # guard: <1 min of data
    free_mb = shutil.disk_usage(root).free / 1e6
    print(f"{mb:.3f} MB over {days * 24:.1f} h -> {mb / days * 90:.1f} MB projected for 90 days")
    print(f"{free_mb:.0f} MB free on the partition")
    print("write the projected number into build-steps/ANSWERS.md Q2")

    print("\n== 5. invariants ==")
    assert "latency_ms" not in duckdb.sql(f"SELECT * FROM {S} LIMIT 0").columns, \
        "samples has a latency_ms column — invariant 1"
    assert duckdb.sql(
        f"SELECT count(*) FROM {R} WHERE latency_ms IS NULL OR latency_ms <= 0"
    ).fetchone()[0] == 0, "requests rows with null/non-positive latency_ms"
    assert duckdb.sql(f"SELECT count(DISTINCT entity_id) FROM {S}").fetchone()[0] == 1, \
        "more than one entity_id — invariant 5"
    stale = [p for p in glob.glob(f"{root}/*/*/*.part")
             if time.time() - os.path.getmtime(p) > 86400]
    assert not stale, f"stale .part files (each one a hard kill): {stale}"
    print("all four pass")

    print("\n== 6. the d=5 path ==")
    from scraperd.schema import core_row
    # ponytail: dicts by hand, not .df() — pandas/numpy are not runtime deps here.
    q = duckdb.sql(f"SELECT * FROM {R} LIMIT 5")
    rows = [dict(zip(q.columns, r)) for r in q.fetchall()]
    print([core_row(r) for r in rows])
    return 0


if __name__ == "__main__":
    sys.exit(main())
