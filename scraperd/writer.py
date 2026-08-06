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
import time
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
        self._seg_opened = 0.0

    # ── public ────────────────────────────────────────────────────────
    def add(self, row: dict) -> None:
        self._rows.append(row)

    def flush(self) -> int:
        """Drain the buffer into the current segment. Returns rows written."""
        self._touch_heartbeat()
        if not self._rows:
            self._close_if_stale()
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
        written += self._write(batch)
        self._close_if_stale()
        return written

    def close(self) -> None:
        """Write the footer and publish the segment. Idempotent."""
        self.flush()
        self._close_segment()

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

    def _close_if_stale(self) -> None:
        """Footer an idle segment on the wall clock.

        _needs_rotate only fires when the *next* row arrives, so a table with
        gaps in its traffic — requests, mostly — would otherwise sit on an
        unreadable .part indefinitely and lose it all to a SIGKILL.
        """
        if self._w is None:
            return
        # Wall clock since the segment *opened*, not row time: drained request
        # rows are up to two minutes old, and that lag is not exposure.
        if time.monotonic() - self._seg_opened >= self.segment_seconds:
            self._close_segment()

    def _close_segment(self) -> None:
        if self._w is None:
            return
        self._w.close()
        os.replace(self._part, self._final)
        log.info("segment closed: %s", self._final)
        self._w = self._part = self._final = self._seg_start = None

    def _rotate(self, ts) -> None:
        self._close_segment()
        self._seg_start = ts
        self._seg_opened = time.monotonic()
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
