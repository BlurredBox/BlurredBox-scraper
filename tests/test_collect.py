"""skip_fields: the field is not read, the column still exists and reads null."""

from __future__ import annotations

import os

import psutil
import pyarrow.parquet as pq
import pytest

from scraperd import collect, schema
from scraperd.daemon import load_config, write_manifest
from scraperd.writer import TableWriter

SKIP = frozenset({"num_connections", "gpu_usage", "gpu_vram_mb", "gpu_temperature"})


def test_skipped_fields_absent_from_row():
    proc = psutil.Process(os.getpid())
    row = collect.sample(proc, collect.host_metrics(SKIP), SKIP)
    assert not SKIP & set(row)
    assert row["process_id"] == os.getpid()
    for name in ("cpu_usage", "memory_usage_mb", "timestamp", "num_fds", "ctx_switches"):
        assert name in row


def test_skipped_column_reads_back_null(tmp_path):
    w = TableWriter(
        root=tmp_path, table="samples", schema=schema.SAMPLES_SCHEMA,
        entity_id="t", heartbeat=tmp_path / "hb",
    )
    w.add(collect.sample(psutil.Process(os.getpid()), collect.host_metrics(SKIP), SKIP))
    w.close()

    t = pq.read_table(str(tmp_path / "samples" / "entity_id=t"))
    assert tuple(t.schema.names) == schema.SAMPLE_FIELDS  # schema unchanged
    assert t.column("num_connections")[0].as_py() is None
    assert t.column("cpu_usage")[0].as_py() is not None


@pytest.mark.parametrize("bad", ["latency_ms", "cpu_usage", "process_id", "nonsense"])
# latency_ms and nonsense are not samples fields at all; cpu_usage and
# process_id are, and are locked.
def test_config_refuses_bad_skip_field(tmp_path, bad):
    cfg = tmp_path / "c.yaml"
    cfg.write_text(f"entity_id: t\noutput_root: {tmp_path}\nskip_fields: [{bad}]\n")
    with pytest.raises(SystemExit, match=bad):
        load_config(cfg)


def test_valueless_keys_are_not_a_traceback(tmp_path):
    """`watch:`/`manifest:` with nothing after them parse to None, not {}."""
    cfg = tmp_path / "c.yaml"
    cfg.write_text(f"entity_id: t\noutput_root: {tmp_path}\nwatch:\nmanifest:\n")
    loaded = load_config(cfg)
    assert loaded["watch"] == {"patterns": [], "pids": [], "refresh_seconds": 60}
    assert write_manifest(loaded, tmp_path).exists()


def test_empty_config_exits_readably(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text("")
    with pytest.raises(SystemExit, match="entity_id"):
        load_config(cfg)
