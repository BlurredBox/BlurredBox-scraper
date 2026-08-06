"""The producer half stays stdlib-only; the consumer half rebuilds the row."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

from scraperd.schema import CORE_V1
from scraperd.sdk_hook import drain, record

ROOT = Path(__file__).resolve().parents[1]

CHILD = dedent(
    """
    import json, pathlib, sys

    class BlockPyarrow:
        def find_spec(self, name, path=None, target=None):
            if name == "pyarrow" or name.startswith("pyarrow."):
                raise ImportError("no pyarrow in the serving image")
            return None

    sys.meta_path.insert(0, BlockPyarrow())
    try:
        import pyarrow
    except ImportError:
        pass
    else:
        raise SystemExit("the block itself is broken: pyarrow imported")

    from scraperd.sdk_hook import record

    spool = sys.argv[1]
    record(spool, "req-child", 7.5)

    files = list(pathlib.Path(spool).glob("*.jsonl"))
    assert len(files) == 1, files
    lines = files[0].read_text().splitlines()
    assert len(lines) == 1, lines
    row = json.loads(lines[0])
    assert row["request_id"] == "req-child", row
    assert row["latency_ms"] == 7.5, row
    assert "pyarrow" not in sys.modules
    """
)


def test_record_needs_no_pyarrow(tmp_path):
    """record() runs in the model process — importing it must not need pyarrow."""
    env = os.environ | {"PYTHONPATH": str(ROOT)}
    p = subprocess.run(
        [sys.executable, "-c", CHILD, str(tmp_path / "spool")],
        capture_output=True, text=True, env=env,
    )
    assert p.returncode == 0, p.stderr
    assert list((tmp_path / "spool").glob("*.jsonl"))


def test_drain_roundtrips_a_recorded_row(tmp_path):
    spool = tmp_path / "spool"
    record(spool, "req-1", 12.5, status="success", input_size=3, output_size=4)

    [path] = list(spool.glob("*.jsonl"))
    path.rename(spool / f"{os.getpid()}-199001010000.jsonl")

    rows = drain(spool)
    assert len(rows) == 1
    row = rows[0]
    assert row["request_id"] == "req-1"
    assert row["latency_ms"] == 12.5
    assert isinstance(row["timestamp"], datetime)
    assert row["timestamp"].tzinfo is not None
    assert row["timestamp"] <= datetime.now(timezone.utc)
    assert [k for k in CORE_V1 if row.get(k) is None] == []
    assert not list(spool.glob("*.jsonl"))  # consumed exactly once
