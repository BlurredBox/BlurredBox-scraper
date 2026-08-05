# 01 — Bootstrap

`pyproject.toml`, the package skeleton, a venv. Fifteen minutes. No logic yet — this exists
so step 2 has somewhere to put `schema.py` and something to run `pytest` with.

## Do

### 1. `pyproject.toml`

```toml
[project]
name = "scraperd"
version = "0.1.0"
description = "Per-host telemetry daemon for model-serving processes"
requires-python = ">=3.11"
dependencies = ["psutil", "nvidia-ml-py", "pyarrow", "pyyaml"]

[project.optional-dependencies]
dev = ["pytest", "duckdb", "pytz"]

[project.scripts]
scraperd = "scraperd.daemon:main"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools]
packages = ["scraperd"]
```

`nvidia-ml-py` provides the same `import pynvml` as the `pynvml` distribution but is the
maintained one; `pynvml` prints a deprecation FutureWarning on every import, which over 90
days of restarts is journal noise for nothing (decision D3). `pytz` is dev-only — duckdb
needs it to hand a `TIMESTAMPTZ` column back to Python (D4).

Nothing else goes in here. pydantic, zmq, protobuf, scipy and numpy each have a trigger in
[file-layout.md](../file-layout.md) §6 and none have fired.

### 2. `scraperd/__init__.py`

```python
__version__ = "0.1.0"
```

Version only. Anything else imported at package level makes `import scraperd.schema` drag
in psutil, which the schema tests do not need.

### 3. Empty package dirs

```bash
mkdir -p scraperd tests deploy
touch tests/__init__.py 2>/dev/null; rm -f tests/__init__.py   # tests are not a package
```

### 4. Venv and install

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

`uv sync` works too if you have uv; `pip install -e` is the one that needs nothing else
installed first.

`.venv/` is not in `.gitignore` yet — [.gitignore](../.gitignore) covers `cold/`, `*.parquet`,
`manifest.json`, `heartbeat` and `*.db`. Add `.venv/` and `__pycache__/` now:

```bash
printf '\n.venv/\n__pycache__/\n*.egg-info/\n' >> .gitignore
```

## Verify

```bash
.venv/bin/scraperd --help
```

The console script resolves to `scraperd.daemon:main`, which does not exist yet, so this
**must fail** with:

```
ModuleNotFoundError: No module named 'scraperd.daemon'
```

That error is the pass condition — it proves the entry point is wired. Anything else
(`command not found`, a packaging error) means the install did not take.

```bash
.venv/bin/python -c "import scraperd, pyarrow, psutil, yaml, duckdb, pytz; print(scraperd.__version__, pyarrow.__version__)"
```

```
0.1.0 25.0.0
```

## Done when

- [ ] `pyproject.toml` with exactly those four runtime deps
- [ ] `scraperd/__init__.py` contains one line
- [ ] `.venv/bin/scraperd --help` fails on the missing `daemon` module and nothing else
- [ ] all six imports resolve
- [ ] `.gitignore` covers `.venv/`

→ [02-schema.md](02-schema.md)
