# 04 — Narrow `watch.patterns` to the real targets

Fixes [B1](../issues/004-launch-blockers.md#b1--watchpatterns-over-matches-by-18). Config only,
no code. Do this on the host the daemon will actually run on.

## 1. See what the current patterns select

```bash
pgrep -af freqtrade
pgrep -af uvicorn
```

On this dev box that is 36 processes, 34 of them `entrypoint.chaos-entity.sh` bash wrappers.
Count what you get; that number × 86400 × 90 is the run's row count.

## 2. Pick a pattern that matches only the process doing the work

The match is a substring of the full cmdline, falling back to the process name —
[scraperd/collect.py:85-100](../scraperd/collect.py#L85-L100). Make it specific enough to exclude
the shell wrappers:

```yaml
watch:
  patterns: ["freqtrade trade"]     # the python process, not entrypoint*.sh
  pids: []
  refresh_seconds: 60
```

`"freqtrade trade"` matches `/usr/local/bin/python3.14 .../freqtrade trade --config ...` and not
`/bin/bash /freqtrade/entrypoint.chaos-entity.sh`. Drop `uvicorn` unless something is actually
serving under it — on this host, nothing is.

If the target set is fixed and known, `pids: [2819, 2836]` is exact, at the cost of not
surviving a restart of those processes. Patterns plus `refresh_seconds` handle restarts;
prefer patterns.

## 3. Confirm the selection before committing to 90 days

```bash
.venv/bin/python -m scraperd.daemon --config config.yaml --log-level INFO 2>&1 | head -2
```

The daemon logs `watching N process(es): [...]` at startup. Check N and the pids are what you
expect. Wrong here means either a fleet of empty rows or a silently missing target.

## 4. Measure a real day before trusting the size projection

The only measurement so far used 5-second segments, which compress badly (46 B/row). Run for a
full day, then:

```bash
.venv/bin/python check.py --config config.yaml     # section 4 does the 90-day projection
df -h /var/lib                                     # against actual free space
```

Write both numbers down. The projection is only meaningful once segments are the real 900s size
and the process set is final.

## 5. Trim fields that are structurally dead on this host

`skip_fields` in [config.yaml](../config.yaml) — the field is never read, the column stays in the
schema and reads null, so every host's files still glob together:

```yaml
skip_fields: [gpu_usage, gpu_vram_mb, gpu_temperature, num_connections]
```

Use it for the `gpu_*` three on a CPU-only host ([B4](../issues/004-launch-blockers.md#b4--no-gpu-on-this-host))
and `num_connections` when the targets are containerised
([B6](../issues/004-launch-blockers.md#b6--num_connections-is-structurally-0-for-containerised-targets)) —
that one costs a `/proc/net` scan per process per second to produce zeros.

`process_id`, `timestamp`, `cpu_usage` and `memory_usage_mb` are rejected by `load_config`.
