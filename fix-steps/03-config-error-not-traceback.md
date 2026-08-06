# 03 — Config errors, not tracebacks

Fixes [003](../issues/003-empty-watch-key-crashes-startup.md). Two lines.

## The change

**[scraperd/daemon.py:31](../scraperd/daemon.py#L31)** — `setdefault` doesn't help when the key
exists with a `None` value, which is what `watch:` on its own parses to:

```python
    cfg["watch"] = cfg.get("watch") or {}        # was: cfg.setdefault("watch", {})
```

**[scraperd/daemon.py:65](../scraperd/daemon.py#L65)** — same shape, in `write_manifest`:

```python
        "version": (cfg.get("manifest") or {}).get("version") or __version__,
```

Line 55 already uses the safe `cfg.get("manifest") or {}` form; this makes line 65 match.

## Verify

```bash
printf 'entity_id: t\noutput_root: /tmp/x\nwatch:\n' > /tmp/c.yaml
.venv/bin/python -c "from scraperd.daemon import load_config; print(load_config('/tmp/c.yaml')['watch'])"
```

Before: `AttributeError: 'NoneType' object has no attribute 'setdefault'`
After: `{'patterns': [], 'pids': [], 'refresh_seconds': 60}`

For the manifest half, the config needs `manifest:` empty and `write_manifest()` actually called
— run the daemon against a config with a bare `manifest:` key.

## Optional, same neighbourhood

Every other failure in `load_config` exits with a sentence explaining itself (missing
`entity_id`, unknown `skip_fields`, locked `skip_fields`). Once the two `None` cases are handled,
`load_config` has no path left that raises anything but `SystemExit` with a readable message —
which is the property worth keeping as more config keys are added.
