# 003 — `watch:` with no value crashes with a traceback

**Severity: low.** Fails at startup, loudly, before any data is collected.
**Fix:** [../fix-steps/03-config-error-not-traceback.md](../fix-steps/03-config-error-not-traceback.md)

## What happens

[scraperd/daemon.py:31-34](../scraperd/daemon.py#L31-L34):

```python
cfg.setdefault("watch", {})
cfg["watch"].setdefault("patterns", [])
```

`setdefault` only supplies a default when the key is **absent**. YAML `watch:` with nothing
after it parses to `None`, the key is present, and the next line calls `.setdefault` on `None`.

## Reproduction

```console
$ printf 'entity_id: t\noutput_root: /tmp/x\nwatch:\n' > c.yaml
$ .venv/bin/python -c "from scraperd.daemon import load_config; load_config('c.yaml')"
AttributeError: 'NoneType' object has no attribute 'setdefault'
```

Compare the `manifest:` key, which is handled correctly in `write_manifest()` via
`cfg.get("manifest") or {}` — the `or {}` is what makes it null-safe.

## Why it is only low

It cannot corrupt data or lose a run. `scraperd` exits non-zero at startup, systemd's
`Restart=always` retries into the same crash, and the operator sees it immediately.

It is worth fixing anyway because it will happen while someone is editing config on the client
machine at deploy time — commenting the patterns out to test something is the obvious way to
trip it — and an `AttributeError` sends them reading source instead of reading their config.

Every other config mistake in `load_config` produces a clean `SystemExit` with a sentence
explaining it (missing `entity_id`, bad `skip_fields`). This is the one that doesn't.

## Same shape, second site

[scraperd/daemon.py:65](../scraperd/daemon.py#L65) in `write_manifest`:

```python
"version": cfg.get("manifest", {}).get("version") or __version__,
```

`manifest:` written empty gives `cfg.get("manifest", {})` → `None` → same `AttributeError`. The
line above it uses the safe `cfg.get("manifest") or {}` form. Fix both in one pass.
