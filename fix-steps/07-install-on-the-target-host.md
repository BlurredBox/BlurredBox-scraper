# 07 — Install on the target host

Fixes [B4](../issues/004-launch-blockers.md#b4--no-gpu-on-this-host)–[B6](../issues/004-launch-blockers.md#b6--num_connections-is-structurally-0-for-containerised-targets),
and the fact that nothing is installed anywhere yet.

**Do not start the 90-day run before this step is complete.** Running `scraperd` in a terminal
works, but for an unattended run the realistic failure is the daemon dying in week 3 and nobody
noticing until week 12 — `Restart=always` and the heartbeat cron are what prevent that.

## 0. Which host

scraperd reads `/proc` via psutil. It must run on the **same kernel** as the watched processes —
it cannot scrape a remote host. So it installs on the client machine, next to the model-serving
process, always.

Everything measured in [004](../issues/004-launch-blockers.md) came from the dev box. On the
target host, re-check: process count (`pgrep -af <pattern>`), GPU presence (`nvidia-smi -L`),
whether targets are containerised (`num_connections`), and free disk on `/var/lib`.

## 1. Install

```bash
sudo git clone <repo> /opt/scraperd && cd /opt/scraperd
sudo python -m venv .venv && sudo .venv/bin/pip install -e .   # or: uv sync
sudo mkdir -p /etc/scraperd /var/lib/blurredbox-agd/spool
sudo cp config.yaml /etc/scraperd/config.yaml
```

## 2. Configure for this host

Edit `/etc/scraperd/config.yaml`:

- `entity_id` — **the host**, not the process. One per host (invariant 5). Not `vm1` on two
  machines.
- `watch.patterns` — per [04](04-narrow-the-watch-patterns.md). Confirm with `pgrep -af`.
- `skip_fields` — `gpu_*` if `nvidia-smi -L` fails; `num_connections` if targets are containerised.
- `agd_repo` — uncomment and point at the BlurredBox-AGD checkout, or accept that
  `freqtrade_cpu` / `freqtrade_memory_mb` stay 0.0 for the whole run (invariant 8).
- `manifest.*` — `model_id`, `model_version_id`, `slug`, `tenant_id`, `operator_id`,
  `artifact_id`. These are run-constant; filling them in later means the manifest describes the
  run incorrectly.
- paths — `/opt/scraperd`, `/etc/scraperd`, `/var/lib/blurredbox-agd` are hardcoded in
  [deploy/scraperd.service](../deploy/scraperd.service) too. Change both or neither.

## 3. Permissions

Reading `/proc/<pid>/io` and `/proc/<pid>/fd` for another user's process needs privilege. A
mismatch is **silent**: `io_read_bytes` and `num_fds` come back 0 with no error
([scraperd/collect.py](../scraperd/collect.py) swallows the `AccessDenied`).

`User=root` in the unit file covers it. If root is off the table, set `User=` to the owner of
the watched processes — and the same user needs read + unlink on `spool_dir`.

## 4. systemd + cron

```bash
sudo cp deploy/scraperd.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now scraperd
sudo systemctl status scraperd

echo '*/5 * * * * /opt/scraperd/deploy/heartbeat-check.sh' | sudo crontab -
```

The cron line is not optional. It is ten lines of shell and the highest-value file in the repo —
without it, `Restart=always` covers a crash but nothing covers a daemon that is alive and
writing nothing (see [02](02-heartbeat-must-mean-writing.md)).

Confirm cron can actually reach someone: `MAILTO=` set, or the alert routed wherever alerts go.
A cron job mailing a mailbox nobody reads is the same as no monitor.

## 5. Verify the install, then again after a day

```bash
journalctl -u scraperd -f | head -20         # "watching N process(es)" — N is right?
ls /var/lib/blurredbox-agd/cold/             # manifest.json, heartbeat, samples/
stat -c %y /var/lib/blurredbox-agd/cold/heartbeat   # advancing every flush_seconds

SCRAPERD_MAX_AGE_S=1 /opt/scraperd/deploy/heartbeat-check.sh; echo $?   # 1 — alarm works
sudo systemctl stop scraperd                 # SIGTERM path: no .part left behind
find /var/lib/blurredbox-agd/cold -name '*.part'
```

After 24 hours:

```bash
.venv/bin/python check.py --config /etc/scraperd/config.yaml
```

Sections 1–4 are for reading: process coverage, cadence gaps, dead columns, and the 90-day size
projection against real free disk. Section 5 asserts the invariants and exits non-zero on
failure. Section 6 prints real `core_row()` output — or tells you [05](05-wire-sdk-hook-into-serving.md)
isn't done.
