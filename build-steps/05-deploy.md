# 05 — `deploy/`: systemd unit and the heartbeat cron

Build order step 4. Two hours. **Do not start a long run before this step exists**
(file-layout.md §7). Everything up to here produces data; this is what notices when it stops.

## Do

### 1. `deploy/heartbeat-check.sh`

```sh
#!/bin/sh
# Alert when scraperd's heartbeat goes stale. The single highest-value file here:
# for a 90-day unattended run the realistic failure is the daemon dying in week 3
# and nobody noticing until week 12.
#
# cron:  */5 * * * *  /opt/scraperd/deploy/heartbeat-check.sh
# Exits non-zero and writes stderr on failure, so cron mails it; also logs to syslog.
set -eu

HB="${SCRAPERD_HEARTBEAT:-/var/lib/blurredbox-agd/cold/heartbeat}"
MAX_AGE="${SCRAPERD_MAX_AGE_S:-300}"

mtime=$(stat -c %Y "$HB" 2>/dev/null || echo 0)
if [ "$mtime" -eq 0 ]; then
    msg="scraperd heartbeat MISSING: $HB"
else
    age=$(( $(date +%s) - mtime ))
    [ "$age" -le "$MAX_AGE" ] && exit 0
    msg="scraperd heartbeat STALE: ${age}s old (max ${MAX_AGE}s) at $HB"
fi

logger -t scraperd-heartbeat -p daemon.err "$msg" 2>/dev/null || true
echo "$msg" >&2
exit 1
```

```bash
chmod +x deploy/heartbeat-check.sh
```

`MAX_AGE` must exceed `flush_seconds`, with margin. 60s flush → 300s is right: five missed
flushes, not one slow one.

### 2. `deploy/scraperd.service`

```ini
[Unit]
Description=scraperd — per-host telemetry for model-serving processes
Documentation=file:///opt/scraperd/README.md
After=network-online.target

[Service]
Type=simple
ExecStart=/opt/scraperd/.venv/bin/scraperd --config /etc/scraperd/config.yaml
Restart=always
RestartSec=5

# Reading /proc/<pid>/io and /proc/<pid>/fd for another user's process needs
# privilege. Either run as root, or set User= to the same user as the watched
# processes. A mismatch shows up as io_read_bytes and num_fds silently zero.
User=root

# SIGTERM -> the daemon's stop event -> flush -> footer -> rename. Give it room:
# a stop that times out becomes SIGKILL and costs the open segment.
KillSignal=SIGTERM
TimeoutStopSec=90

# Never let the collector starve the thing it is measuring.
Nice=10
IOSchedulingClass=idle

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### 3. Install

```bash
sudo mkdir -p /opt/scraperd /etc/scraperd /var/lib/blurredbox-agd/cold
sudo cp -r scraperd deploy pyproject.toml /opt/scraperd/
sudo python3 -m venv /opt/scraperd/.venv
sudo /opt/scraperd/.venv/bin/pip install /opt/scraperd
sudo cp config.yaml /etc/scraperd/config.yaml     # edit entity_id + patterns first
sudo cp deploy/scraperd.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now scraperd
```

### 4. Cron

```bash
sudo crontab -l 2>/dev/null | { cat; echo '*/5 * * * * /opt/scraperd/deploy/heartbeat-check.sh'; } | sudo crontab -
```

Cron mails stderr to `MAILTO` (or root). If this box has no mail, replace the `echo` in the
script with your actual alert channel — a curl to a webhook, `systemd-cat`, whatever gets
looked at. **An alert nobody receives is the same as no heartbeat at all**, which is the
failure mode this whole file exists to prevent.

## Verify

Prove the alarm works before trusting it. All three cases, real output:

```bash
$ SCRAPERD_HEARTBEAT=/tmp/nope deploy/heartbeat-check.sh; echo "rc=$?"
scraperd heartbeat MISSING: /tmp/nope
rc=1

$ touch /tmp/hb; SCRAPERD_HEARTBEAT=/tmp/hb deploy/heartbeat-check.sh; echo "rc=$?"
rc=0

$ touch -d '10 minutes ago' /tmp/hb; SCRAPERD_HEARTBEAT=/tmp/hb deploy/heartbeat-check.sh; echo "rc=$?"
scraperd heartbeat STALE: 600s old (max 300s) at /tmp/hb
rc=1
```

Then the service:

```bash
sudo systemctl status scraperd            # active (running)
journalctl -u scraperd -n 20 --no-pager   # manifest, watching N, flushed N rows
stat -c '%y %n' /var/lib/blurredbox-agd/cold/heartbeat
```

Kill it and confirm both halves react:

```bash
sudo systemctl stop scraperd
journalctl -u scraperd -n 5 --no-pager | grep -E "segment closed|stopped cleanly"
sleep 310 && /opt/scraperd/deploy/heartbeat-check.sh; echo "rc=$?"   # must be rc=1
sudo systemctl start scraperd
```

And the restart path — `Restart=always` is only real if you have watched it happen:

```bash
sudo kill -9 $(systemctl show -p MainPID --value scraperd)
sleep 8 && systemctl is-active scraperd                              # active
ls /var/lib/blurredbox-agd/cold/samples/entity_id=*/*.part           # one orphan, expected
```

That `.part` is the SIGKILL'd segment: unreadable, ignored by the reader glob, ≤15 minutes of
rows (decision D2). Leave it. It is the visible cost of a hard kill, and sweeping it up
automatically would just hide how often that happens.

## Before the long run

- [ ] `build-steps/ANSWERS.md` Q1 filled in. If failure events in the trailing 90 days are
      single digits, **stop** — collecting 7.8M rows against twelve positives is not a
      training set, and no amount of clean parquet fixes it (file-layout.md §5.2)
- [ ] Q2 filled in: rows/day × 90 measured against real free disk, not estimated
- [ ] `max_disk_pct` set below the point where anything else on the box breaks
- [ ] the heartbeat alert reaches a human who will still be reading it in week 12

## Done when

- [ ] all three heartbeat-check cases behave as above
- [ ] `systemctl is-active scraperd` → `active`
- [ ] `kill -9` → the unit comes back within `RestartSec`
- [ ] `systemctl stop` → journal shows `segment closed` then `stopped cleanly`
- [ ] cron entry installed and its output goes somewhere a human reads
- [ ] `io_read_bytes` is non-zero in the data (if zero: user mismatch, see the unit comment)

→ [06-sdk-hook.md](06-sdk-hook.md)
