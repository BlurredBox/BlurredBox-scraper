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
