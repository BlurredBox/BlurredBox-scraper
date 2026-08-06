# ANSWERS — the two blocking pre-run questions

Answers to [fix-steps/06](fix-steps/06-answer-the-two-blocking-questions.md), which closes
[B3](issues/004-launch-blockers.md#b3--the-label-count-question-was-never-answered).

Measured 2026-08-06 on the dev box (`/dev/nvme0n1p5`, 12 cores, 14 GB RAM). Re-measure §2 on the
target host — psutil reads `/proc`, so the daemon runs wherever the watched processes live.

---

## Q1 — Label count

```
Failure events in the last 90 days:  1,149  (0.201% of 570,550 rows)
Source queried:                      BlurredBox-AGD/data/chaos_harness.csv  (schema v2)
                                     BlurredBox-AGD/logs/chaos_entities/*/fault_schedule.jsonl
Definition of "failure" used:        label == 1, as already emitted by the chaos harness
Window actually covered:             2026-07-07 → 2026-07-19 (12.6 days, 64 entities)
```

Positives by source:

| source | rows | label=1 | rate |
|---|---|---|---|
| `clean` | 167,142 | **0** | 0% |
| `cpu_burn` | 286,550 | 34 | 0.012% |
| `mem_leak` | 116,858 | 1,115 | 0.954% |

**The answer to "is it single digits" is: it depends entirely on fault injection, and injection is
already running.** Zero positives came from the `clean` stream — the organic failure count over
12.6 days across 64 entities is 0. Every positive in the dataset was injected. The
`entrypoint.chaos-entity.sh` processes on this host (12 of them) are that harness.

So of the three options in fix-step 06, **fault injection is not a design change to make — it is
the existing design, and the run's positive count is a dial, not a discovery.** Consequences:

- The 90-day run must keep chaos injection running for its whole duration. A run against a quiet
  fleet yields ~0 positives, which is the failure mode the question was asked to prevent.
- At the observed 0.2% rate, 93.3 M rows (see Q2) would carry ~187 k positives. That is not a
  class-imbalance problem. But the number is a function of injection intensity, not of the
  infrastructure — set it deliberately.
- `mem_leak` produces 28× the positive rate of `cpu_burn`. A run weighted the way this sample was
  trains mostly on memory exhaustion. Decide the fault mix on purpose.
- The 12.6-day window is all the incident history there is. There is no trailing-90-day record to
  count, because the harness has only been running since 2026-07-07.

**Still open:** `chaos_harness.csv` is a d=5 CSV the harness writes itself. Whether the scraperd
run's labels get joined from `fault_schedule.jsonl` timestamps or the harness keeps emitting its
own label column is a decision, not a measurement. It changes nothing about starting the run.

## Q2 — Population and disk

```
Hosts:                                        1 (this box) — confirm the fleet count before the run
Processes per host (after fix-step 04):       12   (was 36 before narrowing the pattern)
Rows/day = 86400 x 12 x 1:                    1,036,800
x 90 =                                        93,312,000 rows
Free disk on the output partition today:      367 GB free of 684 GB (44% used)
Measured one-day parquet size:                NOT YET MEASURED — needs a real day at 900s segments
x 90 =                                        pending
```

Bounds, until a real day is measured:

- Pessimistic ceiling: 46 B/row (the only measurement so far, taken with 5-second segments, which
  compress badly) → **~4.3 GB**. That is 4× the ~1 GB design target in [CLAUDE.md](CLAUDE.md).
- Real 900-second segments compress considerably better, so the true figure is below that. How far
  below is the thing to measure.

Either way 4.3 GB against 367 GB free is not a disk risk on this host. `max_disk_pct: 90` has 46
points of headroom. **The size question is about the design target, not about running out of
space** — if the run lands at 4 GB, that is a doc correction, not a blocker.

The real constraint is the one fix-step 06 names: 12 processes on 1 host is **one host's worth of
correlated observations**, not 93 M independent ones. Per-entity splits are leak-free at the host
boundary (invariant 5), and with one host there is no split. **Confirm the fleet size before the
run** — a single-host run cannot produce a held-out entity.

### Do this after 24 hours of real collection

```bash
.venv/bin/python check.py --config config.yaml     # section 4: the 90-day projection
df -h /var/lib
```

Write the measured one-day size into the table above. Until that number exists, the projection is
an extrapolation from a smoke run.
