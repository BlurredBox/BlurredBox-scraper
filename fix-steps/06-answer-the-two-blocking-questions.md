# 06 — Answer the two blocking questions

Fixes [B3](../issues/004-launch-blockers.md#b3--the-label-count-question-was-never-answered).
No code. Do this **before** starting the run — both answers can change the design, and neither
is recoverable after 90 days have passed.

These lived in `build-steps/ANSWERS.md`, deleted in `c0c203f` with both blank. Record the
answers below and keep the file.

---

## Q1 — Label count

The training label is joined from incident records: 1 at the failure step, else 0. Count real
failures in the **trailing 90 days** of incident records:

```
Failure events in the last 90 days:  ____
Source queried:                      ____
Definition of "failure" used:        ____
```

**If single digits:** the run as designed produces ~280 M rows and a dozen positives. That is a
class-imbalance problem, not a training set, and no amount of collection fixes it. The run needs
one of:

- **fault injection** — deliberately induce failures on a schedule, so positives are by
  construction rather than by luck
- **a broader failure definition** — degradation, latency SLO breach, OOM-kill, restart, not
  only hard outage
- **a different target variable** — predict `latency_ms` or a resource ceiling directly, which
  every row carries, instead of a rare binary event

The host already runs `entrypoint.chaos-entity.sh` processes, which suggests chaos tooling is
present. If so, fault injection is the cheapest of the three.

## Q2 — Population and disk

```
Hosts:                                        ____
Processes per host (after fix-step 04):       ____
Rows/day = 86400 x processes x hosts:         ____
Free disk on the output partition today:      ____   (df -h /var/lib)
Measured one-day parquet size:                ____   (check.py section 4, after a real day)
x 90 =                                        ____
```

Measured, not projected from a 14-second smoke run. The one measurement so far used 5-second
segments at 46 B/row compressed; real 900-second segments compress considerably better, so the
smoke number is a pessimistic ceiling, not an estimate.

Two VMs and six processes is a small-data problem regardless of how many rows come out of them
— 280 M rows from 36 processes on one host is not 280 M independent observations. Write the
number down and multiply against actual free disk in week 1, not week 12.

---

## Why this is a gate and not a formality

Everything else on the list is work that can be done in any order and fixed afterwards. These
two decide whether the dataset is worth collecting at all. A run that starts before they are
answered spends 90 days and ~1 GB to find out the answer was "no".
