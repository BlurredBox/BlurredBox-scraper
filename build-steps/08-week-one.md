# 08 — Week one: the questions that decide whether the run is worth doing

The code is done and running. These are the three things
[file-layout.md](../file-layout.md) §5 says nobody has checked, and they can each invalidate
90 days of collection. None of them is a coding task.

## 1. Count the labels — before week two

The training label is joined from incident records: 1 at the failure step, else 0. Nothing in
this repo produces it.

```
Failure events in the trailing 90 days: ____
Query / source: ____
```

Interpretation:

| count | what it means |
|---|---|
| 0–9 | **Stop and redesign.** 7.8M rows against a handful of positives is a class-imbalance problem, not a training set, and clean parquet does not fix it. Options: deliberate fault injection, a broader definition of "failure" (OOM-kill, restart, SLO breach — not just full outage), or a different target variable entirely (latency regression, saturation onset) |
| 10–100 | Marginal. Workable with a broader failure definition or heavy resampling. Decide *now* which |
| 100+ | Proceed |

Do the count against real incident records, not an estimate. It is the single question that
decides whether the collection is worth doing at all, and none of the three design documents
asked it.

## 2. Size the population against real disk

```
Hosts: ____   Processes per host: ____
Rows/day  = 86400 × processes × hosts = ____
Measured one-day parquet (step 07 §4) = ____ MB
× 90 = ____ GB      free on the partition today = ____ GB
```

Two VMs and six processes is a small-data problem regardless of how many rows it produces —
row count is not sample size when the rows are 1 Hz samples of six processes. If that is the
population, say so out loud before month 2.

## 3. Confirm the data is what you think

Re-run [07-verify-the-data.md](07-verify-the-data.md) at day 1, day 7, and monthly. The failure
this catches is a column that has been silently zero since the start.

## Then leave it alone

The run is 90 days. The temptation between now and then is to build something from the
deferred table. Each entry has a trigger; check the trigger fired first
([file-layout.md](../file-layout.md) §6, [CLAUDE.md](../CLAUDE.md)):

| you might want to build | only after |
|---|---|
| SQLite write buffer | you actually lost data you needed, or `flush_seconds` must drop below ~10 |
| a `drift_stats` collector | a consumer reads it — Welford and PSI compute after the fact from the parquet you already have |
| `model_perf` / `fairness` / `policy_signals` / `action_taker_io` | that bus emits real traffic. Writing a collector against a stream with no producer is the most expensive code there is, because nothing tells you it is wrong |
| ZeroMQ / `sink.py` | a live consumer exists. The 3-month deliverable is the parquet tree itself |
| the C probe | profiling shows psutil perturbs the measurement. At 1 Hz it will not |
| a projection module | a third consumer of the data exists |
| `signing.py` | scraperd produces a signature rather than recording one |

What is worth doing in the meantime: nothing in this repo. Watch the heartbeat alert, answer
question 1, and re-run the step-07 checks.

## Done when

- [ ] `ANSWERS.md` Q1 answered from real incident records, with the decision it implies
- [ ] `ANSWERS.md` Q2 answered from a measured day, not an estimate
- [ ] step 07 re-run at day 7, all boxes ticked
- [ ] the heartbeat alert has fired at least once in a test and reached a human

## The finished repo

```
BlurredBox-scraper/
├── pyproject.toml              4 runtime deps, 3 dev
├── config.yaml
├── scraperd/
│   ├── __init__.py             version only
│   ├── schema.py               CORE_V1, the field tuples, core_row()
│   ├── collect.py              1 Hz psutil sampling, host metrics
│   ├── sdk_hook.py             per-request spool: record() / drain()
│   ├── writer.py               parquet segments, heartbeat, disk guard
│   └── daemon.py               asyncio loop, manifest, SIGTERM flush
├── deploy/
│   ├── scraperd.service
│   └── heartbeat-check.sh
├── tests/
│   ├── test_schema.py          13 tests
│   └── test_writer.py          8 tests
└── build-steps/                these documents
```

```bash
.venv/bin/pytest -q                    # 21 passed
systemctl is-active scraperd           # active
```
