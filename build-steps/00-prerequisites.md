# 00 — Prerequisites

Toolchain check, branch, and the three questions from [file-layout.md](../file-layout.md) §5.
Twenty minutes. Two of the three questions do not block the code — they block the *run*.

## Do

### 1. Confirm the toolchain

```bash
python3 --version          # need >= 3.11 (the code uses `X | Y` type syntax)
git branch --show-current  # need: build
```

If you are not on `build`:

```bash
git checkout build
```

### 2. Answer question 3 now — it is already answered

file-layout.md §4.1 says to confirm `sdk/blurredbox_agd/metrics.py` emits all five d=5
fields in one snapshot. It does not. Verified by inspection of the main repo:

| what | where | verdict |
|---|---|---|
| `get_process_metrics()` | `sdk/blurredbox_agd/metrics.py:103-140` | emits `cpu_usage`, `memory_usage_mb`, `gpu_usage`, `gpu_vram_mb` — **four of five, no `latency_ms`** |
| `_RequestContext.__exit__` | `sdk/blurredbox_agd/tracking.py:86-93` | has the real per-request wall-clock, `(perf_counter() - t0) * 1000` — **but no resource snapshot** |
| `TelemetryDaemon._collect()` | `sdk/blurredbox_agd/daemon.py:73-110` | puts all five in one dict, but `latency` comes from `self._latency_provider()` — an aggregate the caller supplies, defaulting to `0.0`. **Not this request's latency.** |

So the five co-occur nowhere as a genuine single-request snapshot. The hook belongs in
`_RequestContext.__exit__`, which already owns the true latency — see
[06-sdk-hook.md](06-sdk-hook.md) for the exact change. Steps 1–5 do not depend on it.

### 3. Log the two questions that do block the run

Create `build-steps/ANSWERS.md` and fill it in before step 5. Both are counting exercises,
not design work, and both can invalidate the whole run.

```markdown
# Pre-run answers

## Q1 — Label count  (file-layout.md §5.2)
Failure events in the trailing 90 days of incident records: ____
Source queried: ____
If single digits: the run needs fault injection, a broader failure definition, or a
different target variable. 7.8M rows and twelve positives is not a training set.

## Q2 — Population  (file-layout.md §5.3)
Hosts: ____   Processes per host: ____
Rows/day = 86400 x processes x hosts = ____
Free disk on the output partition today (`df -h /var/lib`): ____
Measured one-day parquet size after step 4, x 90 = ____
```

Q1 is the one that decides whether collecting is worth doing at all. Ask it before
step 5, not in month 2.

## Verify

```bash
python3 --version && git branch --show-current && ls build-steps/ANSWERS.md
```

## Done when

- [ ] Python ≥ 3.11
- [ ] on branch `build`
- [ ] `build-steps/ANSWERS.md` exists, Q1 and Q2 written down (values can still be blank)
- [ ] you have read the six decisions in [README.md](README.md), especially D1

→ [01-bootstrap.md](01-bootstrap.md)
