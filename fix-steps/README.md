# Fix steps

Actionable fixes for [../issues/](../issues/). Ordered: code defects first (01–03, small and
independent), then the decisions that gate the run (04–07).

Each step states the file and line, the exact change, and a command that fails before the fix
and passes after. Do them top-down; nothing here depends on a later step.

| step | fixes | size | blocks launch? |
|---|---|---|---|
| [01](01-bound-the-request-loss-window.md) | [001](../issues/001-request-rows-lost-on-hard-kill.md) | 1 config line, or ~12 lines of code | no, but it silently costs data |
| [02](02-heartbeat-must-mean-writing.md) | [002](../issues/002-heartbeat-green-while-writes-refused.md) | 4 lines | no, but it hides the run dying |
| [03](03-config-error-not-traceback.md) | [003](../issues/003-empty-watch-key-crashes-startup.md) | 2 lines | no |
| [04](04-narrow-the-watch-patterns.md) | [B1](../issues/004-launch-blockers.md#b1--watchpatterns-over-matches-by-18) | config only | **yes** — 18× the rows and disk |
| [05](05-wire-sdk-hook-into-serving.md) | [B2](../issues/004-launch-blockers.md#b2--nothing-calls-sdk_hookrecord) | one call site in the SDK | **yes** — no d=5 rows without it |
| [06](06-answer-the-two-blocking-questions.md) | [B3](../issues/004-launch-blockers.md#b3--the-label-count-question-was-never-answered) | two queries, no code | **yes** — decides if the run is worth doing |
| [07](07-install-on-the-target-host.md) | [B4](../issues/004-launch-blockers.md#b4--no-gpu-on-this-host)–[B6](../issues/004-launch-blockers.md#b6--num_connections-is-structurally-0-for-containerised-targets) | install + config | **yes** — nothing is deployed |

## Minimum path to starting the run

06 → 04 → 05 → 07, then 01 and 02 before walking away from it for 90 days. 03 whenever.

Do **not** start the run before 07 is complete — without systemd `Restart=always` and the
heartbeat cron, the realistic failure is the daemon dying in week 3 and nobody noticing until
week 12, which is the failure the whole design is shaped around.
