# Validation artifact index

All workload send rates are **aggregate logical sends/second, never per device**.
This also applies to older JSON without a `send_rate_scope` field. At rate 2,
300 recipient devices expect 600 device deliveries/s, not 600 logical sends/s.

- `load-*`, `failure-*`, `summary.json`: original validation, including failed
  stages. See `../validation-2026-10-23.md`. These raw reports remain unchanged.
- `optimization-baseline-tests.log`: 26 existing race-enabled tests passed before
  dispatcher changes. `optimization-before-query-profile.txt` is a cumulative
  query profile, not a per-stage statement rate.
- `before-shared-*`: exploratory instrumented original-app measurements. The
  wall-time-derived query rate in `before-shared-100-rate10.json` is invalid due
  to a host wall-clock step; it must not be used. Recipient durations were
  monotonic. All stages were repeated after fixing the counter denominator.
- `before-shared-v2-*`: original app, monotonic query-rate window, original
  generator HTTP pooling. `after-shared-*`: first shared-feed build; the
  1,000-device stage and its diagnostic repeat failed. Pool creation snapshots
  bracket that diagnostic repeat. `shared-initial-comparison.json` pairs those
  initial shared-feed results with v2 baseline stages.
- `after-shared-v2-*`: shared feeds plus bounded idle database-pool retention,
  original generator pooling. The 1,000-device stage still failed and the
  300-device p99 regressed; `shared-pool-comparison.json` retains both facts.
- `before-shared-v3-*` and `after-shared-v3-*`: matching generator with owned,
  bounded HTTP pools per device. Use this pair for the final app comparison.
  `baseline-retained-image.json` identifies the actual original app image used.
  `shared-comparison.json` contains the derived final comparison. The 1,000-device
  attempt failed on both builds; no report is relabeled a pass. Both v3 failure
  scenarios passed 24 assertions. The six `audit-*-v3-*-final.json` files audit
  acknowledged IDs again after the final app restart.
- `environment-shared.json`: machine/runtime snapshot for the optimization.
  Resource samples and before/after PostgreSQL/Redis diagnostics are embedded in
  each raw workload report. They are sampled evidence, not continuous profiling.

JSON `passed` and command exit status determine pass/fail; a saved file alone does
not imply success. Failed/missing deliveries remain in expected counts, and latency
quantiles describe received samples only. Counter rates include transaction
commands and background polling over the full recorded monotonic counter window,
not just the nominal recipient measurement interval. `../harness.md` defines all
metrics and commands; `../validation-shared-delivery.md` records final outcomes.

PowerShell may format Docker's normal stderr progress as `NativeCommandError` in
redirected logs. That formatting is not a failing Go test. Test PASS/FAIL/SKIP
lines and checked process exit statuses are reported separately.
