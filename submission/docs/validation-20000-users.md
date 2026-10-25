# Distinct-user load validation — 2026-10-25

## Scope

The target is **20,000 distinct authenticated recipient users**, each with one
WebSocket, rather than 20,000 deliveries or devices sharing Bob's identity.
One additional seeded sender sends a 64-character message body through HTTP.
Recipients are divided among groups containing the sender and at most 99
recipients, preserving the 100-member limit. A single 20,000-recipient driver
creates 203 groups. The final four-driver workload creates 204 groups across
disjoint 5,000-user cohorts. This does not simulate 20,000 simultaneous writers.

All send rates are **aggregate logical sends/second across the workload, never
per user**. The connection-capacity stages offer 20 sends/s at 10,000 users and
40 sends/s at 20,000 users. The earlier 204-sends/s attempt at 10,000 users is a
separate, substantially heavier traffic workload and remains a failed result.

The one-minute four-driver run passed at 20,000 recipients and 40 aggregate
sends/s: all 235,248 expected measured deliveries arrived and all 2,800 acknowledged
warm-up/measured messages were recovered. Five HTTP repair retries occurred;
there were no send attempt failures, connection failures, reconnects, unacknowledged
messages or callback duplicates. Subsequent five-minute failures remain below.
The final clean five-minute repeat (`users-20k-soak-shared-repair-awake`) passed:
all 20,000 recipients remained connected, all 1,176,436 expected deliveries
arrived, and all 12,400 acknowledgements including warm-up passed durable audit.
There were no send errors, repair retries, connection retries, queue drops,
duplicate callbacks or detected clock discontinuities. Delivery p50/p95/p99
upper bounds were 86/327/803 ms. The final 20,000-user node-failure run also
passed: all 235,248 deliveries and 2,800 durable acknowledgements, with a pinned
client resuming elsewhere in 3.271 seconds. Outage retries are reported below.

## Environment and configuration

See [machine snapshot](results/users20k-environment.json) and the
[initial image/build record](results/users20k-build.json).

- Intel Core i5-8265U, four physical/eight logical CPUs; 16,516,628,480 host RAM bytes.
- Docker Desktop 28.5.2 / WSL2 kernel 6.6.87.2; eight logical CPUs and
  7,998,763,008 RAM bytes exposed to Docker. Compose v2.40.3.
- Go 1.26.2; three Go app processes, PostgreSQL 17, Redis 7 and Nginx 1.28.
- Shared development host. Unrelated Osai containers appear in resource samples;
  they were not stopped. These runs are not isolated server benchmarks.
- `compose.yaml` plus `compose.scale.yaml`: 12,000 socket slots, 500 handshake
  attempts/s and 512 conversation feeds per app; 32 SQL connections per app.
  The conservative default Compose profile retains its original lower limits.
- Scale proxy uses multiple workers and bounded upstream keep-alive connections.
  There is no automatic proxy retry of uncertain POST requests.
- Final driver configuration: four Linux containers, each with 65,536 file
  descriptors, `ip_local_port_range=1024 65535`, hard memory limit 1 GiB, Go soft
  memory limit 800 MiB and `GOMAXPROCS=2`. The proxy also uses the expanded port
  range for its outgoing connections. Earlier single-driver variants used a
  3-GiB hard / 2,500-MiB soft memory budget. The client uses small streaming socket buffers, a shared write
  buffer pool and 32 queued inbound message frames. Application outbound queues
  remain 128 events. Read limits and maximum message body size are unchanged.
- Each driver shares a bounded 256-connection HTTP transport across its recipients;
  each recipient still has its own authenticated WebSocket and independent
  cursor. This avoids giving each simulated user a permanent idle HTTP socket.
- The fallback HTTP timer is jittered between 15 and 30 seconds in scale runs.
  Final fixed-subscription clients negotiate shared server reconciliation and
  use HTTP for initial/reconnect/gap repair; older servers retain periodic HTTP
  fallback. The shared PostgreSQL sweep remains every second. The ordinary
  fallback default is one second. This differs from the original device tests.

## Reproduction

From `submission/` in PowerShell, with Docker running and at least 5 GiB free:

```powershell
./scripts/users-scale.ps1 -Users 300 -AggregateRate 2 -Prefix review-setup
./scripts/enable-query-metrics.ps1
./scripts/users-sharded.ps1 -Users 10000 -AggregateRate 20 -FixtureDirectory .artifacts/review-setup-fixtures -Prefix review-10k
./scripts/users-sharded.ps1 -Users 20000 -AggregateRate 40 -FixtureDirectory .artifacts/review-setup-fixtures -Prefix review-20k
./scripts/users-sharded.ps1 -Users 20000 -AggregateRate 40 -DurationSeconds 300 -RampMilliseconds 20 -FixtureDirectory .artifacts/review-setup-fixtures -Prefix review-20k-soak
./scripts/users-sharded.ps1 -Users 20000 -AggregateRate 40 -DurationSeconds 60 -RampMilliseconds 20 -FixtureDirectory .artifacts/review-setup-fixtures -Prefix review-20k-failure -KillNode
```

Use fresh prefixes. Reports reject overwrites. The scale script runs the current
mounted-source race suite, builds the Linux controller, creates deterministic local
mock identities, inserts missing users, builds the apps and runs stages in order.
A failed stage stops escalation and remains a failure. The optional
`-UseExistingAppImages` is appropriate only after verifying that those app images
match the current application source; it was used for later driver-only changes.

Run the remaining correctness checks from the same Windows PowerShell directory:

```powershell
go vet ./...
go test -tags compose -count=1 -v ./integration
go build -buildvcs=false -o .artifacts/exercise-final.exe ./cmd/exercise
./.artifacts/exercise-final.exe -mode failure -warmup 3s -duration 15s -rate 5 -out docs/results/review-dependencies.json
node --test scripts/aggregate-shards.test.mjs
./scripts/read-ready.test.ps1
node scripts/check-diagrams.mjs
```

The dependency scenario deliberately interrupts this project's app, Redis and
PostgreSQL services; run it after load testing, not concurrently. It restores the
services afterward. The final consolidated check record is
[users20k-final-checks.json](results/users20k-final-checks.json).

For another traffic rate, pass `-AggregateRate`; for another repair interval,
pass `-RecoveryInterval`. The default aggregate rate is two sends/s per thousand
requested recipient users, rounded up. This is not two sends/s per user.

`users-sharded.ps1 -KillNode` uses the same recovery client and groups. It pins one recipient
to app1, kills app1 twenty seconds after warm-up starts, and requires that recipient
to resume on another instance. It checks delivery and durable history for the
whole workload and restores app1 in `finally`. Only the `faultgen` test container
mounts Docker's control socket; the app containers and ordinary drivers do not.
The sharded variant uses `shardfault` for that one controller only. The preparation
step builds current application images; subsequent sharded runs reuse those images.

## Metric definitions and assertions

- Each send timer starts immediately before its first HTTP send attempt, after
  leaving the generator's bounded work queue. Recipient latency ends in the
  corresponding successful `OnMessage` callback. Sender acknowledgement latency
  ends after decoding the committed HTTP response. Retries keep the first start.
- Both endpoints of each delivery measurement run in the same Go process and use
  `time.Time` monotonic durations. PostgreSQL timestamps and host/container wall
  clock subtraction are not used for delivery latency. Wall timestamps identify
  runs; they do not determine the percentile samples. The four drivers wait at a
  filesystem barrier until every cohort has resumed. Cohorts use disjoint fixture
  index ranges. Aggregate validation requires overlapping measurement windows and,
  for healthy runs, all users maintained throughout each window. Cross-process
  wall timestamps are used only for this same-VM overlap check, not for latency.
- Aggregate percentiles merge latency histogram counts at 1-ms resolution. They
  are upper bounds with at most 1-ms rounding, not averages of shard percentiles.
  Histogram sample counts must equal received-delivery/acknowledgement counts.
- The standard stages have a ten-second warm-up, sixty-second offered-send
  window, eight-millisecond requested spacing between starts in each of four
  drivers and up to
  thirty seconds of delivery drain. Actual connection ramp and total run time
  can exceed those nominal durations on an overloaded host.
- Percentiles describe **received measured deliveries only**. Missing deliveries,
  scheduler queue drops, unacknowledged logical messages, HTTP attempt failures,
  HTTP recovery retries and connection failures are separate counters. Failed
  deliveries are not silently removed from the expected count or turned into a pass.
- Every distinct recipient must connect and receive a measured message. All
  expected deliveries, including scheduled sends dropped by the bounded sender
  queue, contribute to the failure assertions. Callback duplicates fail; overlap
  suppressed by the client's contiguous cursor is counted separately.
- Independent paginated HTTP history reads verify every acknowledged ID,
  conversation and sequence. An unavailable audit is **unverified**, even when
  its missing-row counter happens to be zero. Authorization errors remain fatal.
- The Linux driver intentionally has no Docker CLI. Its embedded Docker/SQL
  diagnostic fields report unavailable errors. Host-side scripts separately
  collect container CPU/memory and PostgreSQL statement counters. External rates
  use a monotonic stopwatch over the full controller run, including preparation,
  ramp, warm-up, delivery, teardown and audit; they are not steady-window rates.
- Docker resource values are sampled, not continuous peaks. CPU 100% represents
  one logical CPU. Generator startup and teardown can contribute to reported maxima.

## Retained failures and diagnosis

The earlier validation remains in [the shared-delivery report](validation-shared-delivery.md).
The following newer records are also retained:

- `review-20k-baseline-1000.json`: reused original device harness; all 1,000
  sessions initially resumed, then the Docker engine became unavailable.
  30,214 measured deliveries were missing and the final history audit failed.
  This is a failed run with an infrastructure interruption, not a clean
  application capacity comparison. Low disk space was observed, but its causal
  relationship to the engine loss was not established.
- `users20k-run2-*`: 300, 1,000 and 3,000 distinct-user stages passed at 8, 22 and
  62 aggregate sends/s. The 10,000-user / 204-sends/s stage failed: 9,209 scheduled
  sends were dropped by the bounded sender queue, 793,070 measured deliveries
  were missing, and all 5,071 acknowledged warm-up/measured messages were durable.
  All 10,000 sockets stayed resumed during that attempt. Socket count alone did
  not establish successful messaging throughput.
- `users20k-run3-*`: 10,000 at 20 aggregate sends/s passed. The 20,000 / 40 stage
  failed with 211,554 missing measured deliveries; all 978 acknowledged messages
  were recovered. The driver peaked at 766.12% sampled CPU and 1,668.096 MiB.
- `users20k-cpu-diagnostic.json` and [CPU profile](results/profiles/users20k-driver-cpu.pprof):
  a separate thirty-second offered-send diagnostic failed. The profile includes
  ramp and teardown and is not an independent passing capacity measurement.
  About 72% of sampled CPU was in TCP `connect` calls, while sampled Go cumulative
  GC fractions were around 4–5%. This did not support blaming GC as the main cause.
  See [top functions](results/users20k-cpu-profile-top.txt) and
  [cumulative functions](results/users20k-cpu-profile-cumulative.txt).
- `users20k-run4-*`: buffer/memory changes with the original ephemeral range;
  10,000 passed and 20,000 failed. The attempted supervisor stop arrived after
  natural completion; `users20k-run4-stop-request.json` records that no stop
  occurred. The complete failed delivery report is preserved.
- `users20k-run5-*`: widened driver range and corrected handshake backoff;
  10,000 passed and 20,000 failed. The final audit returned HTTP 503, so durable
  recovery from that particular run was not verified. Proxy logs showed upstream
  connection timeouts followed by all peers temporarily unavailable.
- `users20k-run6-*`: retaining healthy sockets across transient HTTP failures and
  a 30-second maximum HTTP repair interval still did not produce a passing
  single-driver 20,000-user run. It remains a failure.
- `users-sharded-10k-*`: four synchronized cohorts passed at 10,000 users / 20
  aggregate sends/s, with all 115,248 measured deliveries and all 1,400 acknowledged
  warm-up/measured messages recovered; no connection or send errors.
- `users-sharded-20k-*`: four cohorts initially resumed all 20,000 users, but
  failed with 77,922 missing measured deliveries, 798 unacknowledged logical
  messages and 34,482 reconnect retries. All 2,002 acknowledged messages were
  durable. Moving to multiple drivers alone did not solve the problem.
- `users-sharded-20k-proxy-ports-*`: same four-driver workload after expanding
  the proxy's outgoing port range passed. The previous failure is preserved.

## One-minute before/after comparison

| Users / global sends/s | Configuration | Result | Received / expected | Delivery p50 / p95 / p99 ms, 1-ms upper bounds | Full-run SQL statements/s |
| --- | --- | --- | --- | --- | --- |
| 10,000 / 20 | Four drivers, original proxy range | PASS | 115,248 / 115,248 | 92 / 369 / 629 | Not collected |
| 20,000 / 40 | Four drivers, original proxy range | FAIL | 157,326 / 235,248 | 29,817 / 77,978 / 88,674 | 1,650.85 |
| 20,000 / 40 | Four drivers, expanded proxy range | PASS | 235,248 / 235,248 | 109 / 2,639 / 5,006 | 2,398.59 |

The failed row's quantiles describe survivors, not the 77,922 missing deliveries.
The passing row sustained 20,000 resumed users across a 59.967-second common
measurement overlap, with 3,835.74 recipient deliveries/s including drain and
39.13 acknowledged logical messages/s including drain. Sender ACK p50/p95/p99
upper bounds were 32/723/1,531 ms. The shared host was not controlled for all
background activity, so this is measured evidence consistent with proxy port
pressure, not an isolated proof that no other resource influenced the difference.

The client previously discarded a healthy WebSocket whenever periodic HTTP
recovery returned a transient failure. That feedback could amplify an HTTP
overload into reconnect traffic. It now retains the socket, retries durable
repair on its timer and counts recovery failures. Gapped live events wait for
durable catch-up; callbacks still advance only contiguous positions. HTTP
401/403/404 and processing errors are not ignored. A regression checks an HTTP
503 followed by ordered recovery without another socket, and a 401 remains fatal.

The memory changes reduce reserved per-session buffers, but were not proven to
be the primary CPU fix. TCP range tuning and longer repair intervals alter the
test configuration. Results from different configurations must not be presented
as an isolated application-code speedup.

## Five-minute failures and recovery fixes

All rates below are **40 aggregate logical sends/second across four drivers**,
not per user. These failures remain historical evidence:

| Run prefix | Result | Received / expected deliveries | Conditional delivery p99 ms |
| --- | --- | --- | --- |
| `users-sharded-20k-soak` | FAIL | 872,035 / 1,176,436 | 121,193 |
| `users-20k-soak-diagnostics` | FAIL: connection continuity | 1,176,436 / 1,176,436 | 34,984 |
| `users-20k-soak-feed-retry` | FAIL | 726,539 / 1,176,436 | 198,720 |

The diagnostic run audited all 12,400 acknowledged messages (including warm-up)
successfully, but recorded 591 connection retries and a sum of cohort minimum
connections of 19,794. Delivering everything eventually does not satisfy the
healthy-run continuous-connection assertion. Diagnostics showed saturated SQL
pools and transient feed-refresh errors before disconnections. Feed refresh now
retains its last committed snapshot on transient database failures and retries
on the bounded periodic sweep. Missing/invalid authoritative state still closes
sessions. A regression holds the database pool unavailable and checks subsequent
delivery without replacing the socket.

The next run retained sessions during initial feed errors but subsequently
recorded lease-renewal errors and read timeouts. A failed lease renewal now
permits another attempt only until the last successfully established lease
deadline. It never extends that deadline on failure. Tests check both transient
recovery and socket expiry during persistent renewal failure. Neither fix changes
PostgreSQL commit-before-acknowledgement or permits delivery from partial reads.

`users-20k-soak-lease-grace` failed during startup, before any measurement
window. Two cohorts ultimately reported 5,000 users; the remaining cohorts were
canceled. The controller read readiness files while they were being created:
PowerShell accepts empty input as a null JSON result, which was incorrectly
treated as an incomplete cohort. `read-ready.ps1` now retries missing, empty and
partial reports until the existing deadline while rejecting a complete report
with an incorrect count or cohort identity. Its focused regression passed.
This is a harness defect diagnosis, not a passing load result. The canceled
startup remains recorded as FAIL. `users-20k-soak-readiness-fix` is the repeat
five-minute validation; consult its status and reports for its actual outcome.

That repeat (`users-20k-soak-readiness-fix`) also failed before measurement:
cohort zero reported 3,652/5,000 connections after its 30-second post-ramp wait.
This is a real incomplete-cohort report, distinct from the empty-file reader
bug. `users-20k-soak-startup-budget` explicitly allows two minutes after ramp
for readiness and five minutes for the controller startup deadline. All users
must still be resumed before the barrier, with the same five-minute measurement
and 40 aggregate sends/second. A longer startup allowance is a parameter change,
not evidence that the earlier startup succeeded.

`users-20k-soak-startup-budget` failed before measurement with 4,079/5,000
connections in cohort zero. Extending the wait alone did not fix startup churn.
The subsequent `users-20k-soak-gentle-ramp` used 20 ms between connection starts
per driver (nominally 200 starts/s globally), rather than 8 ms (500/s). It reached
20,000 resumed users but **failed** the five-minute measurement: 423,550 of
1,176,436 recipient deliveries arrived; 752,886 were missing, 6,974 measured
sends were unacknowledged and 6,487 were scheduler drops. There were 107,291
connection retries and 262,297 HTTP repair retries. All 5,426 acknowledged
messages including warm-up passed the PostgreSQL audit. Delivery p50/p95/p99
among received messages were 69,089/200,029/266,352 ms upper bounds; these
exclude missing deliveries and must not be interpreted as a successful SLO.
Full-generator-window SQL rate was 873.92 statements/s over 549.76 seconds;
the lower query rate reflects this failed workload, not an efficiency claim.
See the retained resource samples and generated resource summary for this run.

Privacy-safe lease diagnostics separated deadline, network and pool-timeout
errors; renewal errors included all three categories while SQL pools were also
saturated. The next change batches lease renewals in one worker per app, with
a queue bounded by configured connection capacity, at most 128 operations per
pipeline and a 10 ms collection wait. Each lease remains a separate atomic Lua
operation using Redis time. Only successful individual results extend the socket
deadline, measured conservatively from before enqueueing. Expired callers are
discarded before sending; caller waits and Redis operations are bounded. EVAL
avoids relying on a script cache surviving Redis restart. This trades a small
batch wait and repeated script bytes for fewer round trips and pool contenders.
Initial registration and removal remain direct operations. The run prefix
`users-20k-soak-batched-leases` identifies validation of this change; no pass is
claimed without its completed report.

That batched-renewal run failed: 834,421/1,176,436 deliveries, 342,015 missing,
2,358 unacknowledged measured sends, 2,344 scheduler drops, 1,710 connection
retries and 217,957 HTTP repair retries. All 10,042 acknowledged messages passed
durable audit. Received-only delivery p50/p95/p99 were 32,608/148,649/212,467 ms
upper bounds. Full-run SQL rate was 1,504.98/s over 512.20 seconds. Final counters
showed zero lease errors/read timeouts, but outbound queue overflows occurred.
Normalized database statement totals (cumulative across runs, not isolated
steady-state samples) showed millions of short authorized head and shared-feed
reads. Pool pressure and HTTP recovery retries remained substantial.

The next change explicitly negotiates shared durable reconciliation in the
`resumed` frame. A fixed-subscription client no longer repeats per-device HTTP
history polls while the server is already reconciling the same conversation
against PostgreSQL. Initial/reconnect history reads and gap repair remain;
servers omitting the capability retain HTTP periodic fallback. This changes
the recovery implementation, not offered sends, measurement duration or error
assertions. Tests cover missing hints repaired by the real server without extra
client history polls, explicit gap repair and legacy fallback. The first
`users-20k-soak-shared-repair` attempt failed preflight compilation in a new test
and never reached load. The corrected rerun is `users-20k-soak-shared-repair-v2`.

That rerun reached and maintained all 20,000 users with zero connection retries,
zero HTTP repair retries and 20,000 negotiated shared-reconciliation resumes.
It nevertheless failed: 1,103,714/1,176,436 deliveries, 72,722 missing, 822
unacknowledged measured sends and 159 scheduler drops. All 11,578 acknowledged
messages passed the audit. Received-only delivery p50/p95/p99 were
225/4,588/14,656 ms upper bounds. This is **not a clean performance baseline**:
resource timestamps have a 3,277.086-second gap, and Windows power events record
a sleep/resume transition during the run. Wall and reported monotonic windows
are inconsistent. See `users-20k-shared-repair-clock-disturbance.json`; the
evidence does not isolate the exact contribution of host sleep versus VM clock
adjustment to failures. The run stays FAIL and is not silently excluded.

`users-20k-soak-shared-repair-awake` repeats the same application/workload with
temporary controller-thread idle-sleep prevention. It changes no persistent
Windows power setting and cannot prevent manual sleep. The harness now compares
wall and monotonic measurement/drain durations, rejecting a discrepancy above
one second. The aggregator independently rejects a flagged discontinuity; its
five regression tests passed. Earlier reports do not contain this clock check.

## Final clean five-minute comparison

Both rows use 20,000 recipients, 40 aggregate sends/s, four drivers, a 20 ms
connection ramp per driver, 10-second warm-up and 300-second measurement.
The before row includes lease batching but still has redundant per-device HTTP
polls. The after row negotiates shared PostgreSQL reconciliation. All failures
and missing deliveries remain part of the report.

| Metric | Before: batched-leases (FAIL) | After: shared-repair-awake (PASS) |
| --- | --- | --- |
| Received / expected deliveries | 834,421 / 1,176,436 | 1,176,436 / 1,176,436 |
| Delivery p50 / p95 / p99 upper ms | 32,608 / 148,649 / 212,467, received only | 86 / 327 / 803 |
| Sender ACK p50 / p95 / p99 upper ms | 995 / 6,306 / 10,587 | 15 / 125 / 276 |
| Missing deliveries | 342,015 | 0 |
| Unacknowledged measured sends / scheduler drops | 2,358 / 2,344 | 0 / 0 |
| HTTP send errors / repair retries | 801 / 217,957 | 0 / 0 |
| Connection failures / retries | 122 / 1,710 | 0 / 0 |
| Minimum resumed users, sum of cohort minima | 19,801 | 20,000 |
| ACKs checked durably, including warm-up | 10,042; zero missing | 12,400; zero missing |
| Recipient deliveries/s, including drain | 2,324.21 | 3,921.37 |
| Full-run SQL statements/s | 1,504.98 over 512.20 s | 1,397.84 over 438.34 s |
| Same-sample apps CPU peak / RAM MiB | 337.37% / 1,549.5 | 324.10% / 1,473.5 |
| Same-sample drivers CPU peak / RAM MiB | 349.90% / 1,000.5 | 194.74% / 924.6 |
| PostgreSQL CPU peak / RAM MiB | 259.88% / 444.9 | 260.76% / 332.4 |
| Redis CPU peak / RAM MiB | 93.85% / 21.54 | 41.89% / 25.23 |
| Proxy CPU peak / RAM MiB | 227.09% / 644.5 | 102.89% / 617.5 |

100% CPU means one logical CPU. Resource maxima include startup/teardown and
are sampled, not continuous. Group maxima sum simultaneous samples; independent
component maxima are not simultaneous totals. SQL windows include ramp, warm-up,
measurement, drain and audit, so rates are not isolated steady-state query costs.
The failed run completed less work and cannot establish an efficiency baseline
per delivered message. The successful run's final diagnostics show zero SQL pool
waits, feed errors and lease errors during measurement. The host and persistent
database were shared with prior runs; these are local observations, not a
controlled production capacity experiment. The after run has 1,176,436 delivery
latency samples and 12,000 measured ACK latency samples; histograms use 1 ms upper
bounds. Each shard's clock difference was under one second (about 21 ms), with
299.998 seconds of common measurement overlap. All 20,000 resumes negotiated the
shared-reconciliation capability.

## Final failure recovery and correctness checks

`users-20k-failure-shared-repair-v2` PASS: 20,000 users at 40 aggregate sends/s,
10-second warm-up and 60-second measurement. The controller killed app1 after
20 seconds from warm-up start, after verifying the selected client was pinned
there. All 235,248 measured deliveries arrived, all 2,800 acknowledged messages
(including warm-up) passed durable audit, with no duplicate callbacks, remaining
unacknowledged sends, scheduler drops or clock discontinuities. The selected
client resumed on another instance in 3,271.402 ms; this is not every user's
recovery time. Samples subsequently showed all 20,000 sessions on two survivors.

The outage produced 42 HTTP send-attempt errors, 1,977 HTTP repair retries,
7,435 failed WebSocket establishment attempts and 14,081 connection retries.
These are retained, not reported as zero errors. Minimum resumed count was
13,505 (sum of cohort minima). Delivery p50/p95/p99 upper bounds were
120/9,160/14,893 ms, maximum 16,993 ms; sender ACK bounds were 24/2,298/6,044 ms.
There were 5,672 suppressed duplicates. Full-generator-window SQL rate was
1,387.85 statements/s over 196.47 seconds. The script restored app1 and ingress.

The first fault attempt was deliberately stopped before its barrier after finding
legacy Bob send selection would violate distinct-group membership. Its controller
FAIL and separate `ABORTED_HARNESS_STARTUP` stop record remain. The correction
uses the shared authorized sender for distinct groups, preserving the two-sender
legacy scenario. Its regression is included in the final 56 race-enabled tests:
all passed, with zero failed/skipped tests in the final preflight log.

`users20k-final-compose-tests.log`: three separate-process Compose tests passed.
`go vet ./...` passed. The first native harness build failed Git VCS stamping
due to checkout ownership, then `go build -buildvcs=false` succeeded.
`users20k-final-dependency-failure.json` passed all 24 assertions, including lost
ack retry, conflicting reuse, ordered durable history, multi-device presence,
lease expiry, Redis outage/outbox drain, missed receipts and PostgreSQL outage.
Its active exchange checked all 90 acknowledged messages against durable history.
All three Archify diagrams were regenerated and passed four native gates plus
static-export/link checks; source correspondence remains manual working-file
review with hashes, not Git-pinned verification.

The final higher-rate 204-sends/s workload was not rerun; its earlier failures
remain unresolved at that offered rate. No longer-than-five-minute clean soak,
20,000-writer workload, multi-host test or dependency-HA test is claimed. The
20,000-user fault scenario is distinct from the smaller multi-device/dependency
scenario; no multi-device presence-at-20,000 assertion is implied.

## Reliability boundary and remaining limits

Application-node recovery is distinct from dependency high availability.
PostgreSQL, Redis, ingress and the Docker host remain single points of failure.
Durable send acknowledgement still follows PostgreSQL commit; cache hints are
not acknowledgement evidence. Database failover would require tested durable
replication, backup/restore and an explicit acknowledgement/failover policy.
Redis failover also needs defined lease and subscription-recovery behavior.

These small-message, bounded-group workloads do not establish production capacity,
20,000 simultaneous writers, Internet/TLS behavior, or arbitrary hot-conversation
distributions. Long-duration reliability and multi-host/database/cache failover
need separate tests. At 10x users, configured socket/feed caps require additional
replicas or revised bounds, with measured memory, lease and egress capacity.
At 100x, connection routing, conversation placement, database/outbox partitioning
and failure recovery need a separately tested design. Reconnect/gap HTTP traffic
still scales with affected devices, although healthy fixed subscriptions share
durable reconciliation. More app replicas
alone do not remove those limits. No untested scale is presented as verified.
