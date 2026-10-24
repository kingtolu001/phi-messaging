# Shared delivery optimization validation — 2026-10-23

All rates in this report and its workload JSON are **aggregate logical sends per
second across the workload, not per device**. Rate 2 with 300 recipient devices
offers 600 device deliveries/second; rate 2 with 1,000 offers 2,000. The higher-rate
stage is 100 devices at 10 aggregate sends/second, or 1,000 offered deliveries/s.
This is one shared conversation with many devices belonging to Bob, not hundreds
of distinct users or independently active conversations.

## Evidence and implementation choice

Before changing delivery, the existing 26 race-enabled tests passed against real
dependencies (`results/optimization-baseline-tests.log`). The running original
app images were retained while measuring a fresh baseline. Inspection showed:

- Each WebSocket session queried a repeatable-read history transaction and
  receipts on every one-second timer and hint. Every hint woke all local sessions.
- The real client independently recovered over HTTP every second, repeating the
  same history transaction even when already current.
- The saved pre-change query profile (`results/optimization-before-query-profile.txt`)
  contains 133,121 message-history SELECT calls, 136,426 head/membership SELECTs,
  82,634 receipt SELECTs and 133,303 read-transaction BEGINs. These are cumulative
  observations across exploratory stages, not one stage's rate. The send row-lock
  query accumulated substantial execution/wait time too; it remains serialized.
- Fresh comparable baseline rates were approximately 4,950 statements/s at 300
  devices and 5,573 at 1,000. At 300, sampled PostgreSQL CPU reached 412.05%.
  Counts and CPU support eliminating repeated reads; neither proves every delay
  was caused by polling or excludes host contention.

The implementation shares a committed message tail and receipt snapshot per
subscribed conversation per app node. Hints target only that feed. A one-second
durable sweep remains independent of Redis. Four workers coalesce refresh work;
128 queued jobs, 128 feeds and 128 cached messages/feed bound resources. Last
subscriber departure removes the feed. Cache coverage is validated before use;
older backlogs fall back to authorized PostgreSQL history. HTTP always checks
current membership/head in PostgreSQL, including on cache hits. The client's
one-second HTTP recovery cadence was deliberately unchanged for comparability.

Send/read transactions, outbox publication, message IDs, client ordering/dedup,
leases, socket queues and write deadlines retain their durability and failure
contracts. Shared state is internal dispatcher input, never an unchecked public
history API. Observing head H and then reading immutable messages through H needs
no per-session read transaction because head/message/outbox commit atomically.

### Intermediate failure and follow-up

The first shared-feed build improved the 300-device and higher-rate workloads,
but **failed** at 1,000: 23,413 missing measured deliveries, 98 HTTP attempt errors,
6,582 WebSocket establishment failures and 26 unacknowledged logical sends across
all phases. All 24 acknowledged IDs remained durable; one further send was found
committed without acknowledgement. The diagnostic repeat also **failed**, missing
1,399 deliveries and recording 5,374 establishment failures. These reports remain
`after-shared-1000-rate2.json` and `after-shared-diagnose-1000-rate2.json`.

Counter snapshots around the diagnostic repeat (`shared-pool-before.txt` and
`shared-pool-after.txt`) recorded 19,719 → 21,279 PostgreSQL sessions: 1,560 newly
created sessions across the run and surrounding diagnostics. The pool allowed 32
open connections but retained only eight idle. Fast synchronized poll bursts
could discard extra idle backends and recreate them repeatedly; backend creation
is not counted as an application SELECT. Sampled CPU in the first failed run
reached 693.07% despite only about 515 completed statements/s. Timeout/reconnect
feedback further reduced useful work. This supports pool churn as an additional
bottleneck, without attributing every failure to it.

The app now retains up to 32 idle connections within the same 32-open limit
per app. Maximum SQL concurrency was not increased. It trades idle backend memory
for reuse. Three app pools can occupy 96 connections, close to PostgreSQL's default
100; more nodes require an explicit connection budget/pooler. Initial shared-feed
results and the diagnostic failure remain in `shared-initial-comparison.json` and
their raw reports.

Pool retention reduced new database sessions to 49 in the following 1,000-device
run, but that run still **failed**: 15,585 missing deliveries, 7,068 establishment
failures and one HTTP attempt error. All 50 acknowledgements remained durable.
The pool-adjusted 300-device run passed but its p99 regressed to 4,254.431 ms;
this is retained too (`after-shared-v2-*`). Pool retention addressed backend churn,
not every bottleneck or timeout.

Inspection then found that all simulated devices shared Go's default HTTP
transport and its two idle connections per host. A native TCP snapshot observed
8,169 sockets in TIME_WAIT involving localhost:8080 and 8,281 total; the dynamic
TCP range contains 16,384 ports. This is evidence of connection churn, not proof
that every timeout exhausted ports. The client now owns a bounded HTTP transport
per device: at most two active/idle connections, 30 s idle expiry, deferred idle
cleanup on shutdown and the same five-second request timeout. Caller-provided
clients remain caller-owned. A behavioral regression checks three polling rounds
from 20 clients use exactly 20 connections and release them at shutdown.

To compare fairly, all three stages were repeated for **both** original and
optimized app images with this same corrected generator (`before-shared-v3-*`
and `after-shared-v3-*`). Its JSON records `device_http_pool`. The original image
is the locally retained pre-dispatcher build, with digest/config recorded in
`results/baseline-retained-image.json`; it is not reconstructed from memory.
Final comparison must use these matching v3 stages rather than selecting the
best individual measurements across builds/generators.

## Environment and measurement

Actual environment is captured in `results/environment-shared.json`: Windows
amd64, Go 1.26.2, Intel i5-8265U (4 physical/8 logical cores), 16,516,628,480 bytes
RAM. Docker Desktop 28.5.2/Compose 2.40.3 runs Linux amd64 WSL2 with eight logical
CPUs and 7,998,763,008 bytes RAM. PostgreSQL 17, Redis 7 and Nginx 1.28 run beside
three application processes. Unrelated osai containers remained running. CPUs
were not isolated; the native generator was not separately profiled. Short local
diagnostics and sampled resource peaks are not production capacity/SLA proof.

Every comparable stage uses 5 s warm-up, 20 s measurement, 10 ms connection-start
interval, up to 30 s connection settling, 15 s delivery drain, 64-character bodies,
16 send workers and a 128-job generator queue. Each stage creates a fresh group.
At aggregate rate 2 there are 10 warm-up and 40 measured sends; at rate 10 there
are 50 warm-up and 200 measured sends. The scheduler never silently drops offered
slots, and failed/unacknowledged sends still contribute expected deliveries.

Delivery latency starts just before the first HTTP send attempt and ends at the
recipient's first ordered callback, including retries/recovery. It excludes prior
scheduler/worker-queue waiting, which is reported separately. Sender acknowledgement
latency is separate. Both endpoints execute in one native process and use Go's
monotonic clock; server/DB clocks are not subtracted. Quantiles use received
samples only: missing deliveries prevent claiming a finite full-workload p99.
Throughput uses actual measurement-plus-drain duration, not merely the nominal 20 s.
See `harness.md` for full definitions and all raw error counters.

Query rate is a difference of `pg_stat_statements` calls for database `phi`,
including transaction commands and background outbox work. Its separately saved
monotonic window spans ramp through cleanup/diagnostics, before the history audit;
it is longer than the recipient measurement interval. Instrumentation overhead
is included. Counters were not reset between stages. Missing/decreased counters
yield unavailable, not zero. Docker CPU/memory peaks are maxima of discrete
samples, not continuous maxima or averages; app CPU is the peak simultaneous sum
of the three app samples. Raw samples retain network and block I/O counters too.

An exploratory baseline (`before-shared-100-rate10.json`) crossed a two-hour host
wall-clock step. Its old wall-time-derived 16.568 statements/s is **invalid** and
excluded from comparison. Delivery durations already used monotonic time. The
counter denominator was fixed and regression-tested, then **all three baseline
stages** were repeated as `before-shared-v2-*` before deploying delivery changes.
Other exploratory and original historical reports remain unchanged.

## Reproduction and retained history

From `submission/`, exact final-build commands are:

```powershell
docker compose up --build -d
./scripts/enable-query-metrics.ps1
# If rebuilding replaces replica IPs, refresh Nginx's resolved upstreams:
docker compose restart load-balancer
# Release idle benchmark pools before fixtures need additional DB connections:
docker compose restart app1 app2 app3
docker compose --profile test run --build --rm tests
go test -tags compose -count=1 -timeout=90s -v ./integration
go vet ./...
go build -o .artifacts/exercise.exe ./cmd/exercise
./.artifacts/exercise.exe -mode failure -warmup 3s -duration 15s -rate 5 -out docs/results/review-failure.json
./scripts/delivery-stages.ps1 -Prefix review-final
./scripts/compare-delivery.ps1 -Before before-shared-v3 -After review-final -Output docs/results/review-final-comparison.json
```

Use unused output names. Reports are never overwritten. Saved original
`load-*.json`, `failure-*.json`, the historical `summary.json` and
`validation-2026-10-23.md` retain all original failures. Their send rates have always
been aggregate; older JSON simply lacks the newly explicit `send_rate_scope` field.
The v2 higher-rate baseline passed with 11 HTTP retries, unlike the original
historical failed run. That variability is reported rather than relabeling the
old failure or using it as the only before value. The matched v3 baseline failed
at the higher rate too; it is the before value in the final tables.

The baseline used local image
`sha256:05390543cda03b71e89185adc5d89319755158d52359c4b5e3eece3b0d72070f`.
To repeat it while that image is retained, create a Compose override:

```powershell
$baseline = (Get-Content docs/results/baseline-retained-image.json -Raw | ConvertFrom-Json).Id
docker image inspect $baseline
# Stop if that inspection fails; the old image is not published by this exercise.
@{services=@{app1=@{image=$baseline};app2=@{image=$baseline};app3=@{image=$baseline}}} | ConvertTo-Json -Depth 5 | Set-Content .artifacts/baseline-compose.json
docker compose -f compose.yaml -f .artifacts/baseline-compose.json up --no-build -d app1 app2 app3
docker compose restart load-balancer
./scripts/delivery-stages.ps1 -Prefix review-before
# Independent failure check; run even if the saved load stages failed:
./.artifacts/exercise.exe -mode failure -warmup 3s -duration 15s -rate 5 -out docs/results/review-before-failure.json
# Restore optimized images without changing data volumes:
docker compose up --no-build -d app1 app2 app3
docker compose restart load-balancer
```

The actual v3 execution order was original-image 300/1,000/higher-rate loads,
original-image failure check, restore optimized images, Compose checks,
optimized-image failure check, then optimized 300/1,000/higher-rate loads.
No compile/test workload ran alongside a load stage. The retained baseline image
is local evidence, not a portable published image; if pruned, those exact original
binary reruns are blocked until that artifact is restored. Saved results remain.

## Matched before/after load results

Raw v3 reports and `results/shared-comparison.json` are the source of these values.
All latencies are recipient delivery milliseconds; send rates are aggregate.

| Devices / aggregate sends/s | Build | Result | Received / expected | p50 | p95 | p99 | Statements/s (full run) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 300 / 2 | Original | PASS | 12,000 / 12,000 | 755.668 | 1,675.023 | 2,060.773 | 4,116.50 |
| 300 / 2 | Shared | PASS | 12,000 / 12,000 | 139.442 | 256.220 | 312.148 | 443.69 |
| 1,000 / 2 | Original | **FAIL** | 36,579 / 40,000 | 6,185.412 | 17,106.617 | 24,506.873 | 4,235.19 |
| 1,000 / 2 | Shared | **FAIL** | 27,311 / 40,000 | 1,933.185 | 27,112.406 | 32,112.916 | 708.49 |
| 100 / 10 | Original | **FAIL** | 15,700 / 20,000 | 4,497.854 | 13,377.708 | 15,439.433 | 1,674.41 |
| 100 / 10 | Shared | PASS | 20,000 / 20,000 | 80.027 | 143.918 | 183.155 | 485.37 |

At 300 devices, statements fell about 89% with the same maintained connection
count, and delivery p99 improved about 85%. The higher-rate stage now passed in
this short run. **The 1,000-device regression remains unresolved**: fewer completed
deliveries, worse p95/p99 and more HTTP errors. A lower statement rate in that failed
stage is not proof of handling an equivalent successful workload; connection
counts and completed sends differed. Failed-stage quantiles omit missing samples.

| Devices / aggregate sends/s | Build | Min/peak accepted sessions | WS establishment failures / retries | HTTP attempt errors | Unacknowledged sends (all phases) | Missing measured / warm-up | Generator queue drops |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 300 / 2 | Original | 300 / 300 | 0 / 0 | 0 | 0 | 0 / 0 | 0 |
| 300 / 2 | Shared | 300 / 300 | 0 / 0 | 0 | 0 | 0 / 0 | 0 |
| 1,000 / 2 | Original | 680 / 999 | 59 / 1,802 | 5 | 0 | 3,421 / 65 | 0 |
| 1,000 / 2 | Shared | 624 / 966 | 210 / 977 | 50 | 14 | 12,689 / 5,595 | 0 |
| 100 / 10 | Original | 100 / 100 | 0 / 0 | 142 | 46 | 4,300 / 0 | 30 |
| 100 / 10 | Shared | 100 / 100 | 0 / 0 | 0 | 0 | 0 / 0 | 0 |

All six runs had zero duplicate callbacks, terminal client errors or unscheduled
slots. Expected deliveries include failed/queue-dropped sends. The shared
1,000-device run started measurement with only 624 sessions after exhausting the
30-second settling deadline. Its retries included 18 HTTP 429 admission responses
and 959 status-zero transport/protocol failures; no claim isolates their exact
cause. The original 1,000-device run did not maintain all requested sessions either.

| Devices / aggregate sends/s | Build | Measured ack samples | Ack p99 (ms) | Completed acks/s | Completed recipient deliveries/s | Counter window (s) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 300 / 2 | Original | 40 | 1,688.879 | 1.922 | 576.747 | 33.604 |
| 300 / 2 | Shared | 40 | 181.168 | 2.000 | 599.970 | 35.306 |
| 1,000 / 2 | Original | 40 | 11,605.661 | 1.035 | 946.501 | 88.934 |
| 1,000 / 2 | Shared | 31 | 21,012.737 | 0.673 | 593.177 | 103.839 |
| 100 / 10 | Original | 154 | 14,430.699 | 1.956 | 199.401 | 90.791 |
| 100 / 10 | Shared | 200 | 104.481 | 9.949 | 994.915 | 30.898 |

Delivery sample counts equal received deliveries in the first table. History
audits checked **50, 50, 204** acknowledged IDs for the original stages and
**50, 36, 250** for shared stages, respectively (including warm-up): zero missing
or changed acknowledgements. Three unacknowledged original higher-rate sends were
additionally found committed, demonstrating why transport failure cannot be
interpreted as rollback. All audit results are embedded in their raw reports.
After restarting the final app replicas, six independent `-mode audit` runs again
passed with zero missing/changed acknowledgements (`audit-*-v3-*-final.json`).
Idle benchmark pools were released before leaving the replicas ready; the
post-restart connection snapshot is `shared-review-ready-state.txt`.

### Sampled resources

CPU percentages can exceed 100% across cores. App CPU/memory are peak simultaneous
sums across the three containers. Other columns are each container's sampled
peak CPU / peak memory MiB; those two peaks need not occur in the same sample.

| Devices / aggregate sends/s | Build | Samples | Apps CPU / MiB | PostgreSQL CPU / MiB | Redis CPU / MiB | Proxy CPU / MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 300 / 2 | Original | 5 | 337.91% / 66.87 | 429.22% / 222.30 | 45.96% / 4.36 | 52.16% / 19.43 |
| 300 / 2 | Shared | 5 | 136.31% / 50.77 | 73.66% / 114.30 | 23.38% / 6.07 | 73.82% / 18.02 |
| 1,000 / 2 | Original | 8 | 434.35% / 157.37 | 377.12% / 222.80 | 29.38% / 5.84 | 110.84% / 49.71 |
| 1,000 / 2 | Shared | 9 | 286.21% / 124.26 | 133.22% / 227.30 | 39.56% / 4.96 | 113.60% / 44.51 |
| 100 / 10 | Original | 14 | 357.45% / 118.25 | 646.69% / 218.80 | 47.55% / 6.55 | 49.32% / 13.62 |
| 100 / 10 | Shared | 5 | 90.23% / 108.30 | 58.00% / 225.10 | 22.51% / 4.96 | 34.84% / 12.41 |

The shared 300-device and higher-rate results support removing repeated database
work. At 1,000, database CPU fell but successful work also fell. The proxy has one
worker and closes upstream HTTP connections; its sampled CPU reached 113.60%.
Per-device HTTP authorization/recovery, socket/encoding cost, proxy connection
churn and admission/reconnect feedback remain candidates for focused profiling.
These snapshots do not isolate a single cause of the 1,000-device regression.
Redis did not report rejected connections or evicted clients in final stage
diagnostics. The final counter check reports zero statement-entry deallocations,
unchanged stats reset time and zero database deadlocks
(`results/shared-final-database-state.txt`). No untested capacity is inferred.

### Failure recovery, same corrected generator

Both `before-shared-v3-failure.json` and `after-shared-v3-failure.json` **passed all
24 assertions**. They use three devices, 3 s warm-up, 15 s measurement and
5 aggregate sends/s, alternating Alice/Bob. The harness requests SIGKILL during
active messaging, checks replica IDs on accepted resume and exercises real
Redis/PostgreSQL stop/start, response loss and durable audits.

| Observation | Original | Shared |
| --- | ---: | ---: |
| Acknowledged IDs audited, including warm-up | 90 / 90 durable | 90 / 90 durable |
| Measured recipient deliveries | 225 / 225 | 225 / 225 |
| Kill request → affected accepted resume elsewhere | 994.968 ms | 724.517 ms |
| Recipient p50 / p95 / p99 | 59.867 / 114.854 / 1,122.203 ms | 67.524 / 115.285 / 1,125.962 ms |
| Crash-only device observed offline, from kill request | 21,444.357 ms | 21,517.709 ms |

These single recovery observations include Docker command overhead; lease expiry
is measured from the last heartbeat, not exactly from process death. Retry delay
regressions enforce capped exponential jitter; the final shared scenario recorded
two retries and zero establishment failures. Other devices remained online.
Identical lost-ack retries returned the original message; conflicting reuse was
rejected. Durable order and duplicate suppression passed. Redis outage sends/read
positions committed, presence returned unavailable, pending outbox entries drained
after restart and missed messages/receipts recovered. PostgreSQL outage returned
503 without acknowledgement; the same ID succeeded after recovery. Passing these
assertions does not relabel any failed load stage as a pass.

## Correctness verification

The final code passed **34 race-enabled tests, zero failed/skipped**, against
real PostgreSQL and Redis (`results/shared-correctness-client-pools.log`). The three
separate-process Compose tests passed (`results/shared-compose-client-pools.log`).
`go vet ./...`, Compose configuration and Nginx syntax checks passed.

Eight new tests cover shared periodic reconciliation across 20 devices; targeted
hints and remaining-device subscription; bounded atomic feed admission/cleanup;
complete tail coverage and bigint boundary; old/stale-cache fallback, current
authorization and database failure; a captured committed head during new writes;
monotonic query-rate timing/reset rejection; and per-device HTTP connection reuse
and release. Existing tests still exercise
concurrent sends, identical/conflicting IDs, direct creation races, group limits,
stable pagination, unread/read monotonicity, lost hints/receipt recovery, outbox
duplicate attempts/backend crash, dependency failures, leases, bounded queues
and shutdown. Queue overflow is deterministic; this is not a measured network
slow-reader limit or an arbitrary 128-conversation throughput benchmark.

## Remaining limits

**10,000 connections: SKIPPED.** The intermediate 1,000-device workload still
failed, so scaling further is not justified. The configured limit is also 1,000
sockets/node (3,000 total); it was not raised. Maximum simultaneous accepted
sessions observed across retained matching v3 attempts was 999 on the original
build and 966 on the shared build. The largest fully passing connection stage
remains 300 devices at 2 aggregate sends/s; the 100-device/10 aggregate sends/s
stage also passed on the shared build. No 1,000-device capacity claim is made.

Long-duration isolated capacity tests, a successful 1,000-device recovery workload,
arbitrary conversation distributions and dependency HA remain **UNVERIFIED**.
There were no skipped correctness tests or blocked Docker prerequisites. The
failed load stages are **FAIL**, not skipped/blocked. A network slow-reader
threshold, 10x and 100x capacity were not measured.

Sharing removes repeated SQL and unnecessary wake-ups, not O(device count) socket
encoding/network work. Public HTTP recovery still performs one membership/head
query per poll. Older backlog reads and connection setup remain per device; many
distinct conversations consume the feed budget and add reconciliation work.
Sends to one conversation remain ordered by one row lock. Snapshot memory and
per-socket queue bounds remain finite; admission/slow-client failures recover by
durable history rather than retaining unbounded work.

Application-instance resilience is distinct from dependency HA. PostgreSQL,
Redis, Nginx and the local Docker host remain single points of failure. Redis
outage leaves durable sends/history available but interrupts presence/live setup;
PostgreSQL outage prevents acknowledgements. At 10x load, measure client poll
cost, idle backend budget, fan-out CPU, network and distinct-conversation mix
before changing cadence or pooling. At 100x, likely changes include conversation
partitioning, a connection tier, batching and durable retention/HA plans; these
are proposals, not verified scale. HA/replication/failover requires separate
configuration and failure proof. No time-spent number, commit or push was made.
