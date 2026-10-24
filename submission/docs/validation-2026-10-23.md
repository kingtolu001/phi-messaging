# Failure, load and correctness validation — 2026-10-23

Historical report, retained with its failed runs. The subsequent optimization
and fresh before/after measurements are in `validation-shared-delivery.md`.
**Every send rate here is aggregate across the workload, not per device.**
Raw historical JSON predates the explicit `send_rate_scope` field; its
`logical_messages_per_second` has the same aggregate meaning.

The automated failure scenario passes. The largest fully completed workload in
this run was **300 simultaneous recipient devices at 2 logical sends/second**:
12,000 of 12,000 measured deliveries arrived, and all 300 sessions remained
connected during the workload. This is a short local diagnostic result, not a
production capacity/SLA claim. The 1,000-device and higher-send-rate attempts
failed; their missing/error counts remain failures below.

## Environment and execution

- Windows amd64, Go 1.26.2; Intel Core i5-8265U, 4 physical/8 logical cores,
  16,516,628,480 bytes host RAM.
- Docker Desktop 28.5.2, Compose v2.40.3; Linux x86_64 WSL2 kernel
  6.6.87.2-microsoft-standard-WSL2; 8 logical CPUs and 7,998,763,008 bytes
  (7.45 GiB) allocated to Docker. PostgreSQL 17, Redis 7, Nginx 1.28.
- Three independently running Go app containers behind Nginx, plus direct
  localhost debug ports 8081–8083. All dependency tests use local containers.
- Shared development machine, not pinned/dedicated CPUs. Unrelated osai containers
  were observed running and were not stopped. Native generator CPU was not
  separately profiled. The initial ten-device exploratory run overlapped tooling
  work; its corrected comparable rerun is identified separately.
- [Environment JSON](results/environment.json), [exact commands and metric
  definitions](harness.md), [summary JSON](results/summary.json). No total time
  spent is inferred from run timestamps or recorded here.

## Automated failure recovery

[Latest verified report](results/failure-proxy-bounded.json): **PASS, 24 assertions**.

The harness starts independent clients, pins one first socket to app1, alternates
Alice/Bob sends at 5 logical sends/second, and requests SIGKILL during the active
workload. Accepted resume frames contain replica IDs, so reconnecting to a different
replica is asserted rather than inferred from a successful HTTP call.

| Observation | Actual result |
| --- | --- |
| Acknowledged messages across warm-up + measurement | 90; all 90 IDs/sequences present in contiguous PostgreSQL history |
| Measurement window | 15 s after 3 s warm-up; 75 measured acknowledgements |
| Expected/measured recipient deliveries | 225/225; 0 missing measured or warm-up deliveries; 0 duplicate callbacks |
| Pinned client accepted resume on another instance | 2,680.558 ms after the kill-command request |
| Recorded retry delays | 75.469 / 74.286 / 168.791 ms; within capped jittered bounds |
| Establishment failures / retries | 1 / 3; recovered rather than terminal |
| HTTP attempt errors during active messaging | 0 |
| Active recipient latency p50/p95/p99 | 101.643 / 839.679 / 1,374.611 ms |
| Sender acknowledgement p50/p95/p99 | 38.123 / 822.813 / 1,311.185 ms |
| Crashed, nonreconnecting device observed offline | 27,627.959 ms after requesting kill; last_seen retained |

The lease is 30 seconds from the last heartbeat, not from process death. The
offline observation includes Docker overhead and 250 ms polling. Accepted socket
resume and first recipient callback are different endpoints: HTTP catch-up may
deliver messages even before the next socket resume completes.

Additional assertions passed:

- A forwarding proxy dropped a real committed 201 response. The same client ID
  retried with identical content returned 200 and sequence 1; changed content
  returned 409. This deliberately models response loss after commit.
- Other devices remained online when one device's node died. The crash-only
  device did not reconnect and was removed by lease expiry rather than cleanup.
- Redis was really stopped: PostgreSQL sends and read positions committed,
  presence returned 503, and two outbox events remained pending. After restart
  the pending count reached zero; history and receipts restored missed state.
- PostgreSQL was really stopped: sends returned 503. After restart, retrying the
  same ID succeeded. No response during database unavailability was counted as an
  acknowledgement.
- The harness restored stopped services. The separate correctness suite also
  terminated an outbox publisher's PostgreSQL backend after publication but before
  marking; the next attempt reclaimed the row and published again.

Failure history is retained. [failure-initial.json](results/failure-initial.json)
passed the initial single-sender scenario. [failure-final.json](results/failure-final.json)
is **FAILED**, despite its filename: a one-shot history audit hit the killed proxy
upstream. Its separate surviving-node history check still found all 90 acks.
The audit now uses a bounded retry and records read attempts; a unit test verifies
that retrying the read never forgives a missing acknowledged ID. The verified
rerun above passed; it does not erase that earlier failed run.
The earlier [verified run](results/failure-verified.json) is also retained: 954 ms
resume, one HTTP attempt error and a roughly 7.19 s delivery p99. A later
all-replicas-stopped check [failed](results/proxy-check.log) because Nginx's default
upstream connect timeout outlived the client's five-second wait. The proxy now
has a one-second connect bound and JSON dependency errors. The
[repeat check](results/proxy-check-final.log) passed, including clean replica exit
codes; the latest full failure run above passed with that configuration. Docker
progress is written on stderr and PowerShell labels that stream NativeCommandError
in these logs; the explicit PASS plus process exit 0 denotes the passing repeat,
while the first run exited 1 with `No proxy response` and remains a failure.

## Measured load

Each load stage uses one fresh three-member group, Alice as sender, and N Bob
device connections with one subscription each. This is shared-conversation fan-out,
not N distinct users. Payload bodies are 64 ASCII characters. Connection starts
are spaced by 10 ms, with up to 30 s to settle; then 5 s warm-up, 20 s measurement,
and up to 15 s delivery drain **after send workers finish**. There are 16 sender
workers and 128 pending-job slots. Extended send completion can make the actual
measurement-plus-drain interval longer than 35 s; it is recorded in each JSON.

All devices use the real recovery client, including WebSocket registration,
resume, ordered HTTP catch-up, periodic recovery and duplicate suppression.
Delivery latency starts before the first HTTP attempt and ends at the first
ordered recipient callback in the same generator process using a monotonic clock.
It excludes time waiting in the generator's pending-job queue. Queue drops,
scheduler lateness and missing deliveries remain explicit. Sender ack latency is
reported separately. No database/server clock is subtracted from a client clock.

The table's delivery percentiles are **among received samples only**. When samples
are missing, they do not establish a finite p99 for the complete offered workload.

| Workload/report | Result | Peak accepted | Received / expected measured | Delivery p50 / p95 / p99 (ms) |
| --- | --- | ---: | ---: | --- |
| [10 devices, 2 sends/s](results/load-10-final.json) | PASS | 10 | 400 / 400 | 64.503 / 129.449 / 174.880 |
| [100 devices, 2 sends/s](results/load-100.json) | PASS | 100 | 4,000 / 4,000 | 1,092.613 / 2,672.228 / 3,289.948 |
| [300 devices, original proxy](results/load-300-initial.json) | **FAIL** | 233 | 10,918 / 12,000 | 1,539.920 / 13,871.053 / 20,643.320 |
| [300 devices, proxy limits fixed](results/load-300-proxy-fixed.json) | PASS | 300 | 12,000 / 12,000 | 1,039.724 / 2,134.644 / 2,495.538 |
| [1,000 devices, 2 sends/s](results/load-1000.json) | **FAIL** | 971 | 38,471 / 40,000 | 8,936.339 / 26,645.879 / 33,798.366 |
| [100 devices, 10 sends/s](results/load-100-rate10.json) | **FAIL** | 100 | 13,800 / 20,000 | 3,531.010 / 12,650.637 / 14,756.494 |

| Workload | Sender ack p99 (ms) | Completed acks/s | Completed device deliveries/s | WS establishment failures / reconnect retries | HTTP attempt errors | Unacknowledged logical sends, all phases |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10, rate 2 | 89.169 | 2.000 | 20.000 | 0 / 0 | 0 | 0 |
| 100, rate 2 | 2,637.092 | 1.929 | 192.869 | 0 / 0 | 0 | 0 |
| 300, original proxy | 1,607.216 | 1.028 | 303.213 | 1,395 / 3,676 | 24 | 3 |
| 300, corrected proxy | 1,897.031 | 1.932 | 579.522 | 0 / 0 | 0 | 0 |
| 1,000, rate 2 | 9,336.699 | 1.001 | 962.907 | 5,421 / 8,217 | 12 | 1 |
| 100, rate 10 | 14,579.168 | 2.161 | 216.149 | 0 / 0 | 120 | 66 |

Throughput denominators include the actual measured completion/drain interval,
excluding warm-up/ramp and post-run diagnostics. Samples are correlated through
one conversation. These short sequential diagnostic stages were not randomized;
the proxy configuration changed between stages. Do not infer a smooth capacity
curve from their relative latency.

The passing 300-device run maintained 300 sessions throughout measurement. In the
1,000 attempt only 742 were connected at measurement start and the minimum was
188. **971 is a transient observed maximum, not verified 1,000-device capacity.**
Its [separate durable-history audit](results/audit-1000.json) found all 49
acknowledged messages, with zero missing/changed acknowledgements. Missing device
deliveries by the deadline are still failures even when history remains durable.
After the additional dependency restarts, separate `audit-load-*.json` reports
again checked all seven saved load runs: every acknowledged ID/sequence was still
present. These audits do not turn a failed delivery-deadline run into a pass.

At rate 10, the scheduler generated all 250 slots (50 warm-up, 200 measured), but
52 jobs were dropped from the bounded pending queue. There were 66 unacknowledged
logical sends across both phases, 138 measured acknowledgements and 6,200 missing
measured deliveries. The audit found all 184 acknowledgements and one additional
unacknowledged-but-committed message. Warm-up had 300 missing device deliveries.
The run's roughly 63.845-second measured completion interval is not disguised as
a successful 20-second throughput result. No terminal client errors or duplicate
callbacks were observed in these reported load stages.

[load-10.json](results/load-10.json) is retained as exploratory evidence. Its earlier
scheduler included the exact end boundary and produced 41 measured sends instead
of 40. It is excluded from the comparable table, replaced by the corrected rerun.
Regression tests now cover interval boundaries and nearest-rank tail quantiles.

## Bottlenecks and resource evidence

The original proxy left worker_connections unset. Nginx defaults to 512 per
worker and counts upstream sockets too, consistent with the observed 233-client
ceiling under concurrent HTTP work. The configuration now explicitly allows
16,384 connections and a 65,536 file limit. The corrected 300-device run passed.
This default/counting behavior is documented in the [Nginx core reference](https://nginx.org/en/docs/ngx_core_module.html#worker_connections).

Raw Docker samples and PostgreSQL/Redis snapshots are embedded in every load
report. Selected **sampled** CPU peaks are below; percentages can exceed 100%.
Peaks per container can occur at different moments and must not be summed as a
single observation. Memory is the sample at that container's peak CPU, not a
guaranteed maximum memory watermark.

| Run | PostgreSQL CPU / memory | App1 / app2 / app3 CPU | Redis CPU / memory | Proxy CPU |
| --- | --- | --- | --- | --- |
| 10, corrected baseline | 24.30% / 64.39 MiB | 8.28 / 12.62 / 10.67% | 8.65% / 3.68 MiB | 3.29% |
| 300, corrected proxy | 414.15% / 211.1 MiB | 111.63 / 100.15 / 90.38% | 26.06% / 4.10 MiB | 67.23% |
| 1,000 attempt | 462.23% / 145 MiB | 166.51 / 214.78 / 176.49% | 54.54% / 4.72 MiB | 107.72% |
| 100, rate 10 | 737.98% / 187 MiB | 131.31 / 135.23 / 125.91% | 42.64% / 4.01 MiB | 50.67% |

The database is the strongest observed CPU bottleneck, especially at rate 10;
memory exhaustion was not observed in the samples. The implementation performs
per-session PostgreSQL transactions and receipt queries for polling/hints, while
each client also polls HTTP recovery. Hints wake all sessions. This structurally
amplifies work with connection count and send rate, consistent with the sampled
CPU/tail/error growth. It is an inference supported by code and samples, not a
function-level profile proving the exact dominant SQL statement.

The load runs preceded the last proxy error-format/connect-timeout correction.
That later correction was verified with a real all-replicas-stopped check and a
repeat failure scenario; the load figures above are retained as measurements of
their recorded runs, not silently rewritten as new measurements.

The additional [query sample](results/load-1000-query-sample.txt) was taken after
clients had drained and shows idle backends; it is not used as evidence of active
saturation. No query-level CPU profile, generator CPU profile, disk latency
profile or long-duration soak was collected. Those remain limitations.

## Correctness review and regression results

The initial rerun of the old 18-test suite failed one test: its manual outbox claim
raced a background failed publisher, so SKIP LOCKED legitimately returned no row.
The test now stops that worker before its manual retry; the production locking
rule is unchanged. The failed baseline is not counted as a passing suite.

Final checks: **26/26 race-enabled tests passed, 0 failed, 0 skipped**, in
[correctness-final-26.log](results/correctness-final-26.log). The three additional
real-process Compose tests passed in
[compose-review-final.log](results/compose-review-final.log). `go vet ./...` and
the final Nginx configuration check passed. Earlier passing suite logs are retained
with their smaller case counts; they are not added together as unique tests.

| Area | Evidence/changes |
| --- | --- |
| Concurrent sends, identical/conflicting IDs, direct races | Storage concurrency tests plus three-process HTTP concurrency/retry/direct-race test |
| Membership/auth, 100-member cap | HTTP/WS denial tests; 100 valid distinct members accepted, 101 rejected in storage tests |
| History under new writes, unread/own sends, monotonic reads | Existing storage tests rerun against PostgreSQL; HTTP assertions rerun |
| Read receipts after missed hints | Added assertion to the no-hint periodic recovery test; real Redis-outage receipt retrieval |
| Outbox duplicate publication/crash | Real publisher backend terminated between callback and mark; rollback/reclaim/republication asserted |
| Redis and PostgreSQL outages | Actual Compose stop/restart assertions, alongside isolated pool/client failure tests |
| Slow consumers/queue/shutdown | Deterministic bounded queue overflow, write/read deadlines, connection caps and shutdown tests; graceful-node client recovery |
| Client retry correctness | Jitter remains variable at the cap; bad resume rejected permanently; unknown conversations/invalid positions rejected; nonadvancing history pages fail closed |
| Harness correctness | Half-open schedule, quantile tail, bounded audit retry requiring every acknowledgement, exclusive report creation preserving failure evidence |
| Proxy error contract | Oversized requests return JSON 413 through Nginx; proxy-generated dependency errors map to JSON 503 instead of HTML |

## Unresolved failures, skipped work and resilience limits

- **Unresolved:** delivery/message deadlines under the 1,000-device and rate-10
  workloads. Backpressure prevents unbounded growth; it does not make those runs
  successful. Performance work and new measurements are required before claiming
  higher capacity. No acknowledgement loss was observed in the audited runs.
- **SKIPPED:** 10,000 simultaneous connections. The 1,000 attempt already failed
  with CPU saturation/reconnect churn, and three nodes retain their 1,000-session
  each limit. Raising that bound alone would not establish sufficient resources.
  Maximum requested: 1,000; maximum simultaneously accepted: 971; largest fully
  passing workload: 300 at the specified rate 2.
- **Not tested:** sustained soak, many independent conversations/users, separate
  machines, network partitions, corrupted storage, dependency failover, UI rendering
  latency, or a measured real-network slow-reader disconnect threshold. The queue
  overflow test is deterministic; it does not claim a network throughput threshold.
- No prerequisite remained blocked: Docker, Go, PostgreSQL and Redis were usable.
  Final required test runs had no skipped dependency tests. `[no test files]`
  package output means compilation only. Earlier native compile checks without
  dependency URLs are not counted as integration evidence.
- Three app replicas survive an app-process failure, not a PostgreSQL/Redis failure
  with uninterrupted service. PostgreSQL is one authoritative write node and disk;
  Redis is one cache/presence/pubsub instance. Nginx, Docker host and native client
  device are also single failure points. Restarting dependencies in the harness
  is recovery, not high availability. Client cursors remain in memory in the demo.

At **10x the verified device workload** (3,000) and **100x** (30,000), no performance
result is asserted. Design changes to evaluate include shared per-conversation
dispatch/catch-up per node instead of per-device database scans, targeted/coalesced
hints, bounded shared recent-history caches, jittered recovery scans, measured
database pool/query tuning and admission control, more proxy/app nodes and
appropriate file/session limits. Hot-conversation sends still serialize and need
their own measured contention limits. At 100x, partition conversation ownership
and message storage only with an explicit sequencing/recovery contract; adding
Redis hints alone cannot remove database ordering costs. Dependency HA would
require replicated durable storage with tested failover/backups, Redis failover
with defined presence-loss semantics, redundant ingress and multiple hosts.
None of those changes is presented as implemented or sufficient without testing.

Teaching explanations are in `../../learning/07-harness-metrics-and-failures.md`.
No commits or pushes were made. Required diagrams, owner-provided actual total
time spent and final live-review rehearsal remain outside this validation result.
