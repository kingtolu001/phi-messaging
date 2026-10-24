# Reproduce failure and load checks

All send rates in this document and the saved workload reports are **aggregate
logical sends per second across the workload, never per recipient device**.
For example, rate 2 with 300 devices offers 2 messages/s and 600 device deliveries/s.

Run from `submission/` on Windows with Docker Desktop, Go 1.26.2 and PowerShell.
These commands operate only on this Compose project's services; database volumes
are retained. Stop unrelated workloads yourself if you want an isolated machine.

```powershell
./scripts/environment.ps1
./scripts/enable-query-metrics.ps1
./scripts/failure.ps1
./scripts/load.ps1 -Connections 10,100,300,1000 -Rate 2 -Warmup 5s -Duration 20s
./scripts/summarize.ps1
docker compose --profile test run --build --rm tests
go test -tags compose -count=1 -timeout=90s -v ./integration
go vet ./...
# Optional destructive-to-processes check; restores replicas in finally
./scripts/proxy-check.ps1
```

Run correctness checks before load stages. If a previous load filled all three
32-connection idle pools, restart the idle app replicas first (`docker compose
restart app1 app2 app3`) or use isolated test dependencies: the default database
has 100 connection slots and those pools can retain 96. Do not run the suite
concurrently with a benchmark. Restarting is test-environment preparation, not
an application high-availability claim.

To repeat the optimization's comparable stages against the currently deployed
build, use a fresh prefix (all three independent stages run even if one fails):

```powershell
./scripts/delivery-stages.ps1 -Prefix review-after
./scripts/compare-delivery.ps1 -Before before-shared-v3 -After review-after -Output docs/results/review-comparison.json
```

The saved `before-shared-v3` reports were measured against a retained original
app image using the same owned device HTTP pools as `after-shared-v3`. The earlier
v2 pair used the original generator pooling. Reproducing a new baseline requires
deploying the original implementation; changing a filename prefix alone does not
create a baseline. All report writers reject existing output names.

If local execution policy blocks a script, invoke it with an allowed PowerShell
execution policy for that process. Do not disable machine-wide policy.
For example: `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/failure.ps1`.
The load script stops at the first failed stage. Retain its report, investigate,
then invoke the next stage explicitly only when it is safe to continue.
The optional stretch is an explicit `-Connections 10000` attempt, subject to
resources and configured limits; it is not part of a passing smaller stage.

Equivalent direct commands, after Compose is ready:

```powershell
go build -o .artifacts/exercise.exe ./cmd/exercise
./.artifacts/exercise.exe -mode failure -warmup 3s -duration 15s -rate 5 -out docs/results/failure-new.json
./.artifacts/exercise.exe -mode load -connections 100 -rate 2 -warmup 5s -duration 20s -ramp 10ms -drain 15s -out docs/results/load-100-new.json
```

Each run creates a fresh three-member group using the demo users. For load, Bob
has N independent device connections subscribed to that group; Alice sends a
64-character body at the requested total rate. This measures a shared-conversation
fan-out workload, not N distinct active users or independent market conversations.
Failure mode alternates Alice and Bob as senders. Every device uses the actual Go
client: register, resume, HTTP recovery, duplicate suppression, periodic catch-up,
ping/pong, and capped jittered reconnect. The first failure-mode device starts on
app1 and subsequently reconnects through the load balancer. Instance IDs are
reported in the registration frame; hooks count only accepted resume sessions.

Script defaults use timestamped output names. The Go runner refuses to overwrite
an existing report, so failures cannot disappear silently during a rerun. Choose
a new name for repeated direct commands. Existing runs can be audited separately:
`./.artifacts/exercise.exe -mode audit -input docs/results/load-1000.json -out docs/results/audit-new.json`.
Readiness is checked before workload fixtures, so startup/restore delays do not
silently enter a healthy-load measurement. Failing the readiness deadline produces
a failed run rather than omitting the stage.

The failure harness drops a real committed send response at a local forwarding
proxy, verifies an identical retry returns 200, kills app1 with SIGKILL during
ongoing sends, and checks acknowledged IDs/sequences against paginated history.
It observes another device remaining online and a nonreconnecting crashed device
becoming offline with last_seen retained. It then stops/restarts Redis and
PostgreSQL, verifies availability semantics, pending outbox recovery and receipts.
It restores stopped services in cleanup. A failed assertion returns process exit
code 1 and is saved as a failure, not silently retried into a passing report.

## Metric definitions

All send starts and delivery callbacks execute in one native load-generator
process. Go `time.Now`/`Sub` retain a monotonic clock for durations. Server and
database timestamps are not subtracted from client timestamps; no distributed
clock synchronization is assumed. UTC timestamps in JSON identify the run only.

Recipient delivery latency starts immediately before the first HTTP send attempt
and ends at the recipient's first successful ordered message callback. It includes
commit, notification/polling, network, reconnect and recovery delays. It measures
application receipt, not screen rendering. The same logical send's retries retain
the original start time. Sender acknowledgement latency ends when a successful
HTTP response is decoded; it is reported separately. Duplicated live/history
frames do not add latency samples. Suppressed duplicates are counted separately.

The scheduler uses fixed rate slots in the half-open warm-up/measurement window.
Warm-up messages are checked for correctness but excluded from latency/throughput
samples. Scheduler lateness is reported separately. There are 16 send workers and
a 128-job bounded queue. Dropped jobs and unscheduled slots remain explicit errors.
Slow sends therefore cannot silently reduce the offered workload. Up to three
HTTP attempts use the same client message ID; every failed attempt is counted,
including ones followed by successful retries.

Every measured logical send expects N device deliveries, including unacknowledged
and queue-dropped sends. Records retain acknowledgement state and delivery counts.
Missing deliveries are expected minus received, with a finite drain deadline after
sends finish. Latency quantiles are nearest-rank quantiles **among received
deliveries only**; failed or missing deliveries have no finite measured latency.
Read the missing/error counts alongside them. A failed run with missing deliveries
does not establish a finite end-to-end p99 for the full offered workload.

Throughput divides completed measured acknowledgements or recipient callbacks by
the actual measurement-plus-drain duration. This includes any slow-send completion
after the nominal window. Connection ramp and warm-up are excluded. Resource
sampling/diagnostic cleanup after the drain is excluded. Connection establishment
failures, retries, terminal errors, maximum simultaneous accepted resumes and the
minimum during the active workload are separate fields. A peak is not proof that
all requested connections remained present for the entire run.

Recovery time runs from requesting `docker compose kill` to the pinned device's
accepted resume on a different instance. It includes Docker command overhead;
first recovered message latency is also reflected in recipient samples. Lease
expiry is observed from that same kill request with 250 ms polling and normal
30-second heartbeat leases. It is a bound on observation, not an exact crash time.

`docker stats` is sampled during each run, including app, PostgreSQL, Redis and
proxy CPU/memory/network/block-I/O where available. Raw samples are retained;
percentage CPU can exceed 100% when multiple cores are used. PostgreSQL counter
and wait-state snapshots and Redis INFO stats are captured before/after. These
are snapshots, not continuous profiling; unrelated development containers and the
native generator compete for the same machine. No measured result is extrapolated
to an untested connection count or a production capacity promise.

For query-rate measurements Compose preloads `pg_stat_statements`; run
`scripts/enable-query-metrics.ps1` to create the extension in an existing or fresh
database. The runner records call counters before connection ramp and after client
shutdown/resource collection/diagnostics, before the durable history audit. It
divides their difference by a separately measured monotonic duration, recorded as
`database_counter_window_seconds_monotonic`. This full-run rate includes warm-up,
ramp, drain, transaction statements, background outbox polling and instrumentation;
it is not a SELECT-only or measurement-window-only rate. Missing/reset counters
are unavailable, not zero. `pg_stat_statements` execution time can include lock
waits and is not CPU time. No statistics reset is required or performed by the
runner. Check `pg_stat_statements_info` for eviction/reset when interpreting data.
