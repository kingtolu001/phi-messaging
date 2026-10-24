# Verification record

Historical delivery record. Current optimization and validation evidence is in
`validation-shared-delivery.md`; the first failure/load review is preserved in
`validation-2026-10-23.md`. Pending items below describe the state on October 22.

Date: 2026-10-22 (Africa/Lagos). This records correctness checks, not performance
benchmarks or total time spent.

## Before implementation

Read PLAN, protocol, storage documentation, the migration, storage implementation
and its tests. Ran the existing seven storage tests on Windows amd64 with Go
1.26.2, PostgreSQL 17 and `-race -count=1`: all seven passed; none skipped or blocked.

Review corrections: require the expected sender/client-ID uniqueness constraint
before resolving a send conflict, make write isolation explicitly read committed,
and default recovery pages to 100 as specified. Other unique violations must not
be silently interpreted as successful idempotency. HTTP limits also validate
explicit zero limits rather than treating them as omitted storage defaults.

## Implemented behavior exercised

| Check | Evidence |
| --- | --- |
| Conversation/send concurrency | Existing direct-pair, identical retry, cross-conversation ID conflict, contiguous order and monotonic read tests |
| HTTP auth and membership | Invalid/missing tokens, nonmember history/send/read/receipts/presence, forged actor fields and bounded input |
| Durable acknowledgements | Failed outbox insertion returns 503, leaves head unchanged; unavailable PostgreSQL never acknowledges; Redis failure still permits committed sends/history |
| Outbox retries and claims | Uncertain publish retried without marking, concurrent workers claim each pending row once in the success case |
| Redis cross-instance delivery | Separate app objects/listeners, periodic scanning disabled, HTTP on one and WebSocket on another; ordered messages and receipts |
| Lost/duplicate hints | Lost-publish simulation with no Redis hint repaired periodically; repeated hints do not duplicate messages |
| Reconnect | Resume from an earlier processed cursor recovers offline messages in order; HTTP recovery agrees |
| Device presence | Two connections on different nodes; one disconnect does not hide the other; unremoved lease expires with last_seen retained |
| Resource limits/shutdown | Deterministic full-queue disconnect, handshake/connection caps, closing sockets/workers without deadline exhaustion |
| Recovery client | Overlapping HTTP/live messages deduplicated; gaps rejected; callback failure does not advance; reverse-proxy switch and graceful node shutdown recover acknowledged messages |
| Three real app processes | Tagged Compose test sends app1 to app2, sends via app3 while receiver offline, resumes through Nginx and verifies PostgreSQL history |

## Run history and limits

Final results for this delivery:

| Run | Outcome |
| --- | --- |
| Native `go test -race -count=1 -timeout=180s -v ./...` with required dependency URLs | 18 tests passed: 1 client, 9 server, 8 storage; 0 skipped, 0 blocked |
| Compose `tests` image, Go 1.26.2 Linux amd64, `-race` | Same 18 tests passed; 0 skipped, 0 blocked |
| `go test -tags compose -count=1 -timeout=60s -v ./integration` | 1 additional test passed against three real app containers and Nginx |
| `go vet ./...` | Passed with no diagnostics |
| `docker compose config --quiet` and app/test image builds | Passed; all three app replicas and load balancer started |

Packages reported as `[no test files]` were compiled, not counted as passing test
cases. The `compose` build-tag test is separate from the default 18-test suite.
PostgreSQL is version 17 and Redis version 7 in the local test/Compose containers.
Compose services remain running for local review; the two standalone dependency
test containers were stopped after validation. No source commit or push was made.

An intermediate compile check ran without integration URLs; dependency-backed
tests skipped and were not counted as correctness evidence. Set
`REQUIRE_INTEGRATION=1` to turn missing test configuration into a failure.

The first new server integration run failed shutdown cleanup: cancellation did
not unblock a Redis subscription read. `context.AfterFunc` now closes the
subscription explicitly. The subsequent server suite passed, including shutdown.
This earlier failed run is not reported as a passing suite.

Native Go integration tests use real PostgreSQL and Redis with uniquely isolated
schemas/key prefixes. App objects run on separate HTTP listeners within the test
process. The tagged Compose check additionally exercises separate Linux app
processes. Dependency failures in the native suite use closed clients/pools and
an injected SQL trigger, rather than stopping shared test infrastructure. Presence
crash behavior uses an unremoved lease. The queue boundary is tested directly,
rather than claiming a measured network slow-reader threshold.

An abrupt container-kill harness, actual crash-to-offline timing and sustained
load/delivery p99 are still unverified. No 10,000-connection claim, database HA
claim, exactly-once delivery claim or time-spent number has been made.

## Commands

```powershell
# Native suite, with isolated local dependencies already running
$env:REQUIRE_INTEGRATION='1'
$env:TEST_DATABASE_URL='postgres://postgres:phi_test@127.0.0.1:55432/phi_test?sslmode=disable'
$env:TEST_REDIS_URL='redis://127.0.0.1:56379/0'
go test -race -count=1 -timeout=180s -v ./...
go vet ./...

# Container suite and real-process smoke check
docker compose up --build -d
docker compose --profile test run --build --rm tests
go test -tags compose -count=1 -timeout=60s -v ./integration
```
