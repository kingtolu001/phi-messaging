# B3.1 implementation plan

## Required scope
- B2.1 Core: direct and group conversations (maximum 100 members), sending messages, cursor history, unread counts.
- B2.1 Senior: WebSocket delivery, client message ID idempotency, conversation ordering, reconnect catch-up, read receipts.
- B3.1 Senior: three application instances behind a load balancer, cross-instance delivery, shared online/offline/last-seen presence, failure recovery without losing acknowledged messages.
- Validation: core tests, concurrency tests, cross-node tests, repeatable node-kill scenario, measured load results.
- Optional stretch: 10,000 simulated connections with delivery p99. No performance claims before measurement.

## Architecture decisions to implement
- Go application; PostgreSQL is the authoritative message and membership store.
- Serialize sends per conversation in a database transaction; assign a monotonically increasing conversation sequence.
- Enforce uniqueness of client message IDs per sender; retries return the original result and conflicting payload reuse is rejected.
- Acknowledge sends only after commit. Persist an outbox entry in the same transaction.
- Redis pub/sub distributes live hints; PostgreSQL history and periodic catch-up repair missed hints.
- Per-conversation client cursors drive reconnect recovery and duplicate suppression.
- Read positions advance monotonically; unread counts derive from eligible messages after that position.
- Redis leases track individual connections; aggregate leases across devices and nodes for presence.
- Bound WebSocket queues and disconnect slow consumers so they can recover from durable history.
- Docker Compose runs PostgreSQL, Redis, three apps and a load balancer.

## Delivery order
1. Define protocol, schema, error behavior and correctness invariants.
2. Implement durable conversations, sends, pagination and read positions with tests.
3. Add WebSocket sessions, cross-instance hints, recovery and presence.
4. Add Compose, failure harness and load generator; record actual results.
5. Complete README and diagrams using the exercise-required diagram tool; rehearse live modifications.

## Submission checklist
- docker compose up starts the project.
- README: setup, architecture, trade-offs, limitations, AI tools and actual total time spent.
- Architecture and key process-flow diagrams produced with Archify or Diagram Design.
- Tests cover core logic and failure claims.
- Mock external services; no cloud account required.

## Current status
Delivery steps 2 and 3 implemented:
- PostgreSQL conversations, ordered/idempotent sends, history, unread counts,
  read positions and receipts. Explicit read-committed write transactions,
  constraint-specific idempotency recovery and recovery's default 100-item page.
- Authenticated bounded HTTP APIs, membership checks and safe error responses.
- Authenticated WebSocket resume subscriptions, ordered PostgreSQL catch-up,
  Redis wake-up hints and periodic repair. Duplicate suppression in sessions and
  the provided recovery-aware Go client.
- SKIP LOCKED outbox workers, transactional marking, bounded publish attempts
  and retry backoff. No acknowledgement depends on Redis.
- Per-connection Redis-time leases, heartbeat renewal, multi-device aggregation,
  retained last-seen values and unavailable responses on Redis failure.
- Bounded queues, write/read deadlines, connection/handshake limits and shutdown
  that closes hijacked sockets and unblocks subscription reads.

Step 4 environment prepared: Compose includes PostgreSQL, Redis, three app replicas,
Nginx, seeded local identities and a race-enabled integration-test image. The
three-process Compose delivery/reconnect test passed. Native integration tests
cover auth, membership, cross-instance hints, missed-event repair, outbox retries,
multi-device leases, dependency failures and client recovery after graceful node
shutdown. See `docs/validation.md` for final run outcomes and limitations.

Historical step 4 failure/load evidence (2026-10-23, before shared delivery):
All workload send rates below are aggregate across the workload, never per device.
- `cmd/exercise` and PowerShell scripts automate active messaging plus SIGKILL,
  client failover, lost-ack retries, durable-history checks, multi-device presence,
  crash lease expiry, Redis outage/outbox recovery and PostgreSQL unavailability.
- Verified failure run recovered all 90 acknowledged messages and all 225 measured
  recipient deliveries. Initial failed validation runs are retained separately.
- Staged fan-out loads at 2 aggregate logical sends/second measured 10, 100, 300 and attempted 1,000 devices. The initial
  300-device run failed on proxy connection limits; the corrected 300-device run
  passed with 12,000/12,000 measured deliveries and all sessions maintained.
- The 1,000-device run FAILED: 971 peak accepted sessions, 1,529 missing measured
  deliveries, one unacknowledged logical send. All 49 acknowledged sends in that
  run remain in durable history. This is a load limit, not verified 1,000-device
  capacity. 10,000 was skipped after saturation and the configured 3,000-node-total
  session cap; no extrapolation is claimed.
- At 100 devices and 10 sends/second, a separate run FAILED: 6,200 missing measured
  deliveries, 52 bounded generator queue drops, 66 unacknowledged sends across
  warm-up/measurement. All 184 acknowledgements remained durable; one send with
  a lost response was additionally found committed. This higher send rate is not
  verified capacity.
- Reviewed correctness, fixed capped jitter and permanent resume rejection,
  hardened client message/history validation, made a racing outbox test deterministic,
  and tested a real publisher PostgreSQL-backend crash before marking publication.
- Final correctness checks: 26/26 race-enabled tests and three separate Compose
  process tests passed, zero skipped or failed; static analysis and proxy syntax
  checks passed. Proxy-generated size/dependency errors now preserve the JSON
  error contract. These passing checks do not relabel failed load stages as passes.
- An all-replicas-down check exposed an unbounded-for-this-client proxy connect
  wait. Setting a one-second connect bound made that check pass; the final full
  failure scenario passed all 24 assertions, with affected resume in 2.681 s and
  all 90 acknowledgements durable. Every saved load run's acknowledgements were
  audited again after dependency restarts. Higher-load delivery failures remain.
- See `docs/validation-2026-10-23.md`, `docs/harness.md` and `docs/results/` for final
  outcomes, exact commands, metrics definitions, failures and resource evidence.

Verified optimization and rerun (2026-10-23):
- Replaced per-session SQL scans/global hint wakes with per-conversation shared
  committed tails and receipts, targeted Redis hints and one-second durable
  reconciliation. Four refresh workers, 128 jobs/feeds, 128 cached messages/feed;
  the last subscriber removes a feed. Old cursors fall back to authorized history.
- HTTP recovery still checks current PostgreSQL membership/head on every call.
  Sends/reads and commit-before-ack transactions are unchanged. Database open
  connections remain capped at 32/app; idle retention now reuses that bounded
  pool. Default client devices own bounded HTTP pools and release them at exit.
- Instrumented actual statement counters with a monotonic measurement window.
  Every workload rate is explicitly aggregate, never per device. A wall-clock
  counter-rate error was fixed; that invalid sample and all failed runs remain.
- Final suite: 34 race-enabled tests passed, zero failed/skipped, plus three
  real-process Compose tests. Static analysis, Compose and proxy syntax passed.
- Matching v3 generator before/after stages used the original retained app image
  versus shared delivery. At 300 devices / 2 aggregate sends/s, both passed all
  12,000 deliveries; p99 fell 2,060.773 -> 312.148 ms and statements/s
  4,116.50 -> 443.69. At 100 devices / 10 aggregate sends/s, shared delivery
  passed all 20,000 deliveries, p99 183.155 ms; the matching original stage failed.
- The matching 1,000-device shared stage still FAILED: peak 966, minimum 624
  accepted sessions, 12,689 missing measured deliveries, 50 HTTP attempt errors,
  14 unacknowledged logical sends. All 36 acknowledged IDs remained durable.
  Tail latency and missing counts regressed versus the original v3 attempt;
  lower query rate is not a successful 1,000-device capacity result.
- Both original/shared v3 node/dependency-failure scenarios passed all 24
  assertions, 90/90 acknowledgements durable and 225/225 measured deliveries.
  Affected accepted resume was 994.968 / 724.517 ms respectively, single local
  observations. Redis outage, missed receipts/events and lost-ack retry passed.
- 10,000 connections SKIPPED after the failed intermediate stage; node caps
  stayed at 1,000 each. Longer isolated load, dependency HA and arbitrary active
  conversation distributions remain unverified. No test was counted as passed
  merely because it was skipped or a report existed.
- See `docs/validation-shared-delivery.md`, `docs/results/shared-comparison.json`,
  raw v3 reports. Historical
  results and intermediate failed optimization builds are preserved separately.

Verified diagrams (2026-10-24):
- Installed and used the pinned Archify 3.2.0 standalone skill locally. Read its
  installation, SKILL.md, authoring, schema and delivery instructions first.
- Created three native editable JSON diagrams with standalone HTML and native
  SVG/PNG exports: runtime, durable send/outbox and application-node recovery.
  Workers stay inside the Go processes; client/server recovery ownership and
  PostgreSQL/Redis single-instance limits match the inspected implementation.
- All current diagrams passed native showcase finalization, strict artifact
  provenance and real-browser checks. A note overlap was repaired; an initial
  Chrome cleanup failure remains recorded separately from later passing runs.
- Reviewed complete light PNG exports. Recorded local hashes/ranges for 13
  implementation/config files. The workspace has no Git snapshot, so native
  commit-pinned source verification was unavailable and was not claimed.
- README links the diagrams, editable sources, validation records and repeatable
  generation commands.
- No application/load-test rerun is claimed for this documentation/tooling change;
  earlier failed load stages and remaining capacity limits are unchanged.

Remaining:
- Investigate the unresolved 1,000-device regression: profile remaining public
  recovery checks, proxy/network and admission/reconnect overhead; rerun longer
  isolated tests before any larger capacity claim. Do not equate fewer queries
  with successful delivery or erase the failed matching stage.
- Final README review and live-review rehearsal.
- Production concerns intentionally outside this local exercise: identity/TLS,
  dependency HA, client cursor persistence and retention/observability design.

No commits or pushes were made for this delivery.
