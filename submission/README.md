# B3.1 Multi-node Messaging

Go HTTP/WebSocket servers share PostgreSQL messages and read positions. A message
is acknowledged only after its PostgreSQL transaction commits, including its
outbox entry. Redis pub/sub refreshes shared conversation feeds; sessions consume
committed history in sequence order. Periodic durable reconciliation repairs
missed hints. Redis leases track each
device connection independently.

## Diagrams

Open the [diagram index](docs/diagrams/index.html) for the three native Archify
diagrams. Each includes editable JSON, standalone interactive HTML and SVG/PNG.

- [Runtime architecture](docs/diagrams/runtime.html): three identical Go processes,
  their local workers, and shared PostgreSQL/Redis single points of failure.
- [Durable send and outbox delivery](docs/diagrams/send.html): commit before success,
  independent publication, and ordered recipient delivery from durable state.
- [Node failure and recovery](docs/diagrams/recovery.html): client-owned reconnect
  and cursors, server reconciliation, missed-event repair and device leases.

[Regeneration and validation notes](docs/diagrams/README.md) document the pinned
Archify tool, source-review snapshot and actual browser/artifact checks. Sequence
positions show interaction order, not benchmark timings.

## Run locally

Requires Docker Compose. From this directory:

```powershell
docker compose up --build -d
```

The load balancer listens at `http://localhost:8080`. Replicas can also be reached
directly at localhost ports 8081, 8082 and 8083 for debugging. PostgreSQL and Redis
are internal to Compose. Poll `/health/ready` on each replica until ready.
`docker compose stop` stops the services and preserves PostgreSQL's named volume.
Database initialization SQL runs only when that volume is empty.

This is a local mock identity provider. The three seeded IDs and their public
exercise tokens are in `config/demo-tokens.json`. Send tokens only in the
`Authorization: Bearer ...` header. Example in PowerShell:

```powershell
$identities = Get-Content config/demo-tokens.json -Raw | ConvertFrom-Json
$alice = $identities.PSObject.Properties | Where-Object Value -eq '11111111-1111-4111-8111-111111111111'
$headers = @{Authorization = "Bearer $($alice.Name)"}
$body = @{kind='direct'; member_ids=@('11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222')} | ConvertTo-Json
$conversation = Invoke-RestMethod http://localhost:8080/v1/conversations -Method Post -Headers $headers -ContentType application/json -Body $body
$message = @{client_message_id=[guid]::NewGuid().ToString(); body='Hello'} | ConvertTo-Json
Invoke-RestMethod "http://localhost:8080/v1/conversations/$($conversation.id)/messages" -Method Post -Headers $headers -ContentType application/json -Body $message
```

See [the protocol](docs/protocol.md) for all APIs and WebSocket resume frames.
Native browser WebSocket APIs cannot set arbitrary bearer headers. This exercise
provides a Go client; a browser application would need an authenticated session
design rather than tokens in URLs.

With Go 1.26.2 installed, run a recovery-aware receiving client in another shell:

```powershell
$identities = Get-Content config/demo-tokens.json -Raw | ConvertFrom-Json
$bob = $identities.PSObject.Properties | Where-Object Value -eq '22222222-2222-4222-8222-222222222222'
$env:APP_TOKEN = $bob.Name
$env:APP_URL = 'http://localhost:8080'
go run ./cmd/client
```

The client prints committed messages as JSON, suppresses overlapping live/history
duplicates, recovers gaps, periodically discovers conversations and reconnects
with jittered backoff. Its device cursors are in memory: restarting it replays
history from zero. A real client must persist processed cursors with its own state.
Read receipts are separate and change only through the read API.

Each default client device owns a bounded two-connection HTTP pool and closes its
idle connections on shutdown. A caller-provided HTTP client remains caller-owned.

## Tests

Run correctness before load. After a load fills the idle SQL pools, restart the
idle app replicas (`docker compose restart app1 app2 app3`) or use separate test
dependencies before starting fixtures. See [the harness guide](docs/harness.md)
for the shared database's connection budget and benchmark isolation.

```powershell
docker compose --profile test run --build --rm tests
go test -tags compose -count=1 -timeout=60s -v ./integration
```

The first command runs race-enabled tests against real PostgreSQL/Redis. It fails
if required integration configuration is missing. Fixtures use isolated schemas
and Redis prefixes; they do not erase the demo database. The second command tests
the three running app processes and load balancer; run `compose up` first.
For native Go tests using separate disposable dependencies, see [storage setup](docs/storage.md).
With no integration URLs, native `go test` explicitly skips dependency-backed
tests; that is not evidence of correctness. [Validation](docs/validation-shared-delivery.md)
records actual outcomes and test limitations.

## Boundaries and trade-offs

Membership is fixed. Direct-pair uniqueness and sender/client-ID uniqueness are
database constraints. Conversation row locks make sequence allocation simple but
serialize a hot conversation. Outbox workers keep one transaction open during a
bounded Redis publish, prioritizing a small explainable implementation. Publishing
can repeat, and a publish with no subscribers can be lost to live consumers.
PostgreSQL recovery is the delivery authority.

Hints target the subscribed conversation. Each node shares a 128-message tail and
receipt snapshot, refreshed by four workers and a one-second sweep. There are at
most 128 active conversation feeds per node and 100 subscriptions per socket.
Public recovery still checks PostgreSQL membership/head on every request; an old
cursor or cache miss uses durable history. This reduces repeated queries without
making Redis or memory authoritative. Queue overflow or write timeout disconnects a
consumer; HTTP history and device cursors recover it. Presence expiry reflects
the last observed heartbeat, not an exact crash timestamp. Redis state is ephemeral.
The sample client does not render receipt events; the API and socket still provide them.

One PostgreSQL and one Redis container are single dependency failure points.
The proxy uses round-robin routing; it does not automatically retry requests,
including sends with an uncertain commit outcome. Clients retry sends using the
same client message ID. Expect failed requests during node detection/recovery.
There is no TLS, production identity provider, durable client cursor store, outbox
retention job, database HA, or measured capacity claim. Compose credentials and
tokens are for local use. Proxy request logging is disabled to avoid credential
exposure; production needs redacted observability.

## Submission work still pending

Repeatable SIGKILL/dependency-outage validation and measured local load results
are now available; see [exact commands](docs/harness.md) and the
[shared-delivery report](docs/validation-shared-delivery.md). All workload send
rates are aggregate across the workload, never per device. Historical failures remain
explicit failures, not claimed capacity. The required diagrams were generated
with Archify and validated; see the links above. No total time spent has been recorded. AI assistance:
OpenAI Codex assisted implementation, review, tests and documentation in this
workspace. The owner must provide actual total time spent before final submission.
Teaching walkthroughs are maintained separately and are excluded from this repository.
