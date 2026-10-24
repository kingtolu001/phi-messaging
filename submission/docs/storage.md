# PostgreSQL storage

`internal/storage` implements delivery step 2. Supply a `database/sql` pool using
the registered `pgx` driver to `storage.New`. Apply `migrations/001_initial.sql`
once to an empty application database and seed `users` before creating conversations.
Compose initializes this migration and `config/seed.sql` on first database creation.
There is no standalone migration upgrade command yet.

Public operations receive an authenticated actor. IDs must be canonical
lowercase, hyphenated UUIDs. Conversation creation receives the complete member
list, including the actor; duplicate members are rejected. `Send` returns a message,
a created flag (false for an identical retry), and an error. Only successful calls
may be acknowledged. A commit error has an uncertain outcome; retry with the same
client message ID. Message and read events are recorded in outbox, with sequences
encoded as JSON strings. `PublishNext` claims one pending row with SKIP LOCKED,
publishes a conversation wake-up hint, then marks it published before committing.
A failed or uncertain publish rolls back the claim; a later worker retries.

`HistoryQuery.Before` and `After` are exclusive cursors and mutually exclusive.
Zero limit defaults to 50 (100 for recovery); valid explicit limits are 1–100. Initial history is
descending; use the last returned sequence as the next `Before`. Recovery is
ascending; use the last returned sequence as the next `After` until an empty page.
`ListConversations` uses the last returned conversation UUID as its next cursor.
`MarkRead` returns the durable maximum, including for stale updates; a no-op does
not append another outbox event. Membership is fixed.

Map `ErrInvalid` to 400, `ErrNotFound` to 404 and `ErrConflict` to 409 at the future
transport boundary. The HTTP layer now performs this mapping. Other errors are
internal/dependency failures and must never be exposed as SQL error text.
`Head` validates a resume cursor against membership and the durable head.
`CanSeePresence` authorizes bounded peer lookups. The separate server/realtime
packages implement HTTP, Redis, WebSockets and connection leases.

`live.go` also provides internal dispatcher reads, not public authorization-free
operations. `DeliveryState` captures committed head and member receipt positions
in one statement. `DeliveryMessages` reads immutable rows only through that head.
The shared dispatcher validates contiguous results and keeps a bounded tail;
public cached recovery still calls `Head` for current membership and falls back
to `History` on incomplete coverage. See `validation-shared-delivery.md` for
measurements and limits. The executable retains up to 32 idle connections within
its existing 32-open-connection limit to avoid backend churn between poll bursts.

## Integration tests

Requires Go 1.26.2 and PostgreSQL 17. Use a disposable database; the test role needs
permission to create/drop schemas and create functions. Each test applies the
actual initial migration in a uniquely named schema, then drops that schema.
Tests skip explicitly when `TEST_DATABASE_URL` is unset; a skipped run is not
storage correctness evidence.

From `submission/` in PowerShell:

```powershell
docker run --name phi-storage-tests -e POSTGRES_PASSWORD=phi_test -e POSTGRES_DB=phi_test -p 127.0.0.1:55432:5432 -d postgres:17-alpine
$env:TEST_DATABASE_URL='postgres://postgres:phi_test@127.0.0.1:55432/phi_test?sslmode=disable'
go test -race -count=3 -timeout=120s -v ./...
```

The container must be ready before running tests. Stop it with
`docker stop phi-storage-tests` when finished. These credentials are only for this
local disposable database.

Coverage includes direct-pair races, identical send retries, sender-scoped keys
conflicting across conversations, contiguous concurrent sends, monotonic concurrent
read updates, unread counts excluding own messages, cursor history during new
arrivals, access checks, group limits, Unicode body limits, and injected outbox
failures proving send/read rollback. These are correctness tests, not load results.

Verified on 2026-10-22 with Go 1.26.2 (Windows amd64) and PostgreSQL 17 in Docker:
all seven tests passed three consecutive runs with `-race` (24.483 seconds total).
The local test container was stopped afterward; use `docker start phi-storage-tests`
to reuse it, or create a new container if it has been removed.
