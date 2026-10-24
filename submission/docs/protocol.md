# Messaging protocol and correctness contract

Status: implementation contract. PostgreSQL storage, HTTP, WebSocket sessions,
Redis hints, transactional outbox publishing and connection leases are implemented.
See `validation-shared-delivery.md` for verified behavior and remaining limits.

## Identity and limits
Local users receive seeded opaque bearer tokens. Authentication resolves a user from server configuration; request bodies cannot select the acting user. This is a local mock identity provider, not production authentication. Authenticate HTTP and WebSocket handshakes and check membership on every conversation operation. Reject untrusted WebSocket origins. Never put tokens in URLs or logs.

UUIDs are canonical lowercase, hyphenated strings. Sequence values are decimal strings in JSON to preserve bigint precision in JavaScript. Message bodies contain 1-4000 Unicode characters, must be valid UTF-8 and cannot contain U+0000 (unsupported by PostgreSQL text). Request bodies and WebSocket frames have finite size limits. Groups have 2-100 distinct members including the creator. Membership is fixed for this exercise; adding/leaving groups is outside the required scope. Direct conversations have exactly two distinct users.

## HTTP operations
- POST /v1/conversations: {kind, member_ids}; returns conversation. For direct conversations, canonicalize the sorted user pair as direct_key and reuse an existing conversation under concurrent creation. Create conversation and members atomically. Validate the group member limit before insertion.
- GET /v1/conversations: authenticated user's conversations with unread_count and last_sequence; keyset pagination by conversation UUID, default 50, maximum 100.
- POST /v1/conversations/{id}/messages: {client_message_id, body}; returns committed message. New sends return 201; identical retries return 200. Reusing an ID for a different body or conversation returns 409.
- GET /v1/conversations/{id}/messages?before={sequence}&limit=50: newest first, exclusive cursor, maximum 100. Initial query captures current last_sequence; further pages exclude newer arrivals.
- GET /v1/conversations/{id}/messages?after={sequence}&limit=100: recovery in ascending sequence order, exclusive cursor. Cannot combine before and after. Paginate until caught up.
- PUT /v1/conversations/{id}/read: {sequence}; advances to max(existing, requested). Reject negative values and positions beyond the conversation head. Publish a read event transactionally through outbox.
- GET /v1/conversations/{id}/receipts: member IDs and durable read positions. Used to restore receipt state after missed live notifications.
- GET /v1/presence?user_ids=...: bounded lookup for online, offline and last_seen. Only allow users who share a conversation with the requester; maximum 100 IDs.
- GET /health/live: process health. GET /health/ready: dependency readiness.

Errors: JSON {code, message}. Use 400 for malformed input, 401 for invalid identity, 403 for rejected WebSocket origins, 404 for absent or inaccessible conversations, 409 for conflicting idempotency keys, 413 for oversized requests, 429 for rate limits, 503 for unavailable dependencies. No SQL or tokens in error messages. Only the configured bearer Authorization header authenticates a request; token query parameters are rejected. HTTP JSON bodies are limited to 32 KiB and unknown fields are rejected. The application uses a five-second operation timeout.

## Durable send algorithm
1. Begin a transaction and lock the conversation row FOR UPDATE.
2. Verify membership. Look up sender_id plus client_message_id. Return the original message only when conversation and body match.
3. Increment last_sequence, insert the message and insert an outbox event in that transaction.
4. Commit before returning the acknowledgement.
5. On a uniqueness conflict caused by concurrent reuse across conversations, roll back and look up the winning committed message to resolve retry versus conflict.

The row lock orders committed sends within a conversation. Failed transactions roll back their sequence allocation. A lost acknowledgement is resolved by retrying the same client ID. The same ID reused in another conversation cannot create another message.

Unread count is the count of messages from other users with sequence greater than the member's read_sequence. Own messages never count as unread. Compute conversation head and unread count from one SQL statement snapshot. Read updates and outbox insertion share a transaction. The service, not the schema alone, enforces membership cardinality and read positions bounded by the head.

## WebSocket delivery
GET /v1/ws upgrades an authenticated connection. HTTP handles durable sends and reads; WebSocket carries live message and read events. Each frame includes type and conversation_id. Message events contain the durable sequence, sender_id, client_message_id, body and created_at. Read events contain user_id and read_sequence.

The server first registers the socket and sends `{"type":"ready","instance_id":"opaque-replica-id"}`. Within five
seconds the client sends one `{"type":"resume","cursors":{"conversation-uuid":"0"}}`
frame containing 1-100 conversation cursors. Each cursor is a nonnegative decimal
string, at most that conversation's current head. Membership is checked before
any events are emitted; invalid subscriptions receive a generic error frame and
close. Clients discover IDs through HTTP listing, and reconnect to change their
subscriptions. Additional application frames close the socket; WebSocket control
frames handle heartbeat and close. Frames are limited to 32 KiB. The `ready` frame
has no conversation ID because it describes the connection. Initial read events
restore current receipts, including zero positions.

After validating all resume cursors the server sends `{"type":"resumed"}` before
events. The client counts a connection as established only after that frame.
Instance IDs support diagnostics, not authentication. Reconnect delays retain
jitter at the ten-second cap (between half the current exponential base and the
base). Bad resume input ends the client run rather than retrying indefinitely.

Each node shares a bounded committed tail and receipt snapshot per subscribed
conversation. Redis hints refresh only that conversation; a one-second durable
reconciliation repairs lost hints. Sessions consume the snapshot in ascending
order, falling back to authorized PostgreSQL history for older cursors. Repeated hints cannot
move a session backwards. The session advances its transient cursor when an event
enters its outbound queue; that is not a client acknowledgement. After any socket
failure, the client resumes from its own last successfully processed contiguous
position. There is no ordering guarantee between different conversations, or
between read events and the message stream.

Register the live connection before catch-up. On opening or reopening a connection the client recovers each conversation over the after endpoint, merges live events by sequence, suppresses duplicates and only advances its cursor across contiguous sequences. A gap triggers another history request. Periodic catch-up also repairs missed Redis events when no later live event exposes a gap. New clients discover conversation IDs through the conversation listing.

Redis notifications are hints, not durable delivery acknowledgements. Outbox workers claim rows with FOR UPDATE SKIP LOCKED, publish and then mark published. Crashes may cause repeats. A successful publish with no active subscriber does not ensure delivery; database recovery remains mandatory. Dispatchers fetch and emit messages in sequence order per conversation. Bound outbound queues; disconnect slow consumers and let them recover. Load generator and sample client must implement this recovery protocol, rather than assume WebSocket delivery is inherently reliable.

## Presence and failures
Each device connection has an independent Redis lease refreshed by heartbeats. A user is online while any lease is valid. Use Redis server time and atomic scripts to prune expired leases and determine status. Keep last heartbeat timestamps after lease expiry; last_seen denotes last observed activity, not the exact crash time. Disconnecting one device must not mark another active device offline. Redis failure yields unavailable presence, never a fabricated offline status.

Defaults are a 30-second lease, server pings every 10 seconds, and renewal only
after the peer's pong. Normal disconnect removes that connection's lease; crashes
leave it until expiry. Unknown last_seen is null. Redis failure rejects new sockets
with 503 and can close existing sockets when a heartbeat cannot renew. Healthy
PostgreSQL HTTP sends/history remain available. Each node allows 1,000 sockets and
100 handshake attempts per second, including failed authentication attempts.
The fixed window limiter is per node. Outbound queues hold 128 events, writes
time out after five seconds, and a full queue closes the connection. Catch up by
HTTP before resuming a large backlog. Shutdown closes subscriptions and sockets,
cancels workers and waits within a ten-second process deadline.

Delivery adds a per-node limit of 128 distinct actively subscribed conversations,
128 cached messages per conversation, four refresh workers and 128 queued refresh
jobs. Resume admission is atomic across all requested conversations; exceeding
this limit sends an unavailable error and closes the socket. The last departing
subscriber removes its feed. HTTP after-cursor recovery always checks current
membership and head in PostgreSQL, even on a cache hit; stale, cold or insufficient
cache coverage falls back to durable history. Cache state never acknowledges sends.

PostgreSQL failure prevents acknowledgements. Redis failure can delay live delivery; durable history remains available when PostgreSQL is healthy. Reconnect clients use capped exponential backoff with jitter. Bound per-node connections and handshake rates. Three application replicas tolerate an application-node failure; a single Postgres or Redis container does not provide database or cache high availability.

## Acceptance evidence to produce
Concurrent identical sends create one row; conflicting IDs return conflict; concurrent sends have a single contiguous order; nonmembers cannot read or send; 101-member groups fail; direct-pair creation is race-safe; history pagination remains stable; own sends do not increase unread; stale read updates cannot decrease receipts; cross-node live delivery works; device leases expire after a crash; killing a node loses no acknowledged messages after client recovery. See `validation-2026-10-23.md` and raw results for actual failure/load evidence, including failed higher-load runs. No 10,000-connection result is claimed.
