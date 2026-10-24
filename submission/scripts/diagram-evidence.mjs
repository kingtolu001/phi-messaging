import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const output = path.join(root, 'docs/diagrams/implementation-evidence.json');
const reviewed = [
  ['compose.yaml', [], 'Three apps and single PostgreSQL, Redis and Nginx containers.'],
  ['config/nginx.conf', [], 'HTTP/WS forwarding, round-robin upstream and no proxy POST retries.'],
  ['cmd/server/main.go', ['func run('], '32 SQL connections; workers belong to app process; bounded shutdown.'],
  ['internal/server/server.go', ['func New(', 'func (s *Server) listen(', 'func (s *Server) publish('], 'One publisher/listener in each app, default limits and targeted hints.'],
  ['internal/server/delivery.go', ['func newDeliveryHub(', 'func (h *deliveryHub) attach(', 'func (h *deliveryHub) refresh(', 'func (s *Server) recoveryPage('], 'Four workers, one-second sweep, bounded shared tails and authorized HTTP fallback.'],
  ['internal/server/websocket.go', ['func (c *session) enqueue(', 'func (s *Server) acceptWebSocket(', 'func (c *session) writeLoop(', 'func (c *session) catchUp('], 'Membership before events, ready/resume/resumed, leases, queued cursor and ordered delivery.'],
  ['internal/server/http.go', ['func (s *Server) auth(', 'func (s *Server) routes('], 'Bearer authentication, Send before success response, history and receipt endpoints.'],
  ['internal/storage/storage.go', ['func lockConversation(', 'func (s *Store) Send(', 'func (s *Store) MarkRead('], 'Conversation lock, idempotency, atomic message/head/outbox commit and monotonic reads.'],
  ['internal/storage/delivery.go', ['func (s *Store) Head(', 'func (s *Store) PublishNext('], 'Fresh membership/head checks; SKIP LOCKED publish then mark in one transaction.'],
  ['internal/storage/live.go', ['func (s *Store) DeliveryState(', 'func (s *Store) DeliveryMessages('], 'Committed head/receipt snapshot and immutable message tail bounded by head.'],
  ['internal/client/client.go', ['func retryDelay(', 'func (c *Client) Accept(', 'func (c *Client) Recover(', 'func (c *Client) Run(', 'func (c *Client) once('], 'Device owns cursor, callback-before-advance, dedup, gap repair, HTTP polling and jittered reconnect.'],
  ['internal/realtime/presence.go', ['func (l Leases) apply('], 'Redis TIME and Lua prune per-connection leases on presence operations.'],
  ['migrations/001_initial.sql', [], 'Authoritative tables, ordering and sender/client-ID uniqueness.'],
];
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
if (process.argv.includes('--record')) {
  const files = reviewed.map(([file, symbols, claim]) => {
    const bytes = fs.readFileSync(path.join(root, file));
    const lines = bytes.toString('utf8').split(/\r?\n/);
    if (lines.at(-1) === '') lines.pop();
    const ranges = symbols.map(symbol => {
      const start = lines.findIndex(line => line.startsWith(symbol));
      if (start < 0) throw new Error('Missing reviewed symbol: ' + symbol);
      let end = start + 1;
      while (end < lines.length && !lines[end].startsWith('func ')) end++;
      return { symbol, line: start + 1, end_line: end };
    });
    return { path: file, sha256: hash(bytes), claim, ranges };
  });
  fs.writeFileSync(output, JSON.stringify({ review_date: '2026-10-24',
    basis: 'Manual inspection of working files; whole-file SHA-256 drift detection',
    git_commit_verified: false, limitation: 'Workspace has no Git repository. Native Git-pinned source evidence is unavailable.', files,
  }, null, 2) + '\n');
} else {
  const evidence = JSON.parse(fs.readFileSync(output));
  for (const file of evidence.files) {
    if (hash(fs.readFileSync(path.join(root, file.path))) !== file.sha256) {
      throw new Error('Diagram evidence drift: inspect code and diagrams before recording new hashes: ' + file.path);
    }
  }
  console.log('PASS: ' + evidence.files.length + ' implementation file hashes match the reviewed snapshot');
}
