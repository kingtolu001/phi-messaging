# Submission diagrams

Open the [diagram index](index.html), or use the standalone outputs below.

| Diagram | Native editable source | Standalone HTML | Static exports |
| --- | --- | --- | --- |
| Runtime architecture | [JSON](runtime.architecture.json) | [Runtime](runtime.html) | [SVG](runtime.svg), [PNG](runtime.png) |
| Durable send, acknowledgement and outbox | [JSON](send.sequence.json) | [Send](send.html) | [SVG](send.svg), [PNG](send.png) |
| Node failure, reconnect and recovery | [JSON](recovery.sequence.json) | [Recovery](recovery.html) | [SVG](recovery.svg), [PNG](recovery.png) |

The HTML files contain Archify's viewer, styles and fonts and can be opened locally
without a running messaging service. SVGs are native dual-theme exports; PNGs
are full-diagram light-theme exports. The index is navigation, not a fourth diagram.
Cards beneath/beside each HTML diagram contain limits and supporting detail;
canonical SVG/PNG exports contain the drawing and legend, not those HTML cards.

## Scope and implementation review

The runtime overview groups app1/app2/app3 because they execute identical code.
There is no separate worker container: each app has one outbox publisher, one Redis
listener, four feed-refresh workers and a one-second reconciliation timer. Each
socket additionally has reader/writer/catch-up work. Send diagram participants
separate these roles without claiming they are separate services. The receiving
app can differ from the sending/publishing app.

The send sequence shows a new-ID path. Identical retries return the prior
committed result; conflicting reuse returns 409. Only a successful new-send commit
permits acknowledgement. ACK and outbox publication are independent after that
commit; ACK-first is an illustrative interleaving. Publication can repeat and
does not prove that any recipient received a message.

The recovery sequence distinguishes the client's processed cursor from the
server's queued cursor. WebSocket registration/resume precedes the Go client's
HTTP catch-up. HTTP may reach a different healthy replica from the socket. HTTP
recovery always checks PostgreSQL membership/head, even with a complete in-memory
tail; old or stale cache coverage falls back to history. Server feed reconciliation
and client HTTP polling independently run every second. Read positions persist
in PostgreSQL; current read events replay, although the sample client ignores
those event frames and processes messages only.

Redis carries conversation hints and per-connection leases, not authoritative
message history. Presence pruning occurs on Redis lease/presence operations, not
through a separate reaper service. Multiple devices remain online while any lease
is valid. Single PostgreSQL, Redis, Nginx and host failures are distinguished from
losing one application process. No diagram implies dependency HA, durable sample
client cursors or a successful 1,000-device load result.

[Implementation evidence](implementation-evidence.json) records SHA-256 hashes,
reviewed function ranges and responsibility claims for 15 actual source/config
files. `diagram-evidence.mjs` detects later drift; it does not prove semantics.
The reviewed implementation contains uncommitted changes, so native
`--repo-root` commit verification was not run against these working bytes. No
revision is claimed to contain those edits and no project commit was created.
Code correspondence is manual review
plus local file hashes, separate from Archify's automated diagram checks.

## Tool and regeneration

[Archify](https://github.com/tt-a1i/archify) 3.2.0 was installed as a project-local
standalone skill in `../../../.tools/skills/archify/`, at upstream revision
`25cf37080fc8b1dbe758a4efc3935b55f3327083`. [Tool metadata](archify-tool.json)
records that pin and package file hashes. Installation and `SKILL.md`, repository
authoring, schemas, examples and delivery/export instructions were read before
authoring. The archive installer initially ran out of disk; the completed requested
Git checkout supplied the standalone package, and only that temporary checkout
was removed. No global tool configuration was changed.

From `submission/`, with Node 18+ (22.20.0 used), Git, PowerShell and local
Chrome/Chromium or Edge:

```powershell
# Verifies an existing package; otherwise downloads the pinned upstream package.
./scripts/install-archify.ps1
node scripts/diagram-evidence.mjs
./scripts/diagrams.ps1 -Review my-reviewed-revision
node scripts/check-diagrams.mjs
```

Use a fresh `-Review` name for every changed artifact. The script runs Archify
`finalize` sequentially at `showcase`, then generates SVG/PNG through the viewer's
native `exportMenu.run` implementation. It records current artifact-bound receipt
paths in `validation.json` and export byte/hash bindings in `exports.json`.
`ARCHIFY_HOME` can point to another verified local package. Update checks are
disabled during generation; no publishing, upload or external preview is used.

Edit JSON sources, never generated HTML/SVG. If Go/config hashes drift, inspect
the changed implementation and revise the diagrams before explicitly recording a
new reviewed snapshot with `node scripts/diagram-evidence.mjs --record`.
Blindly re-recording hashes is not a correctness review. Then rerun the generation
script. The exporter uses the pinned tool's browser/export APIs; upgrading Archify
requires reviewing those adapters and the tool pin.

For additional native screenshots, after a successful finalize:

```powershell
node ../.tools/skills/archify/bin/archify.mjs visual-check docs/diagrams/runtime.html --summary --require-provenance --out-dir docs/diagrams/review/my-visual-review
```

Inspect the captures before claiming perceptual review. They are viewport captures,
while the PNG static export covers the complete drawing. Native receipts contain
local absolute paths; after moving the workspace, regenerate them. Their hashes
bind the original checked bytes, not arbitrary later edits.

The final check verifies current JSON/HTML receipt hashes, all four passed native
gates, six export bindings, the implementation snapshot and authored local links.
Tool/font notices are retained in [Archify MIT](licenses/Archify-MIT.txt) and
[JetBrains Mono OFL](licenses/JetBrainsMono-OFL.txt).

## Historical verification — 2026-10-24

- All three diagrams at that date passed native `finalize`: JSON/schema/layout
  validation, delivery, strict artifact/provenance checks and real Chrome browser
  checks. The current receipts are enumerated in [validation.json](validation.json).
- The complete regeneration/export script ran successfully; all six native SVG/PNG
  exports were generated and byte/hash bound to their HTML. PNG signatures and SVG
  roots were checked, and the final full light PNGs were visually inspected.
- Native light/dark viewport captures were also collected during authoring. Those
  captures belong to their recorded earlier artifact hashes; automated browser
  gates, not those older captures, establish the current artifacts' viewport checks.
  No manual dark-theme perceptual review is claimed.
- A send-note overlap was repaired before delivery. The first recovery browser
  attempt failed Chrome cleanup; its failed root receipt is retained. A later
  unchanged-artifact retry and the final regeneration passed. Use `validation.json`
  for current results rather than treating every historical sidecar as current.
- The installer verified the existing pinned package. The fresh-network bootstrap
  branch of `install-archify.ps1` was not re-executed; initial installation used the
  completed checkout as described above. Application/load tests were not rerun for
  this documentation-only change; prior performance failures remain unchanged.

For that earlier diagram-only delivery, no application code, migrations or
dependency-HA configuration changed. No commit, push or external publication was
performed for it.

## Verification performed — 2026-10-25

The current sources include lease batching, negotiated shared PostgreSQL repair,
and explicit default versus scale bounds. All three native finalize runs passed
validate, deliver, strict check and browser-check under
review/users20k-recovery-20261025/. All six SVG/PNG exports were regenerated;
check-diagrams.mjs passed source/HTML hashes, gate receipts, export bindings and
local links. The light PNG exports were inspected. No new manual dark-theme
review or Git-pinned source verification was performed. Earlier dated receipts
remain historical evidence. The working-file evidence now covers 15 files.
