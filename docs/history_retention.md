# Scoped history retention

The policy-control foundation for [#513](https://github.com/hubuum/hubuum/issues/513)
implements immutable policy revisions, holds, bounded inspection and issued
preview/application under `project/workload-lifecycle`. Permanent hot-history
expiry and its execution/availability boundary are still being implemented.
These policy routes do not start physical deletion. Archive adapters are not a
prerequisite for hot-only expiry.

## Administrator operations

These routes require an unscoped human administrator and recheck the configured
authorization backend. Responses use `Cache-Control: no-store`.

| Method and path under `/api/v1/admin/history-lifecycle` | Result |
| --- | --- |
| `GET /policies?after_revision=0&limit=100` | Immutable policy page and independent-protection status. |
| `POST /inspect` | Bounded effective sources, original anchors, deadlines and blockers. |
| `POST /accounting` | Complete bounded logical bytes and independently protected pressure start/end transitions for an exact policy revision. |
| `POST /policy-previews` | Issued review of one exact proposed change. |
| `PUT /policies` | Revision-bound application with independent checkpoint protection. |
| `POST /holds` | Create an identity-bound hold with an optional fixed expiry. |
| `POST /holds/{hold_id}/release` | Release a hold without rewriting its creation or expiry clock. |
| `GET /operations/{operation_id}` | Bounded policy, hold, pressure transition or review evidence referenced by an audit receipt. |

Write/inspection JSON bodies are limited to 16 KiB. Inspection returns HTTP 404
for an unknown class or collection UUID. Preview issuance returns HTTP 429 when
the unexpired previews fill the registry or its remaining byte budget.

The policy request shapes follow the
[review contract](lifecycle_task_policy_contract.md#proposed-history-policy-boundary-for-513).
A preview request wraps its proposal in `change`; application sends `proposal`
and, when required, `review`. Unknown write fields are rejected. Revisions,
durations and logical byte limits use canonical unsigned decimal strings.
Retention durations support zero through 315360000 seconds. Logical size limits
must fit a signed 64-bit integer, and the lower target must be strictly below the
upper threshold. Timestamps use valid RFC 3339 UTC with `Z` or `+00:00`. Preview, policy and
hold clocks come from the adapter, using PostgreSQL time for database-backed
history. Application-host clock skew does not move these cutovers.

## Scope and original clocks

Policy keys select a history family and deployment, collection or class scope.
Collection history cannot have a class-scoped policy. Collection/class scopes
use stable UUIDs; an optional `current_id` must resolve to that same current
resource. A reused integer never retargets an older policy or hold. Unknown
scopes are rejected; a retained binding remains governable after class deletion.

Resolution uses class, captured historical collection, then deployment. `inherit`
removes an override for the stated application range; it does not merge fields.
`disabled_expiry`, `indefinite` and the absence of a configured source remain
separately explainable. Class moves do not rewrite captured attribution.

Prospective changes govern closures after the recorded adapter decision clock. Retrospective changes also cover
closures through an explicit cutoff no later than review issuance. The gap
between an earlier retrospective cutoff and application remains under its prior
rules. Late commits with old anchors are still covered: neither an allocated ID
nor a timestamp is a committed-data watermark.

Anchors explicitly distinguish closed intervals, deletion tombstones and schema
events. Inspection preserves the original anchor and derived deadline. Open
versions are blocked. Relation decisions require both captured endpoints to
permit expiry. Missing historical attribution, retained dependencies, holds,
protected minimums and unavailable logical-size accounting are explicit blockers.
No present-day parent lookup fills a missing historical reference.
Templates and remote targets may have no class. If a captured class reference
exists, expiry requires its UUID to be resolved. An unresolved class on a
retained schema revision also blocks expiry of a potentially dependent event.

Native deletion tombstones retain their original point interval
(`valid_to == valid_from`); older unbounded tombstones remain supported. Both
represent a deletion gap from that point onward, rather than an empty read
exclusion. Unknown restore ancestry is reported as unresolved inspection coverage
and cannot become an eligible preview candidate. Inspection and preview charge
already excluded copies against the scan budget and omit them from eligible
counts and samples.

Age, size, age-or-size and age-and-size are separate rule variants. Size pressure
uses measured logical bytes and persists hysteresis until its lower target is
reached. Administrators can refresh complete accounting through
`POST /api/v1/admin/history-lifecycle/accounting`, supplying the exact policy
reference (`key` and immutable `revision`) returned by the policy catalog.
The response reports its original observation clock and authority fence,
`compact_json_v1`, scanned and governed row counts, measured bytes, current
pressure, and an optional retained transition UUID. Responses use `no-store`.
The configured authorization backend is rechecked. Any transition must be
published and read back through independent recovery protection before success;
a protection failure can leave a committed transition pending protection.

The v1 measure counts the complete retained record's compact UTF-8 JSON before
compression, including its metadata and payload. Numbers use normalized plain
decimal spelling: no exponent, insignificant fractional zeros or negative zero.
Object ordering and source whitespace do not affect the count. String escaping
uses JSON's standard short escapes and six-byte control escapes. Nesting is
limited to 128 levels; totals must fit signed 64-bit bytes. PostgreSQL computes
this measure inside the database and streams only identity/anchor metadata to
the adapter. Each adapter measures its retained record representation; this is
not a portable archive length, PostgreSQL heap/index size or disk-space promise.

A refresh exhausts all retained rows in the family in one serialized control
transaction. It includes open versions, holds, dependency-protected records and
copies already excluded by an expiry intent. Original captured ownership and
closure/deletion/schema-event clocks determine the effective immutable policy;
open versions use the observation clock only for accounting resolution and never
acquire a closure proof. More-specific overrides are excluded from the broader
policy's governed set. A relation governed by the same source at both endpoints
counts once for that source. A distinct source at each endpoint charges each
source once. Missing attribution, unknown restore ancestry and duplicate version
identities fail the entire refresh, including when a bounded sample looks clear.

Complete scans are limited to 100,000 rows, 1 MiB identity metadata per row and
64 MiB cumulative identity metadata. PostgreSQL uses a statement timeout of at
most five seconds; both adapters enforce a five-second complete-fold deadline.
Exhausting a budget returns unavailable and commits no partial total or pressure
transition. This bounded full scan is an initial accounting implementation;
larger scopes need transactionally maintained counters and separate qualification.
The memory adapter measures immutable resource fields when it records or restores
a history version. Each scan adds the current interval and identity metadata.
An oversized version still contributes to logical pressure even when the expiry
worker cannot select it within the input budget.

Pressure starts strictly above the upper threshold, remains active above the
lower target, and ends at or below that target. Only start/end transitions create
new authority and canonical audit events; intermediate gauges and exact retries
create no new receipt. Each exact immutable policy revision has its own episode.
The append-only journal reserves a terminal slot and control bytes for every
active episode, and preserves original actors, clocks, policy references and
pressure through signed recovery. Operation inspection returns exact retained
transition evidence. Pressure alone changes no history visibility or expiry
membership.

New pressure receipts record the observation source (`administrator`,
`manual_run`, or `scheduled_run`). Their `actor` field identifies the
initiating principal. Worker audit events identify the worker as the immediate
actor and preserve the initiating principal and trigger. The audit policy ID
stays stable across edits, and the revision identifies the measured binding.
Receipts predating the source field replay their original audit metadata.

The gated executor implements oldest-eligible size selection and protected exact
cleanup; production activation still requires qualification. Existing inspection and policy previews
continue to report size accounting as unavailable when it would be required for
an executable expiry decision; their bounded samples are never complete gauges. A proven
age condition suffices for age-or-size; a false age condition makes age-and-size
false without consulting size.

A preview that scans all 1,000 allowed rows reports bounded coverage. Stored
previews count each sampled version once. The sampled eligibility counts,
endpoint deadlines and blockers must agree with the preview at issuance.

An initial size pass folds the complete family, including all effective size
sources, then exhausts the same unchanged family again under the history control
gate. Each pass is capped at 100,000 rows; both share the original five-second
planning clock and cumulative 64-MiB metadata budget. The result reports all
source examinations (up to 200,000), rather than presenting accounting work as
a 1000-row source page. Its oldest-prefix heap retains at most 1000 candidates
and 1 MiB of metadata, with the existing 16-MiB native input, 1-MiB manifest and
reserved completion budgets. No historical payload enters that heap.

Ordering uses the original closure/deletion/schema-event clock, then stable
resource/version UUIDs. Holds, open versions, dependencies, unresolved identities
and exclusions remain protected. Pending unreceipted exact copies reserve their
fresh logical bytes against each policy that counts the copy in the current census; a shared relation
source is charged once. Selection stops adding size-dependent copies at or below
the lower target. One indivisible version can cross that target. Independently
due age work can continue. Individually oversized sources remain untouched.

Each size-dependent member retains its pressure episode, complete observed bytes
and reserved bytes. Administrator operation evidence includes these bounded
witnesses without disclosing retained object names or payloads. The original independently protected exclusion remains a
cleanup obligation after pressure ends; ending pressure does not resurrect that
copy or invalidate its first receipt. Removal still rechecks exact native facts,
current governing policies, holds, minimums and dependencies. Recovery rejects
missing or foreign size witnesses and preserves original authorization through
physical replay. A size pass that selects nothing still protects any new pressure
transition before task success, including after restart.

Only read-only planning can fall back when complete accounting is unavailable.
That path considers independent age eligibility and reports
`size_accounting_unavailable` in task progress. Required audit and intent-commit
failures roll back the transaction. Existing finite native-position continuations
finish their original pass; new size selection starts at a fresh pass boundary.

## Review and protection

An issued preview binds the principal UUID, exact proposal, policy/protection
revision, application incarnation, coverage, original issue time and fixed
expiry. Its maximum lifetime is 300 seconds. Reading or retrying cannot renew
that lifetime. A matching digest alone is not issuance proof. Retrospective
application requires the stored review; any supplied prospective review is also
validated. Storage checks the issue time, expiry and hold protection under the
writer lock before acknowledging a preview. If the review has already expired,
issuance returns HTTP 409 with `preview_expired`. If a hold expired during
the scan, issuance returns `stale_preview`.

Recovery verifies that each retrospective policy retains its original review,
that the cutoff was valid when the review was issued, and that the recorded
authority revision and hold protection permitted application. A stored pending
review cannot name a future history revision or a future deletion revision in
the current application incarnation.

Policy, hold or deletion authority changes and application recovery invalidate
old reviews. A hold expiring between issuance and application also makes the
review stale. Application returns HTTP 409 with `stale_preview`,
`preview_expired` or `preview_mismatch`; unknown/inaccessible references use 404.
A conflict after a lost response does not prove the first application failed:
inspect the immutable applied record and its retained review before reviewing
another change.

Inspection scans at most 1000 source rows and 1 MiB of identity metadata, and
returns at most 16 payload-free samples. Coverage distinguishes complete from
bounded scans. In the memory adapter, several history families share one source,
so rows from other families can consume the row limit. With bounded coverage,
eligible records can exist beyond the scan even when the sample is empty.
A review covers its declared scope and cutoff, including
additional retained or late-committing records, rather than only sampled IDs.
Issuance reserves no later execution capacity and does not pin candidates.
If authority changes during inspection, the endpoint returns HTTP 409. Retry
the inspection to read the current policy and hold decisions.

Configure and initialize the existing
[independent control repository](deletion_recovery.md) before applying policies
or creating/releasing holds. The service commits authority, publishes a signed
checkpoint, reads it back, and only then acknowledges protected application.
A publication failure leaves an inspectable pending outcome. A primary behind
the signed deletion, task, or history authority requires recovery. Policy listing
reports `protected`, `pending`, `recovery_required`, `unconfigured` or
`unavailable`; it does not equate a committed database row with independent
protection.
A freshly initialized checkpoint covers pristine history authority at revision
zero. Its protected history revision stays absent until a history decision is
published.

A repeated hold release returns its existing terminal state after either an
explicit release or a recorded natural expiry. A first transition still
requires the expected authority revision.

## Pending execution boundary

Fully online expiry is the implementation target. Maintenance/drain batches are
not approved. No runtime expiry activation follows from configuring these policies. Manual and
periodic execution must eventually share one durable bounded executor; scheduled
batches do not require per-batch human approval. They do require an independently
demonstrated safety boundary before activation.

The fully online boundary needs exact closed-version intents whose
protected membership excludes those versions from history, as-of, export,
backup and other publication paths while normal work continues. Preparation
must serialize with holds, current policy revisions and retained dependencies,
reject open versions, and capture immutable UUID membership and original anchors.
Only independently checkpointed intents may authorize physical removal. Claim,
cancellation and restart must retain unfinished membership; cancellation cannot
release an already effective exclusion or erase a cleanup obligation. Completion
must record exact outcomes and independently protect them. Recovery must reapply
all retained exclusions and replay original audit before application exposure;
no older backup or stale publication permit may resurrect a removed version.
Each of the nine families needs native reader/writer/concurrency evidence for
this boundary. The existing offline object-history cleanup does not provide it.

A bounded maintenance alternative would interrupt application availability for
each batch: deny new work, drain and fence every runtime, then prove no runtime
or producer can publish from the selected histories before performing protected
cleanup. Merely changing readiness or pausing the expiry worker is insufficient.
It would need explicit activation approval, a bounded interruption contract,
durable cancellation/restart semantics, and independently protected completion
and recovery before reopening. It cannot invoke the existing offline-only
cleanup while any runtime remains active. This alternative is not approved.

Neither boundary relies on a filesystem or S3 artifact adapter. Both preserve
delete/restore authority, holds, protected minimums, original anchors and the
revision-bound review/application contract. Complete bounded accounting and
persistent pressure observation are available to administrators. Size-driven
oldest-first selection and protected cleanup are implemented in the gated executor;
broader concurrency and resource/liveness qualification remain activation gates;
inspection results and pressure receipts are not executable membership manifests.
P6 remains deferred under #561, and production expiry remains disabled.

### Request ownership and publication lifetime

The agreed productive PostgreSQL order is deletion/application authority, the
applicable history read/control authority gate, API request-owner `FOR SHARE`,
separate history publication/task ownership, then exact rows ordered by family
and UUID. The history lock mode must be chosen at entry before API-owner
validation, rather than acquired inside caller query code. A specialized history
entry belongs to #513; API validation and exact-owner settlement belong to #521.
Their shared helpers remain private to the PostgreSQL runtime/context.

| Entry or obligation | Lifetime and exception |
| --- | --- |
| API-owned history read/control | Specialized entry validates application authority, locks the chosen history mode, then validates API ownership before productive work. |
| Generic control transaction or task-lease connection | API-exempt; exemption never bypasses deletion/history safety. It cannot substitute for an API-owned history entry. |
| Initial API admission | No owner exists yet: application/deletion gate, API policy lock, live principal/incarnation, then owner insertion and cumulative charge. It acquires no history lock afterward. |
| API occupancy settlement | Exact-owner deletion outside productive/global/history transactions; waits productive shared holders, acquires no application/history/policy lock. |
| History publication | Separate guard survives inner response buffering and outer transmission. Inner body EOF and API occupancy settlement do not prove publication completion. |
| Publication settlement | Does not reacquire an already retired API owner. Ambiguous or crashed producers retain their obligation until positive fencing/isolation evidence. |

For history-aware registered runtimes, the HTTP application path admits a
publication after API admission, retaining its exact application/API/task scope.
A response extension carries its guard through bounded API buffering into the
outer body wrapper. Producer bodies retain their original execution scope after
the handler returns. Materialized bytes or successful stream EOF can confirm
producer completion; a dropped or failed producer remains unresolved.

API occupancy settles independently. Exact API, publication and producer
settlement does not acquire a fresh application permit or reacquire an old API
owner. The publication guard remains charged until both producer completion and
positive same-worker driver release are observed. The deferred connection
witness runs after synchronous HTTP/1 dispatcher destruction, including its I/O
and write buffers. Lost channels, deadlines and abort requests are not release
proofs. Failed or interrupted supervision retains durable ownership and prevents
clean runtime shutdown acknowledgement.

Each owned response closes its HTTP/1 connection. Requests already queued on
that connection receive HTTP 503 before actor resolution and publication
admission. On Unix, a native PostgreSQL test sends two HTTP/1.1 requests on one
socket and checks that the first response is delivered, the second is rejected,
and the first publication retires before runtime unregistration. The middleware
rejects HTTP upgrades, CONNECT, HTTP/2 and requests missing a transport witness
before publication admission. Selected
API work keeps its original deadline. Ordinary owned HTTP work retains an
inherited task deadline or receives a 60-second limit. A publication deadline
requests socket shutdown, followed by at most 30 seconds each for driver and
producer drain confirmation. Missing confirmation retains the obligation rather
than treating time elapsed as retirement.

Controlled-I/O tests exercise the real actor, API admission, maintenance and
publication middleware with both complete adapters: API occupancy has settled
while payload bytes remain buffered and the publication is still active.
Additional cases cover lazy stream scope preservation, cancellation before EOF,
lost completion witnesses, normal HEAD/empty-body suppression, Legacy
compatibility and transport rejection. Native
plain HTTP, Rustls and OpenSSL cases exercise successful delivery and positive
retirement through the production middleware and TLS configuration.

Handler-local ETag preconditions and export query budgets derive a private,
restricted context from the same publication. The native owner and original
epoch remain unchanged. A local scope can add a revision assertion or tighten a
query budget. Nested and captured work cannot clear or replace the assertion,
remove or enlarge the budget, or replace application, API, task or provenance
facts. Both adapters compose these restrictions through synchronous, asynchronous
and sendable execution scopes.

Backup and export dispatch admits its publication before principal resolution
and domain reads, retaining the supervised task deadline. Ownership spans
capture, spooling, joined serialization/rendering, verified staging and atomic
task/output completion. Errors before staging can settle after the operation
returns. Once staging starts, successful final publication or positively
completed abort is required. Failed cleanup, interrupted work and failed exact
settlement retain native ownership and prevent clean shutdown acknowledgement.
Publication-bound task completion retains its original task execution context;
stopped owned work cannot gain a new deadline exemption to publish output.
An abort failure is logged without losing the primary task error. A deadline or
worker lease loss cannot substitute for producer and driver completion.

Worker queue admission, execution-control admission, terminal lookups and bounded
output maintenance use separate awaited publication phases. They retain the
original application permit and never extend an executing producer's deadline.
Shutdown and lease loss request cooperative stop and await execution drain.
Task-output abort validates the original immutable attempt, task and claim token
without reacquiring productive admission; it cannot publish or read output.

Offline recovery retains each publication's original process-isolation domain
independently of runtime registration. Before registering a configured server or
administrator producer/executor, startup fixes the renderer supervisor to the
same stable runtime lock. Every queued and executing renderer pins a shared
open-file description and inherits it across exec; parent descriptors keep
close-on-exec enabled. Killing the parent cannot release exclusion while a
renderer descendant still holds that lock. Successful supervisor shutdown drops
its pin only after all children are reaped; uncertain shutdown retains it.

A Unix integration test uses a dedicated PostgreSQL database. It pauses a real
renderer and kills the server after the client receives the first chunk of an
unfinished owned HTTP response.
Offline recovery must preserve that exact publication while the renderer holds
the inherited lock. After the renderer exits, recovery retires the publication
and reapplication releases the database while the original runtime stays fenced.

The admitting runtime records the versioned `inherited_lock_v1` attestation in
its isolation descriptor. Exclusive process isolation plus the native application
fence can retire HTTP/export owners only with that original proof. A recovery
caller cannot upgrade older parent-only owners; they remain charged. Backup,
metadata and archive-read owners retain their existing parent-isolation rule.
Unknown domains/protocols and orphaned ownership block recovery. Claimed external
publication outcomes remain charged until independently reconciled. PostgreSQL
recovery refuses lock contention instead of waiting in an exact-settlement cycle.

A committed archive claim pins its original capture while a filesystem producer
stages bytes. If that process dies, offline recovery retains the claim and
reapplication keeps the database quarantined. Reopening the filesystem store
with the original intent can identify and fence the same attempt.

For a filesystem-bound v2 claim, stop the replicas and run `hubuum-admin`
with `--recover-control DIRECTORY --control-checkpoint-key KEY`,
`--archive-cleanup-publication PUBLICATION_UUID` and
`--archive-cleanup-root ABSOLUTE_PROVIDER_ROOT`. Use the original private root
on the original filesystem. Recovery removes the unpublished attempt in batches
of up to 16 blocks and syncs its tombstone. If the producer stopped after
recording ownership but before staging, recovery checks the empty attempt,
finishes reserving its IDs and writes the tombstone directly. A missing owner
record, conflicting index or unexpected payload keeps the claim quarantined.
The provider then answers the
per-attempt commitment recorded before admission. Control verifies the exact
claim before retiring its parent and child and unpinning the original capture.
The database stays quarantined until ordinary signed `--reapply-control` passes.

If the producer exited before reporting its publication UUID, the recovery
database role can list outstanding claim IDs without reading captured payloads.

```sql
SELECT publication_id
FROM hubuum_control.history_archive_publication_claims
ORDER BY publication_id;
```

A missing issuer, an unbound v1 intent, a copied root, a published attempt or
unaccounted files leave the claim charged. Preserve the original root, including
its private `issuer` file, and upgrade all writers before using bound intents.
After a lost acknowledgement, ordinary reapplication still checks all retained
claims. If the claim persists, retry cleanup against its original root. The
receipt covers files managed by this local store. Backups and independent
copies have their own retention obligations.

Production composition still registers Legacy, so these integrations do not
activate online expiry. Restored descendants, external-claim reconciliation, complete worker
entry/cleanup qualification, all-family native races and measured
resource/liveness budgets remain acceptance gates. No timer or absent API-owner
row can substitute for these proofs.

The online model bounds selected membership to 1000 rows
and 16 MiB measured full-row input, with a 1 MiB encoded membership envelope.
Retained manifest, journal and control byte budgets include the spaces after
JSON separators used by PostgreSQL's `jsonb` text representation, including
reserved completion capacity. Wire JSON remains compact.
It preserves anchors and exact UUIDs, retains compact governing-policy references
into the immutable catalog, re-evaluates eligibility against captured authority, and rejects duplicate/open
membership. Retained intent modeling preserves exclusions on cancellation and
completion, retains first removal clocks/checkpoint identities, and merges
compatible recovery descendants without losing local outcomes. Recovery checks
each retained selection against the policies, holds and size pressure at its
original authority revision and selection time. Policy catalog revisions must
be consecutive. Hold and pressure transitions occupy their recorded authority
revisions, and a pressure observation proves its source policy already existed.
A matching active hold, a superseding governing policy, or a pressure episode
that had already ended keeps the database quarantined. Later policy and hold
changes preserve valid earlier selections.
The journal permits
at most 256 retained intents within 4 MiB and reserves 512 encoded bytes per pending removal plus 256 bytes per unrecorded
cancellation. These are bounded source contracts, not measured resource
acceptance. These types are now included in typed protected history authority and the memory adapter's
control state. Focused memory tests cover exact object list/as-of/backup exclusion
and continued newer-version reads with a runtime registered. Both adapters now provide a separate bounded native selection/admission operation.
It requires the original OnlineV1 application scope, a matching authority fence
and an entirely history-aware fleet. PostgreSQL measures complete JSON-encoded
rows in a bounded recursive scan that stops after the first over-budget row;
only identity/authorization
metadata within 1 MiB crosses the adapter boundary. Selected native input is
bounded to 16 MiB. Original intervals, UUIDs, policy revisions and object
authorization facts are retained atomically with the canonical audit and read
exclusion projection. Current/open restored copies and retained schema revisions
block admission. New relation, computed and schema versions capture the required
class/collection UUID attribution when written. Closure and same-version refresh
preserve those facts; older missing attribution is never reconstructed from
current ownership. An uncertain retry must inspect the original batch identity; it
cannot silently select another batch. No batch within a bounded scan is not proof
that the source is empty.

Admission returns optional immutable membership plus bounded scan progress, even
when the page selects nothing. Both adapters advance past held, open, unresolved,
already-admitted and individually oversized source rows without replenishing the
examined-row budget. A row that can fit a fresh byte budget is deferred to the
next page. A pass retains its original finite native boundary while new rows
arrive, and starts again at the beginning after `PassFinished`. The boundary is
a traversal hint, never a committed-data watermark or proof of an empty source.
Later passes revisit late commits, older open versions that close later, and
changed protections. Continuations retain their original run, scope, origin and
application/deletion fence; every page rechecks current governing authority.
This is a breaking SDK return-type change; see the adapter upgrade guidance.

Native tests exercise trigger-created rows from all nine families, held/stale
selection, mixed fleets, oversized payloads, required audit rollback and retained
publication charging. Queued-admission races also cover all nine families: an
actual hold, policy edit or competing expiry batch retains the authority lock
while its required audit append is blocked. After that transition commits, the
waiting admission must reject its stale fence without publishing an exclusion
or overwriting the winner. PostgreSQL lock observations establish both waits;
elapsed time alone does not establish their order.

Native tests race the first physical removal from an admitted batch against a
hold or a policy edit in all nine families. A hold that commits first preserves
the exact copy without a receipt. So does a reviewed edit to its governing
policy with a cutoff at the copy's retention anchor, even when the new revision
keeps identical expiry terms. A prospective edit, or a retrospective cutoff one
microsecond before the anchor, leaves the original revision in force and the
worker can remove the copy. When removal commits first, the prepared hold or
policy edit conflicts. Each successful removal leaves one receipt identifying
the exact version and its protected checkpoint in the original batch.

Bounded-pass tests cover held prefixes and later appends on both adapters
across all nine families, plus native
late commits for all nine and late closure for the eight interval families.
Native dependency tests cover all nine families through current rows, protected
open intervals and retained schema revisions. They restore descendants twice,
also exercise missing derived-version ancestry, and require the bounded pass to
skip the dependency without changing either copy. Clearing the protected copy
lets a later finite pass select the original UUID and interval. Native restore
exposure tests independently admit the batch first, then use the trusted restore
identity branch in the runner's isolated restore database. A clean newer copy
passes the gate; a descendant of admitted history keeps exposure quarantined
without changing the source, restored copy or retained authority. These fixtures
qualify native admission and exposure checks. The signed full-history restore
test covers both PostgreSQL lock orders under age and logical-size policies
for all nine history families. The fixtures update collection and class
descriptions, rename objects, change class-relation labels, delete and recreate
object relations, replace export templates, update remote targets and change
computed definitions. The schema fixture stages and abandons a revision, then
deletes its class so the revision no longer protects the class's history events.
The tests compare the backed-up resource and version identities with the
current and historical identities after each restore. A sibling class retains
its closed object version. The retained manifest preserves each admitted
object's original name, version and ownership.

When restore holds the lock first, it fences the queued expiry request, even
on a second restore with existing receipts. When expiry holds the lock first,
the confirmed restore waits for removal and its first receipt to commit. The
backup then reintroduces that exact version under quarantine. Signed replay
removes it before release and preserves the original receipt, including its
checkpoint and removal time.
Primary admission is not independent checkpoint protection:
its authority must be captured and independently published before the gated
executor can remove a copy. Complete
native concurrency qualification, complete restore execution, production cleanup
activation and measured resource/liveness acceptance remain unfinished.

### Executor implementation and acceptance map

The dedicated `history_expiry` kind now has a closed version-1 payload and
checkpoint reader in SDK 0.20. The action is `expire_configured_hot_history`.
`process_history_expiry_task` commits native admission, canonical audit, read
exclusions, task checkpoint and resident accounting together. Its run UUID is the
original task UUID. A replacement lease resolves that exact retained batch before
any further selection; missing or changed correspondence fails closed. Unknown
payload/checkpoint versions remain queued on both adapters. A finished empty pass
does not become a watermark for a later run.

Manual selection pages require current configured unscoped administrator
authorization tied to the original principal UUID. Recovery can preserve an
already admitted batch after that principal is deleted. The executor independently
publishes and reads back authority before removing an admitted batch, then reuses
that immutable membership proof across exact-copy transactions. It removes at
most one exact closed copy per transaction after current policy/hold,
dependency and positive pre-exclusion publication retirement checks. Deletion,
first receipt, canonical audit and task checkpoint commit atomically. Missing
copies cannot manufacture a first-removal clock. Existing receipts are preserved
on retry. Ordered receipt lookups preserve the manifest's original traversal
order without repeated full receipt scans. Fresh independent read-back must cover
every receipt before a task completes
its one admitted batch; a later task starts a fresh pass. Failed protection or a
blocked removal keeps the original batch waiting. The last-copy adapter result
requests fresh receipt protection without a per-copy task-progress read. This
transient request is not persisted or completion proof; fresh execution always
protects the retained batch on entry. Repository acquisition retries
only contention under the original execution deadline; authentication failures
remain terminal. Cancellation
does not undo exclusions, discharge copy obligations or imply physical cleanup.

Qualified expiry workers reconcile retained batches through
`recover_history_expiry_task`. Waiting tasks resume with the same checkpoint and
pinned deadline, with a minimum 30-second retry interval. Cancellation and timeout
remain terminal; if their obligations survive, or the original task was purged,
a separate internal cleanup task adopts the exact retained batch. Recovery
payload v2 names its original run and batch, preserves original initiation and
first-removal receipts, and cannot select new members. Competing workers serialize
adoption, including an active-owner uniqueness constraint in PostgreSQL. Its
non-secret batch reference survives terminal request/checkpoint redaction in task
discovery metadata. Cleanup
still requires independent membership/receipt protection and the executor's
current policy, hold, dependency and publication checks. Stale leases stay fenced.
Offline replay retains the signed proof for copies with an existing first
receipt, without a composite checkpoint capture per unchanged physical replay.
The adapter still validates the exact deletion and complete unchanged control;
new first receipts receive fresh independent protection before the next copy.
Final quarantine release still requires complete protected authority and no
remaining excluded copies.
Recovery checks every affected current/open inventory under the application and
history authority locks. Native-key pages transfer at most 1,000 identities and
1 MiB each; the first deferred identity is rechecked on the next page. Projection
repair and all pages retain one five-second clock, respecting tighter inherited
timeouts. A single oversized identity, missing or malformed correspondence,
excluded ancestry, incomplete traversal or timeout keeps quarantine intact.
Completing one page cannot release exposure, and these checks never remove a
current or protected-open copy. Admission dependency checks retain their existing
bounded contract.
A native PostgreSQL test interrupts a leased periodic worker after its first
collection-history removal. A signed logical restore brings all three closed
versions back into quarantine. The old worker cannot continue deletion there.
The coordinator replays the original batch, preserves the worker's first receipt
and releases the application after all three copies are gone. Recovery in the
new application incarnation leaves the completed batch without a cleanup task.
This case uses a drained worker. Crash retirement and external-claim
reconciliation remain separate qualification gates.

The history lifecycle migration appends the new per-kind capacity and weight in one
policy revision while preserving all existing limits, lanes and reservations.
Persisted eighth-kind work prevents downgrade. Stop older API, worker, restore
and administrator processes, back up the database, apply the migration and
refresh grants, then start matching eight-kind binaries. Include the new entries
in policy edits; preserve recovery evidence and roll
forward after new work is retained. Production workers still advertise their
existing seven kinds. Production manual/periodic admission and execution remain
gated until the remaining qualifications pass; configuring an expiry policy alone
does not enable either path.

The gated administrator `POST /api/v1/admin/history-lifecycle/runs` operation
accepts the same bounded family/scope/limit request as inspection. It returns 202
for a new task or 200 for the existing active scope owner, after rechecking
configured unscoped administrator authority. Both adapters serialize it with the
periodic producer and retained cleanup. A duplicate preserves the original task
and consumes no additional capacity. Discovery retains the stable scope through
terminal payload redaction, and PostgreSQL enforces active scope uniqueness with
a partial index. A busy owner requires retry; an unsupported owner is never
reinterpreted as permission to create another task.

Periodic admission examines at most 16 retained expiry policy roots per recovery
pass in round-robin order. It retains the immutable policy revision and original
initiator without requiring a new human approval. Later prospective overrides do
not erase earlier closure obligations; actual selection and removal still recheck
governing rules and holds. Retained cleanup takes priority over a fresh scan for
the same key. Both producers require original OnlineV1 admission; the production
fleet continues to register Legacy and does not enable them.

| Area | Implementation target and required evidence |
| --- | --- |
| Admission and authorization | Manual runs require the configured unscoped administrator authorization; periodic work uses its retained policy/root initiation. Preserve UUID authority after principal/task deletion, and reject stale application incarnations. |
| Scheduling class | Both triggers use the existing internal-work lane and shared persisted scheduling policy. Keep ordinary slots and the existing internal reservation unchanged; one-slot deployments retain shared mode. Exercise mixed ordinary/internal backlogs and fairness under replenished work. |
| Initial per-kind policy | One execution, 64 outstanding runs, 64 MiB resident capacity and one outstanding run per principal; weight one. The internal lane budget also applies; ordinary deployment/principal/collection lane budgets remain separate. The coordinated migration preserves configured limits for existing kinds. These defaults are not measured H1/H4 acceptance. |
| Coalescing | One active task per exact family/stable scope key, shared by manual and periodic triggers. Concurrent/retried submissions return the authorized existing reference without replacing its original trigger/principal/clock. No allocated-ID or timestamp cursor proves completion. |
| Ownership and restart | Native admission and the task checkpoint commit together. Replacement claims resume fixed membership and original clocks; an absent/mismatched checkpoint cannot replace an admitted batch. Lost producers/publications still require positive fencing; deadlines alone cannot release obligations or reservations. |
| Cancellation | Stop new selection and settle execution ownership while retaining every committed exclusion, pending copy obligation and original receipt. Terminal task cancellation does not prove physical cleanup complete. |
| Workers and downgrade | Advertise support only with the implemented payload/checkpoint reader. Older workers must leave unsupported work unclaimed. Upgrade SDK adapters and all producers/workers before enabling the kind; do not silently reinterpret or discard it on downgrade. |
| Metadata and discovery | Bounded payload-free progress identifies scope, run/batch, actual governing revisions, blockers and remaining obligations. Advertise enabled/configured state and effective limits only when supported; regenerate OpenAPI/classifier coverage with the kind. |
| Qualification | Require all nine native families, current/deleted/moved owners, held and changed-policy races, interval gaps, cancellation/restart/restore descendants, publication body lifetimes, split-role authority and measured H1/H4 plus affected scheduler/performance scopes. Preserve the #561 P6 deferral. |

The confirmed #521 helper split is an integration boundary, not an unmerged
compile dependency. No broader CI labels or deployment activation follow from
this plan. The final combined tree must revalidate entry modes, lock cycles and
publication settlement after API occupancy has already settled.
Native socket probes cover plain HTTP/1, Rustls and OpenSSL body completion and
backpressure on Unix. They retain a connection-data witness beyond body EOF and require
a same-worker deferred dispatcher-release signal after requesting socket abort.
Broader cancellation, crash retirement and measured resource/liveness gates
remain open.
Controlled-I/O regressions also retain both pipelined response payloads in the
actual dispatcher write buffer, with either both bodies at EOF or the second
body still pending. Awaited dispatcher cancellation must positively release the
body, I/O and deferred driver witness. This covers local dispatcher cancellation.
Windows backpressure did not confirm bounded retirement after socket shutdown.
History-aware HTTP therefore installs no connection witness on Windows and
rejects productive publication with HTTP 503 before admitting history ownership.
Native Windows endpoint tests require that rejection for plain HTTP and both TLS
backends. Legacy HTTP remains available. The failing local-abort mechanism is not
treated as qualified by peer disconnect or a longer fixture deadline.
A fixed-length controlled-I/O case also retains a large chunk in Actix's direct
vectored-write path, separately from its buffered response header. Cancellation
requires the same positive body, I/O and deferred driver release witnesses.
The embedded TLS certificate and key are public loopback test fixtures, never
deployment credentials or production trust material.
History-aware registration retains the existing deletion SHARE/application UPDATE
capacity ordering, then takes history-authority SHARE before checking retained
journal/index state. That locking read is required for stale repeatable-read
registrations: waiting on an application SHARE lock alone does not refresh their
snapshot. First-intent admission must lock deletion/application SHARE and history
UPDATE before checking every registered producer's declared protocol. Native
race qualification must exercise both orders and snapshot isolation modes.

## Audit, restore and upgrade

Policy creation/change, review issuance and hold transitions append canonical
lifecycle audit facts atomically with control state. Unchanged retries emit no
new fact. Applied policies retain their review metadata, original clocks and
operation identity. Hold expiry settlement records the original expiry as its
occurrence time and separately records when it was observed. Required audit
failure rolls back the transition. These facts currently use the administrator
system projection; lifecycle sink delivery remains rejected. Broader safe sink
routing and consumer acceptance remain tracked by
[#550](https://github.com/hubuum/hubuum/issues/550).

The protected control row has no cascading references into replaceable data.
Logical restore preserves authority, changes the application incarnation and
requires retained audit replay before exposure. Signed composite checkpoint
format 3 includes history authority while retaining readers for older formats.
Older executables cannot authorize recovery from the new checkpoint format;
roll forward once it is published. Migration rollback refuses to discard
retained history authority. Keep the repository and authentication key through
every supported recovery horizon.

The format-3 repository parser permits an additional 4 MiB for history authority.
The per-file encoded bound increases from 20.125 MiB to 24.125 MiB; the retained
repository bound (three files plus 64 KiB) increases from 60.4375 MiB to
72.4375 MiB. These are finite encoding/storage envelopes, not measured resident
memory or throughput acceptance. Control transitions reserve 1024 encoded bytes
and one terminal revision for each created hold. Qualification must exercise
combined maximum authority and checkpoint publication/read-back within the
unchanged #504 worker RSS ceiling. No H1–H5 or P1–P8 target is revised here.

This is a breaking storage SDK capability change. Custom complete adapters must
implement `HistoryLifecycleStorage`, atomic canonical audit, bounded source
inspection, compare-and-swap fencing, restore isolation and quarantine-only
recovery. Upgrade all exact SDK dependencies together to the coordinated
incompatible 0.20 release. Deploy migration
`2026-11-19-000002_history_lifecycle` after the idle scheduler migration and
refresh runtime grants. The combined archive adapter accepts both registered
protocols through original publication/producer ownership; retained collection
expiry makes archive capture unavailable. Production still registers Legacy and
does not enable physical expiry.

The policy-control service tests and initial PostgreSQL migration/transaction
probes are development evidence. HTTP authorization, native split-role,
restore/concurrency, resource limits, destructive execution and relevant
frontend/Rust/Python/CLI workflow acceptance still require final qualification.
This foundation does not close #513, #545 or #550 or enable scheduled expiry.

Same-UUID restore correspondence for retained object-population gaps is validated
from the original object, collection and class identities and their relationships
in one productive image. Numeric coordinates may change after supported logical
restore; original names, intervals, class selection and read epoch remain bound.
Configured current-object and historical authorization both run before projecting
an expired population response, followed by correspondence revalidation in the
original execution scope. Missing, ambiguous or conflicting identities remain
unavailable. This read gate does not establish physical expiry, restored live-copy
reconciliation, executor recovery or transport retirement acceptance.
