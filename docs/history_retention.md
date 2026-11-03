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
| `POST /policy-previews` | Issued review of one exact proposed change. |
| `PUT /policies` | Revision-bound application with independent checkpoint protection. |
| `POST /holds` | Create an identity-bound hold with an optional fixed expiry. |
| `POST /holds/{hold_id}/release` | Release a hold without rewriting its creation or expiry clock. |
| `GET /operations/{operation_id}` | Bounded policy, hold or review evidence referenced by an audit receipt. |

Write/inspection JSON bodies are limited to 16 KiB.

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

Age, size, age-or-size and age-and-size are separate rule variants. Size pressure
uses measured logical bytes and persists hysteresis until its lower target is
reached. The policy model includes these semantics; native complete accounting
and destructive size-rule execution still require qualification. Inspection
reports size accounting as unavailable when that measurement is needed, rather
than guessing from a bounded sample or PostgreSQL physical table size. A proven
age condition suffices for age-or-size; a false age condition makes age-and-size
false without consulting size.

## Review and protection

An issued preview binds the principal UUID, exact proposal, policy/protection
revision, application incarnation, coverage, original issue time and fixed
expiry. Its maximum lifetime is 300 seconds. Reading or retrying cannot renew
that lifetime. A matching digest alone is not issuance proof. Retrospective
application requires the stored review; any supplied prospective review is also
validated.

Policy, hold or deletion authority changes and application recovery invalidate
old reviews. A hold expiring between issuance and application also makes the
review stale. Application returns HTTP 409 with `stale_preview`,
`preview_expired` or `preview_mismatch`; unknown/inaccessible references use 404.
A conflict after a lost response does not prove the first application failed:
inspect the immutable applied record and its retained review before reviewing
another change.

Inspection scans at most 1000 source rows and 1 MiB of identity metadata, and
returns at most 16 payload-free samples. Coverage distinguishes complete from
bounded scans. Zero observed eligible records with bounded coverage is not a
complete empty result. A review covers its declared scope and cutoff, including
additional retained or late-committing records, rather than only sampled IDs.
Issuance reserves no later execution capacity and does not pin candidates.

Configure and initialize the existing
[independent control repository](deletion_recovery.md) before applying policies
or creating/releasing holds. The service commits authority, publishes a signed
checkpoint, reads it back, and only then acknowledges protected application.
A publication failure leaves an inspectable pending outcome. Policy listing
reports `protected`, `pending`, `recovery_required`, `unconfigured` or
`unavailable`; it does not equate a committed database row with independent
protection.

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
revision-bound review/application contract. Complete size accounting, persistent
hysteresis, run admission/progress/cancellation and fair repeated claims remain
executor work; inspection results are not executable membership manifests.

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

A typed guard can cross response rebuilding through response-head extensions,
then transfer to the outer body wrapper. The final implementation must prove the
handoff through inner buffering, outer EOF/drop, cancellation and streams, and
prove lock-order behavior for each entry mode. Neither a timer nor an API-owner
row disappearing is a publication-quiescence proof. Export/backup final
publication and restored descendants need the same independently demonstrated
boundary. These lifetime and native race cases remain acceptance gates.

The online model bounds selected membership to 1000 rows
and 16 MiB measured full-row input, with a 1 MiB encoded membership envelope.
It preserves anchors and exact UUIDs, retains compact governing-policy references
into the immutable catalog, re-evaluates eligibility against captured authority, and rejects duplicate/open
membership. Retained intent modeling preserves exclusions on cancellation and
completion, retains first removal clocks/checkpoint identities, and merges
compatible recovery descendants without losing local outcomes. The journal permits
at most 256 retained intents within 4 MiB and reserves 512 encoded bytes per pending removal plus 256 bytes per unrecorded
cancellation. These are bounded source contracts, not measured resource
acceptance. These types are now included in typed protected history authority and the memory adapter's
control state. Focused memory tests cover exact object list/as-of/backup exclusion
and continued newer-version reads with a runtime registered. PostgreSQL projection,
complete native admission/dependencies, publication fencing, physical cleanup and
measured qualification remain unfinished.

### Executor implementation and acceptance map

The dedicated `history_expiry` task kind and payload are planned closed-vocabulary
SDK changes. The action remains `expire_configured_hot_history`. Admission and
periodic scheduling remain disabled until the online gates pass; configuring an
expiry policy alone does not enable either path.

| Area | Implementation target and required evidence |
| --- | --- |
| Admission and authorization | Manual runs require the configured unscoped administrator authorization; periodic work uses its retained policy/root initiation. Preserve UUID authority after principal/task deletion, and reject stale application incarnations. |
| Scheduling class | Both triggers use the existing internal-work lane and shared persisted scheduling policy. Keep ordinary slots and the existing internal reservation unchanged; one-slot deployments retain shared mode. Exercise mixed ordinary/internal backlogs and fairness under replenished work. |
| Initial per-kind policy | Target one execution, 64 outstanding runs, 64 MiB resident reservation and one outstanding run per principal; weight one. Existing deployment/principal/collection/internal limits remain additional constraints. Apply the coordinated policy revision atomically, preserving all configured limits for existing kinds. These defaults are not measured H1/H4 acceptance. |
| Coalescing | One active task per exact family/stable scope key, shared by manual and periodic triggers. Concurrent/retried submissions return the authorized existing reference without replacing its original trigger/principal/clock. No allocated-ID or timestamp cursor proves completion. |
| Ownership and restart | Durable task lease/checkpoint owns a fixed admitted batch. Restart resumes that membership before selecting new work. Lost producers/publications require positive fencing; deadlines alone cannot release obligations or reservations. |
| Cancellation | Stop new selection and settle execution ownership while retaining every committed exclusion, pending copy obligation and original receipt. Terminal task cancellation does not prove physical cleanup complete. |
| Workers and downgrade | Advertise support only with the implemented payload/checkpoint reader. Older workers must leave unsupported work unclaimed. Upgrade SDK adapters and all producers/workers before enabling the kind; do not silently reinterpret or discard it on downgrade. |
| Metadata and discovery | Bounded payload-free progress identifies scope, run/batch, actual governing revisions, blockers and remaining obligations. Advertise enabled/configured state and effective limits only when supported; regenerate OpenAPI/classifier coverage with the kind. |
| Qualification | Require all nine native families, current/deleted/moved owners, held and changed-policy races, interval gaps, cancellation/restart/restore descendants, publication body lifetimes, split-role authority and measured H1/H4 plus affected scheduler/performance scopes. Preserve the #561 P6 deferral. |

The confirmed #521 helper split is an integration boundary, not an unmerged
compile dependency. No broader CI labels or deployment activation follow from
this plan. The final combined tree must revalidate entry modes, lock cycles and
publication settlement after API occupancy has already settled.
Native socket probes cover plain HTTP/1, Rustls and OpenSSL body completion and
backpressure. They retain a connection-data witness beyond body EOF and require
a same-worker deferred dispatcher-release signal after requesting socket abort.
These probes are development evidence for a transport mechanism, not production
publication retirement: upgrade rejection, producer ownership, pipelining,
cancellation/crash behavior and measured resource/liveness gates remain open.
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
incompatible release. The final release number and migration evidence are
recorded when the interface and project integration base are finalized.

The policy-control service tests and initial PostgreSQL migration/transaction
probes are development evidence. HTTP authorization, native split-role,
restore/concurrency, resource limits, destructive execution and relevant
frontend/Rust/Python/CLI workflow acceptance still require final qualification.
This foundation does not close #513, #545 or #550 or enable scheduled expiry.
