# Task receipts and history policy review

Feature-specific review for [#545](https://github.com/hubuum/hubuum/issues/545),
assigned to the server implementation session on 2026-10-30. This document covers
[#512](https://github.com/hubuum/hubuum/issues/512) submission outcomes and the
preview/apply boundary for [#513](https://github.com/hubuum/hubuum/issues/513).
The audit and sink-event review remains owned by
[#550](https://github.com/hubuum/hubuum/issues/550).

The receipt section describes the implementation on server `e6c4ffb6`; it is
not a released-server compatibility claim. The history policy section is a
proposed implementation contract. Its routes, schemas and examples do not
advertise an implemented feature or certify future deletion behavior.

## Receipt outcomes reviewed for #512

The canonical producer fixture is
[task_submission_outcomes.json at e6c4ffb6](https://github.com/hubuum/hubuum/blob/e6c4ffb6349e03b63a29f9a96ed6b0c3f6d994fe/tests/fixtures/lifecycle/task_submission_outcomes.json).
Its SHA-256 is
`28637f643cc800fe24d4886433da21b29f4d1b9328c7e533db05677cdb035e20`.
The same contract applies to keyed import, backup, export, template export,
remote invocation and schema-profile submissions.

| Outcome | Reader behavior | New work |
| --- | --- | --- |
| HTTP 202, existing task reference | Inspect the returned task; this is not proof of another execution. | None for the recognized generation. |
| HTTP 409, `idempotency_key_conflict` | Preserve the failed outcome. Do not change the key automatically. | None. |
| HTTP 410, `task_details_expired` | Preserve recognition of the original submission. Explain that details are unavailable. | None. |
| HTTP 202 after protected receipt retirement | Treat the returned reference as a new generation with new effects. | Subject to normal authorization and admission. |

Both error responses retain the standard string `error` and `message` fields,
use `Cache-Control: no-store`, and omit `Retry-After`. Machine decisions use the
HTTP status and open-ended `reason` string, never message text. A reader must
retain unknown optional fields/reasons or expose the original safe error body.
Unknown reasons fall back to status handling; they cannot imply successful work.

The 410 body contains only the safe envelope, stable task UUID, kind and nullable
receipt expiry. That expiry is a deduplication deadline, not result availability
or permission to submit again. It can remain unknown until terminal settlement.
The server still checks the current principal, permission and protected request;
an unauthorized caller cannot use this response as an existence oracle.

The terminal clock starts once at completion. Reads, retries and supported
restore preserve it. Client cache eviction, elapsed client time, HTTP 404 and
HTTP 410 do not establish protected retirement. Creating a new key must express
a new user request; it must not be an automatic way around conflict, expiry or
an ambiguous transport failure.

### Reader evidence and limitations

| Consumer and source | Reviewed evidence | Remaining implementation |
| --- | --- | --- |
| Rust async/blocking, `b91628d6`, client 0.20.0 | Twelve executed public-client mock-transport checks cover both errors, envelope decoding, raw body preservation, optional fields/reasons and one request despite retry being configured. | Dedicated receipt helpers and complete workflows remain client work. |
| Frontend/BFF, `d2ce3864` | Six executed error-helper checks on pinned Node 24.22.1 preserve the safe messages and tolerate added fields/reasons. Failed submissions retain their key; only 202 completes it. | Recognized-expired presentation and durable key ownership still need implementation. The existing 30-minute/64-entry cache is not server retirement authority. |
| Python sync/async, `f65ed9db`, client 0.0.14 | Both transports call the same error mapper: 409 becomes `ConflictError`, 410 becomes `APIError`; reason and redacted body survive. Neither path automatically retries these HTTP statuses. | This is source review, not executed Python transport or workflow evidence. Typed helpers remain client work. |
| CLI, `8ae0368d` | Committed source uses Rust client 0.20.0 and propagates these errors. Its password-approval retry is restricted to the applicable 403 reason. Status/message rendering uses the standard envelope. | This is source review, not executed CLI workflow evidence. Dedicated receipt display remains CLI work. |

The original four-field 410 response failed the actual Rust reader and produced
a generic frontend message. The standard envelope corrected those demonstrated
compatibility gaps. Server HTTP fixture tests passed in the full `e6c4ffb6`
runner: 6,839 passing tests, zero failures. These facts establish the producer
contract and the listed reader behavior; they do not certify unimplemented UI,
typed helpers, all consumer workflows or consumer-maintainer approval.

Consumer tracking remains [frontend #183](https://github.com/hubuum/hubuum-frontend/issues/183),
[Rust #135](https://github.com/hubuum/hubuum-client-rust/issues/135),
[Python #82](https://github.com/hubuum/hubuum-client-python/issues/82), and
[CLI #146](https://github.com/hubuum/hubuum-cli/issues/146).

## Proposed history policy boundary for #513

Management is restricted to an unscoped administrator, including the configured
authorization backend. Authorization precedes target/preview lookup. A preview
reference is not an authorization grant, and a local wait timeout never cancels
accepted server work.

| Route | Meaning |
| --- | --- |
| `GET /api/v1/admin/history-lifecycle/policies` | Page configured bindings and immutable revisions. |
| `POST /api/v1/admin/history-lifecycle/inspect` | Bounded inspection of effective sources, original anchors, deadlines and blockers. |
| `POST /api/v1/admin/history-lifecycle/policy-previews` | Issue a bounded, revision-bound review of one exact policy change. |
| `PUT /api/v1/admin/history-lifecycle/policies` | Apply an explicit revision-bound change; retrospective changes require the issued preview. |
| `POST /api/v1/admin/history-lifecycle/runs` | Admit an explicit scoped `expire_configured_hot_history` run under configured policy. |

The administrator configuration advertises support, enabled/configured state,
allowed actions and effective preview limits only when implemented. An absent
capability is unknown/unsupported; consumers must not infer support from a server
version or probe a mutating route. This review does not settle #545's wider
permission-filtered capability discovery contract.

### Scope, inheritance and clocks

One policy key selects a history family and deployment, collection or class
scope. The nine families are collection, class, object, class relation, object
relation, export template, remote target, computed definitions and class schema.
They reuse the storage identity history family rather than defining another
independently maintained backend family set.

Durable bindings use stable UUIDs. Existing resource integer IDs remain unchanged.
An optional current integer ID is presentation/lookup information; if supplied
alongside a UUID, both must resolve to the same current resource before review.
A deleted/reused integer cannot retarget a retained policy, preview or hold.

A complete rule replaces an override; inheritance is an operation on that
binding, not a merge of individually inherited fields. Effective resolution is
class, captured historical collection, then deployment/family. Responses identify
the effective source and immutable revision for each explained record/endpoint.
If a historical scope spans several captured collections, return a bounded set
of sources and label its coverage; do not pretend there is one current parent.
Default, inherit, disabled expiry
and indefinite retention remain distinguishable. Removing an override can expose
a more destructive ancestor rule. Its prospective/retrospective mode remains
explicit; retrospective removal requires the same preview as replacement.

Prospective changes govern future closures/events. Retrospective changes bind a
stable historical scope and explicit closure cutoff. Late commits with older
anchors remain covered; a timestamp or allocated ID is not a commit watermark.
Class moves/deletion do not rewrite captured attribution or original clocks.
Open/live versions are never ordinary expiry targets. Schema event occurrence
and deletion tombstone clocks are not invented closed intervals.

Age, size, OR/AND, lower-target hysteresis and minimum guarantees remain explicit
rule components. Logical policy bytes are not physical filesystem quota. Both
relation endpoints must permit expiry; holds, dependencies, minimum guarantees,
unknown attribution and unavailable accounting remain separately visible
blockers. No rule silently converts an unresolved conflict into permission to
delete.

New timestamps use RFC 3339 UTC. New revisions and potentially large counts/byte
quantities use decimal strings, preserving integer precision in JavaScript.
Existing endpoint timestamp/number representations are not changed by this
proposal. A missing deadline is explicit, never silently replaced with now.

### Server-issued review and application

Every write binds its exact action/scope, complete rule, application mode and
expected policy revision. A retrospective write also uses a server-issued
preview. Its immutable binding contains
the principal UUID, action, exact family/scope, complete proposed binding,
prospective/retrospective mode and cutoff, effective source/revision, relevant
policy/hold/deletion authority, coverage and original issue/expiry clocks.
The default maximum lifetime is 300 seconds; the response states its actual
fixed expiry. Retrying or reading never renews that expiry.

The response carries an unpredictable `preview_id` and opaque proposal
fingerprint. A retrospective apply echoes both, the exact proposed change and
the expected policy revision.
The server validates its stored or authenticated issuance proof; recomputing a
proposal digest is not proof that a preview was issued. Caller booleans, caller
timestamps and caller assertions of eligibility are not accepted as proof.
Unknown write actions/fields are rejected.

Prospective requests can omit a preview because their complete explicit proposal
and expected revision govern future closures only. Any supplied preview is still
validated. A retrospective cutoff cannot be later than the authoritative preview
time. Its fixed scope/cutoff also covers a later commit with an older anchor.

The apply transaction rechecks the current principal and relevant authority.
Ordinary receipt settlement, polling and unrelated task activity are not policy
or hold decision revisions. A recovered server invalidates unapplied previews;
reuse across a supported restore cannot recreate old deletion authority.

| Apply outcome | HTTP and reason | Consumer action |
| --- | --- | --- |
| Applied and independently protected | 200 with the immutable applied policy reference and effective change semantics. | Display the exact applied scope/revision; do not claim all physical work is complete. |
| Relevant policy/protection changed | 409, `stale_preview`. | Inspect and review a new preview; never silently refresh and reapply old approval. |
| Review lifetime ended | 409, `preview_expired`. | Obtain a new review; an old fingerprint cannot renew authority. |
| Scope/action/proposal differs | 409, `preview_mismatch`. | Stop; reconcile the intended change. |
| Missing preview or inaccessible target | Normal authorized 404/403 behavior. | Do not infer hidden existence or permission. |
| Required protection unavailable | 503 with a bounded reason and meaningful retry guidance only where applicable. | Preserve the uncertain outcome and inspect authority; do not claim application succeeded. |

Errors use the existing safe `error`/`message` envelope, `no-store`, and additive
reason strings. After a lost response, retrying the identical apply cannot create
a second revision under the old precondition. A conflict is not proof the first
attempt failed: inspect the immutable policy reference/fingerprint before making
a new change. The UI/CLI must not manufacture a new preview or new approval as an
automatic transport retry.

Previewing changes neither retained data nor deletion authority, reserves no
execution capacity, and does not pin candidates. Bounded proof/audit metadata
may be persisted. It has a separate bounded lifetime/capacity; it cannot become
an unbounded retained request ledger.

### Coverage and execution

A preview declares scan/input/sample/output limits, complete versus bounded
coverage, exact versus lower-bound/unknown counts, and safe samples with stable
resource/version identities and original/derived deadlines. It contains no
retained payloads. Zero observed candidates with incomplete coverage is not a
complete empty result. Future-closure effects of a prospective rule are not an
exact prediction of future membership.

The reviewed action covers its declared scope, not just sampled IDs. Applying a
bounded retrospective preview must explicitly present that additional retained
or late-committing versions can be affected. The server binds that coverage to
the proof and rechecks protections at execution. Preview output never proves
future eligibility or physical deletion.

Periodic and manual runs use one executor with explicit scope/actions, durable
task references, progress, cancellation and restart. Approved scheduled expiry
needs no new human approval per batch. Each batch records its actual governing
authority and exact membership; policy edits and holds are checked again before
removal. Cancellation preserves already committed cleanup/recovery obligations.
No artifact provider is required for hot-only expiry.

## Review disposition and implementation gates

The #512 envelope and retry/key semantics above are the reviewed producer
contract. Dedicated consumer behavior remains explicitly tracked; source review
and focused reader tests must not be presented as full workflow qualification.

The #513 section fixes the proposed interface for implementation review. The
shared [examples](contracts/lifecycle-policy/examples.json) are planning fixtures,
with strict write shapes in the
[change schema](contracts/lifecycle-policy/policy-change.schema.json) and
[apply schema](contracts/lifecycle-policy/apply-request.schema.json). A preview
request uses `change`; the PUT uses `apply_request`, containing the exact
`proposal` and its `review` reference. Fingerprint values are illustrative; a
client cannot mint issuance proof from these examples. The 13 executable schema checks cover the prospective and retrospective write
examples, required review references, unknown write fields/actions/families,
invalid ages/IDs and decimal-string revision precision. Negative cases require
a value-validation failure rather than accepting an invalid schema as evidence.
These are not claims that the routes are implemented. Before feature acceptance, the
server and consumer tests must cover stale/expired/mismatched proofs, changed
holds/policies, lost responses, unknown optional fields/reasons, incomplete
coverage, class moves/deletion and supported restore. These tests must exercise
the actual authorization, persistence and execution boundaries.

Consumer tracking is [frontend #185](https://github.com/hubuum/hubuum-frontend/issues/185),
[Rust #137](https://github.com/hubuum/hubuum-client-rust/issues/137),
[Python #84](https://github.com/hubuum/hubuum-client-python/issues/84), and
[CLI #148](https://github.com/hubuum/hubuum-cli/issues/148). Typed clients and UI
must deliberately model these new requests/results before advertising support.
This document does not accept #550's event catalog, close #545, or waive the
owning features' runtime/performance requirements.
