# Durable authentication interactions

Status: #575 implementation in progress, draft PR #591 on `project/auth-support`.
The domain model, complete storage adapters and session issuance service are
implemented. Versioned endpoints, the hosted browser page, transport protections
and scheduling of cleanup remain to be connected. Existing password login is
unchanged. Real OIDC/OAuth execution remains in #577/#578; this storage foundation
does not qualify a live provider.

## Authority and completion

An interaction binds its identifier, selected provider UUID (or local password
provider), authentication configuration revision, client and purpose at creation.
The redemption digest is separate from the visible user-code digest. A user code
can attach one browser, but cannot poll, cancel or redeem a session. Browser
continuation requires its cookie and CSRF challenge; successful authentication
rotates the challenge and requires an explicit approval decision.

Only a leased authentication attempt can submit proof. Failed credentials permit
at most three attempts; an expired or ambiguous claim is terminal. Rejected early
polls commit increasing backoff. Every adapter rechecks current authentication
revision and, for privileged purposes, the initiating unscoped human credential.
Provider verification also rechecks local recovery-administrator membership.

Completion locks the interaction and commits consumption with exactly one result:

| Purpose | Result | Additional binding |
| --- | --- | --- |
| Login | Existing Hubuum session family and audit receipts | Current eligible identity and unchanged credential fingerprint |
| Credential approval | Existing approval record and audit receipt | Original actor/token and canonical operation-request digest |
| Provider verification | Verified candidate, without session or identity changes | Candidate ID/content, selected provider namespace, configuration revision, initiating local administrator and passed identity check |

The session service generates access/refresh credentials, invokes atomic storage
completion and returns the existing session response only after commit. A failed
transaction rolls back both issuance and consumption. The caller supplies a
canonical operation digest that includes operation, target and all relevant
request fields; it must not accept an arbitrary client-provided digest.

Persisted documents are private trusted-storage data, never transport DTOs.
Decoding revalidates phase, mode, claim, proof and purpose correlations. Human
proofs retain a credential fingerprint and event-time subject, with current
identity eligibility checked again at completion. Consumed rows retain only
non-secret issuance metadata and correlation hashes, not reusable proof or raw
access/refresh credentials.

## Response loss and bounded retention

Repeating completion cannot issue another result or recover plaintext credentials.
A protected status poll returns consumed metadata. The original client can use
its redemption capability to revoke exactly the issued session family, including
rotated descendants. Cleanup is repeatable and leaves unrelated families intact.
Configuration changes do not remove this cleanup capability.

Interactions live at most 600 seconds (default 300). Retention is limited to
65,536 rows, with at most 4,096 active globally, 256 per client and 32 per
privileged actor. Creation opportunistically removes up to 128 expired rows;
explicit cleanup accepts batches of at most 4,096. Consumed metadata survives
until one hour after the original expiry for response-loss cleanup. Other rows
can be removed at expiry. Browser attachment and claim state are bounded within
each record; persistence documents are limited to 256 KiB.

PostgreSQL serializes creation capacity checks and locks settings before
interaction, principal and credential rows. Terminal rows cannot be reopened.
The memory adapter stages the whole mutation and commits only after success.
Both adapters retain transition rejections that intentionally mutate state.

## Upgrade and recovery

This is an offline migration. Stop all API and worker replicas and take a
verified backup. Apply migrations and role grants, then start matching binaries
everywhere. Do not run older recovery or restore tools: they do not know to clear
interaction authority. Restore the pre-upgrade snapshot before downgrading.

Logical backups exclude interactions. Logical restore and offline security
recovery discard pending, ready and consumed interaction records. External Rust
adapters must upgrade the complete SDK graph to `0.9.0`; see the
[SDK migration guide](storage_adapter_sdk.md#upgrading-from-08-to-09).
