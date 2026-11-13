# `hubuum-domain` Rust API Policy

Status: experimental public API in the storage SDK `0.10` release train.

## Purpose and Callers

`hubuum-domain` provides validated, persistence-neutral policy values shared by
Hubuum applications and storage adapters. External backend crates may depend on
these types without linking the server. It is not an HTTP client or a server
embedding API.

## Compatibility

The crate follows the lockstep versioning, exact dependency, MSRV, feature,
deprecation, and release rules in the
[Storage Adapter SDK Compatibility policy](../storage_adapter_sdk.md). The
release-train MSRV is Rust 1.88.

Its values are in-memory contracts; it makes no wire-format guarantee unless a
type's documentation explicitly says otherwise. The optional `openapi` feature
implements Utoipa 6 schema traits for values also used by Hubuum's HTTP API.

## Errors, Runtime, and Security

Validation returns crate-owned errors and does not panic for caller input.
There is no asynchronous runtime requirement, I/O, or cancellation behavior.
`PlaintextPassword` validates nonempty passwords up to 4096 Unicode characters,
preserves their exact contents, and redacts debug output. Hashing and verification
belong to consumers; plaintext is accessible only through its explicit accessor.
JSON Schema validation rejects external references and uses a bounded process-local compilation cache; cache contents do not change
validation results. Private fields and validating builders protect invariants.
Positive resource identifiers and `ResourceRevision` are opaque newtypes with
validated constructors and explicit primitive accessors. Generic storage
metadata uses `ResourceId`; operation-specific contracts use the corresponding
resource ID type so an adapter cannot silently mix identity domains.

`IdentityScopeName` preserves nonempty names without surrounding whitespace.
Deployment authentication and runtime settings use its shared constructor;
reserved provider names remain a consumer-specific policy.

Authentication interaction values preserve the provider UUID, client, settings
revision and purpose together. Privileged purposes retain the initiating actor
and credential plus the exact operation or candidate digest. `hubuum-auth-core`
re-exports these values for provider adapters. Subject proofs redact their subject
in debug output and require the same complete binding when consumed; adapters
remain responsible for verifying the upstream protocol before constructing them.

`InteractionLifecycle` applies bounded lifetimes, polling backoff, leased
authentication attempts, browser binding and explicit approval, purpose-specific
consumption and terminal replay rules using a caller-supplied clock. Its consumed
state retains only issuance metadata for orphan-session cleanup. Consumers must
authenticate redemption/browser capabilities, fence configuration and initiating
credentials, persist each transition under a lock, and commit consumption with
issuance in the same transaction. A polling slowdown is a committed state change.
PR #591 implements durable memory/PostgreSQL storage, versioned HTTP operations
and hosted local/LDAP browser authentication around these primitives. Real
OIDC/OAuth protocol adapters remain subsequent #577/#578 work.

`BoundedJsonPatch` validates JSON Patch size, operation count, pointer depth,
and cumulative application work before an adapter sees the document. Patch
application and the portable storage-JSON envelope use crate-owned error
classifications. This keeps patch behavior identical across storage backends
without exposing PostgreSQL JSONB rules or an adapter error to callers.

## Ownership and Verification

Hubuum maintainers own the crate. Ordinary workspace tests and storage
consumers verify its behavior. CI packages it, builds rustdoc with warnings
denied, and compares it with the latest crates.io release when a baseline
exists.

## External authentication proofs

SDK 0.10 adds private-fielded external identity, membership, admission and fresh
approval proofs, also re-exported by the internal `hubuum-auth-core` crate.
Constructors validate facts from trusted protocol adapters; they do not verify
signatures or authenticate client-supplied profiles. These proof types cannot be
deserialized. Fresh evidence retains the interaction start for the storage fence.
`ExternalFreshnessPolicy` measures upstream eligibility independently of session
rotation, with fallible conversion from a membership policy.
