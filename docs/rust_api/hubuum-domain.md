# `hubuum-domain` Rust API Policy

Status: experimental public API.

## Purpose and Callers

`hubuum-domain` provides validated, persistence-neutral policy values shared by
Hubuum applications and storage adapters. External backend crates may depend on
its public types. It is not an HTTP client or a server embedding API.

## Compatibility

The crate follows SemVer from its first crates.io release. During the `0.x`
experimental period, incompatible public API changes require a minor-version
bump, changelog migration guidance, and a deprecation period when practical.
The MSRV is Rust 1.88.

Its values are in-memory contracts; it makes no wire-format guarantee unless a
type's documentation explicitly says otherwise. The optional `openapi` feature
implements Utoipa schema traits for values also used by Hubuum's HTTP API.

## Errors, Runtime, and Security

Validation returns crate-owned errors and does not panic for caller input.
There is no asynchronous runtime requirement, I/O, credential handling, or
cancellation behavior. JSON Schema validation rejects external references and
uses a bounded process-local compilation cache; cache contents do not change
validation results. Private fields and validating builders protect invariants.
Positive resource identifiers and `ResourceRevision` are opaque newtypes with
validated constructors and explicit primitive accessors. Generic storage
metadata uses `ResourceId`; operation-specific contracts use the corresponding
resource ID type so an adapter cannot silently mix identity domains.

`BoundedJsonPatch` validates JSON Patch size, operation count, pointer depth,
and cumulative application work before an adapter sees the document. Patch
application and the portable storage-JSON envelope use crate-owned error
classifications. This keeps patch behavior identical across storage backends
without exposing PostgreSQL JSONB rules or an adapter error to callers.

## Ownership and Verification

Hubuum maintainers own releases. CI builds rustdoc with warnings denied, tests a
clean `cargo package`, and compares public API compatibility with the latest
crates.io release. Hubuum's storage crates are the pinned downstream
compatibility consumers.
