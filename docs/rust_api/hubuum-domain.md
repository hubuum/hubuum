# `hubuum-domain` Rust API Policy

Status: experimental public API in the storage SDK `0.1` release train.

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

Hubuum maintainers own the crate. Ordinary workspace tests and storage
consumers verify its behavior. CI packages it, builds rustdoc with warnings
denied, and compares it with the latest crates.io release when a baseline
exists.
