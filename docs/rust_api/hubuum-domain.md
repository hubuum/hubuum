# `hubuum-domain` Future Rust API Design

Status: workspace-internal; candidate for a later publication review.

## Purpose and Callers

`hubuum-domain` provides validated, persistence-neutral policy values shared by
Hubuum applications and storage adapters. A future external backend crate could
depend on these types after promotion. It is not an HTTP client or a server
embedding API.

## Compatibility

There is no current third-party SemVer promise or crates.io release. A separate
promotion change must define the initial supported surface and compatibility
policy. The workspace MSRV is Rust 1.88.

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

Hubuum maintainers own the workspace crate. Ordinary workspace tests and the
storage consumers verify it today. A later promotion must enable rustdoc,
package, and crates.io compatibility gates before the first public release.
