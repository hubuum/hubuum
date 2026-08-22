# `hubuum-events-core` Future Rust API Design

Status: workspace-internal; candidate for a later publication review.

## Purpose and Callers

`hubuum-events-core` defines Hubuum's backend-neutral event catalog, envelopes,
and mutation provenance. Applications, event integrations, and storage adapters
may use these types without depending on the server crate.

## Compatibility

There is no current third-party SemVer promise or crates.io release. A separate
promotion change must define the initial supported surface and compatibility
policy. The workspace MSRV is Rust 1.88.

The default feature set is empty. The `schema` feature adds Utoipa schema
implementations and is supported. Serialized event values are compatible only
where their type documentation or Hubuum's persisted event format says so;
ordinary Rust helper representations are not independent wire protocols.

Event envelopes and subscription filters are intentional serialized
integration DTOs. Their public fields use validated event sequence, entity,
collection, principal, and task identifier types. Mutation helpers keep their
representation private, and catalog parsing uses semantic `parse` methods;
database column terminology is not part of the public API.

## Errors, Runtime, and Security

The crate performs no I/O and requires no asynchronous runtime. Constructors
return explicit errors for invalid caller input and do not intentionally panic.
Debug implementations and event APIs must not expose credential material.
Cancellation is not applicable.

## Ownership and Verification

Hubuum maintainers own the workspace crate. Storage and event-sink consumers
verify it today. A later promotion must enable rustdoc, package, and crates.io
compatibility gates before the first public release.
