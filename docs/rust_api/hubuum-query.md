# `hubuum-query` Future Rust API Design

Status: workspace-internal; candidate for a later publication review.

## Purpose and Callers

`hubuum-query` owns database-neutral parsing and bounded query option types.
Applications and backend crates may consume its public parsers and value types.
It does not construct SQL or authorize a query.

## Compatibility

There is no current third-party SemVer promise or crates.io release. A separate
promotion change must define the initial supported surface and compatibility
policy. The workspace MSRV is Rust 1.88. There are no feature flags.

Parser behavior, accepted operators, and public validation errors are part of
the supported API. Rust values are not a separate serialized wire contract.

## Errors, Runtime, and Security

Untrusted query text returns crate-owned parse errors and must not cause a
panic. Parsing is synchronous, performs no I/O, uses no global state, and has no
runtime or cancellation requirement. Query debug output must remain bounded and
must not reveal filter values where a type promises redaction.

`QueryOptions` has a private representation. Its `QueryFilters`, `QuerySort`,
and `QueryCursor` components validate their bounds and expose only
invariant-preserving mutation. `SortParam` and `ParsedQueryParam` remain simple
leaf DTOs with public fields as an intentional construction surface; placing
them into the bounded collections still performs collection-level validation.
Scalar inference uses application-neutral value categories. SQL expressions,
column names, and database type identifiers belong to adapters and are not part
of this crate's API.

## Ownership and Verification

Hubuum maintainers own the workspace crate. `hubuum-storage-core`, the root
application, and parser benchmarks verify it today. A later promotion must
enable rustdoc, package, and crates.io compatibility gates.
