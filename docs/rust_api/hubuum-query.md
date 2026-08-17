# `hubuum-query` Rust API Policy

Status: experimental public API.

## Purpose and Callers

`hubuum-query` owns database-neutral parsing and bounded query option types.
Applications and backend crates may consume its public parsers and value types.
It does not construct SQL or authorize a query.

## Compatibility

The crate follows SemVer from its first crates.io release. During the `0.x`
experimental period, incompatible public API changes require a minor-version
bump and migration guidance. The MSRV is Rust 1.88. There are no feature flags.

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

Hubuum maintainers own releases. CI builds rustdoc with warnings denied, tests a
clean package, and checks SemVer compatibility. `hubuum-storage-core` and the
root application are pinned downstream consumers; parser benchmarks guard
important construction paths.
