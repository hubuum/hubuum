# `hubuum-events-core` Rust API Policy

Status: experimental public API.

## Purpose and Callers

`hubuum-events-core` defines Hubuum's backend-neutral event catalog, envelopes,
and mutation provenance. Applications, event integrations, and storage adapters
may use these types without depending on the server crate.

## Compatibility

The crate follows SemVer from its first crates.io release. During the `0.x`
experimental period, incompatible public API changes require a minor-version
bump and changelog migration guidance. The MSRV is Rust 1.88.

The default feature set is empty. The `schema` feature adds Utoipa schema
implementations and is supported. Serialized event values are compatible only
where their type documentation or Hubuum's persisted event format says so;
ordinary Rust helper representations are not independent wire protocols.

## Errors, Runtime, and Security

The crate performs no I/O and requires no asynchronous runtime. Constructors
return explicit errors for invalid caller input and do not intentionally panic.
Debug implementations and event APIs must not expose credential material.
Cancellation is not applicable.

## Ownership and Verification

Hubuum maintainers own releases. CI builds all-feature rustdoc with warnings
denied, tests a clean package, and checks SemVer compatibility. The storage and
event-sink crates are pinned downstream consumers.
