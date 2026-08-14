# `hubuum-task-core` Rust API Policy

Status: experimental public API.

## Purpose and Callers

`hubuum-task-core` provides validated task identifiers and idempotency values
used across application and storage boundaries. External backend crates may use
these values without depending on Hubuum's server implementation.

## Compatibility

The crate follows SemVer from its first crates.io release. During the `0.x`
experimental period, incompatible public API changes require a minor-version
bump and migration guidance. The MSRV is Rust 1.88. There are no feature flags
or independent serialization guarantees.

## Errors, Runtime, and Security

Invalid input returns crate-owned errors and does not intentionally panic. The
crate performs no I/O, has no asynchronous runtime or cancellation behavior,
and stores no secrets. Debug output must not disclose idempotency values where
their type promises redaction.

## Ownership and Verification

Hubuum maintainers own releases. CI builds rustdoc with warnings denied, tests a
clean package, and checks SemVer compatibility. `hubuum-storage-core` and the
root task services are pinned downstream consumers.
