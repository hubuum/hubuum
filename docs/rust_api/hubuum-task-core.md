# `hubuum-task-core` Rust API Policy

Status: experimental public API in the storage SDK `0.1` release train.

## Purpose and Callers

`hubuum-task-core` provides validated task identifiers and idempotency values
used across application and storage boundaries. External backend crates may use
them without depending on Hubuum's server implementation.

## Compatibility

The crate follows the lockstep versioning, exact dependency, MSRV, deprecation,
and release rules in the
[Storage Adapter SDK Compatibility policy](../storage_adapter_sdk.md). The
release-train MSRV is Rust 1.88. There are no feature flags or independent
serialization guarantees.

## Errors, Runtime, and Security

Invalid input returns crate-owned errors and does not intentionally panic. The
crate performs no I/O, has no asynchronous runtime or cancellation behavior,
and stores no secrets. Debug output must not disclose idempotency values where
their type promises redaction.

## Ownership and Verification

Hubuum maintainers own the crate. `hubuum-storage-core` and the root task
services verify its behavior. CI packages it, builds rustdoc with warnings
denied, and compares it with the latest crates.io release when a baseline
exists.
