# `hubuum-task-core` Rust API Policy

Status: experimental public API in the storage SDK `0.3` release train.

## Purpose and Callers

`hubuum-task-core` provides validated task identifiers, idempotency values,
cancellation reasons, execution limits, and cooperative stop contexts used across
application and storage boundaries. External backend crates may use them without
depending on Hubuum's server implementation.

## Compatibility

The crate follows the lockstep versioning, exact dependency, MSRV, deprecation,
and release rules in the
[Storage Adapter SDK Compatibility policy](../storage_adapter_sdk.md). The
release-train MSRV is Rust 1.88. There are no feature flags or independent
serialization guarantees.

## Errors, Runtime, and Security

Invalid input returns crate-owned errors and does not intentionally panic. The
crate performs no I/O and requires no asynchronous runtime. Execution contexts
share an atomic stop signal and a monotonic deadline; callers must check them at
work boundaries and complete cleanup before acknowledging a stop. Stop signals
do not replace durable coordination or lease fencing. Debug output redacts
operator cancellation explanations and idempotency values where their types
promise redaction.

## Ownership and Verification

Hubuum maintainers own the crate. `hubuum-storage-core` and the root task
services verify its behavior. CI packages it, builds rustdoc with warnings
denied, and compares it with the latest crates.io release when a baseline
exists.
