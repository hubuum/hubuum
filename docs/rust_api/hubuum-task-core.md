# `hubuum-task-core` Future Rust API Design

Status: workspace-internal; candidate for a later publication review.

## Purpose and Callers

`hubuum-task-core` provides validated task identifiers and idempotency values
used across application and storage boundaries. A future external backend crate
could use them without depending on Hubuum's server implementation.

## Compatibility

There is no current third-party SemVer promise or crates.io release. A separate
promotion change must define the initial supported surface and compatibility
policy. The workspace MSRV is Rust 1.88. There are no feature flags or
independent serialization guarantees.

## Errors, Runtime, and Security

Invalid input returns crate-owned errors and does not intentionally panic. The
crate performs no I/O, has no asynchronous runtime or cancellation behavior,
and stores no secrets. Debug output must not disclose idempotency values where
their type promises redaction.

## Ownership and Verification

Hubuum maintainers own the workspace crate. `hubuum-storage-core` and the root
task services verify it today. A later promotion must enable rustdoc, package,
and crates.io compatibility gates.
