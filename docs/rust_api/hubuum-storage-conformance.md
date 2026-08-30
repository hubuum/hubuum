# `hubuum-storage-conformance` Rust API Policy

Status: experimental public API in the storage SDK `0.1` release train.

## Purpose and Callers

`hubuum-storage-conformance` is the reusable behavioral certification harness
for complete Hubuum storage adapters. Adapter authors implement its fixture
traits using public `hubuum-storage-core` values and run the exported verifiers
without depending on the root server crate or PostgreSQL implementation.

The supported entrypoints are the public fixture traits, probe types, recording
observer and sink helpers, and `verify_*` functions exported by the crate.
Application-owned HTTP fixture implementations remain outside this crate.

## Compatibility

The crate follows the lockstep versioning, exact dependency, MSRV, deprecation,
and release rules in the
[Storage Adapter SDK Compatibility policy](../storage_adapter_sdk.md).
There are no feature flags or independent serialization guarantees.

Adding a required fixture method or strengthening an existing expectation is a
behavioral compatibility change for adapter authors. It must accompany the
corresponding storage contract change and use the coordinated incompatible SDK
release when an already conforming adapter needs source or behavior changes.

## Errors, Runtime, and Cancellation

Fixture setup and verification return crate-owned `ConformanceError` values;
invalid adapter results are reported rather than panicking. Panics are reserved
for defects in the conformance harness itself.

The asynchronous fixture traits require Send-capable futures but do not select
an executor. Dropping a verifier future requests cancellation. Fixtures own
cleanup for durable test state and must document backend work that can continue
after cancellation.

## Security and Data

The harness accepts only synthetic fixture credentials and bounded recording
values. Debug output and errors must not reveal live credentials, bearer
tokens, connection strings, claim capabilities, or unbounded application
payloads. Probe structs are in-memory test results, not persisted or wire
formats.

## Ownership and Verification

Hubuum maintainers own the crate. CI packages it, builds rustdoc with warnings
denied, compares it with the latest crates.io release, and runs its verifiers
through every selectable in-repository backend. Adapter authors additionally
own their backend-native failure and consistency tests.
