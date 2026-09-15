# `hubuum-schema-diagnostics` Rust API Policy

Status: experimental public API in the storage SDK `0.3` release train;
standalone publication is under consideration and is not part of this change.

## Purpose and callers

Converts `jsonschema` validation errors into bounded, structured, value-redacted
repair diagnostics. It is usable without any other Hubuum crate. The upstream
validator types accepted as input are the intentional integration surface;
output representations, errors, and invariants belong to this crate.

## Compatibility and release

The crate currently follows the coordinated versioning, Rust 1.88 MSRV,
deprecation, and release rules of the
[storage SDK policy](../storage_adapter_sdk.md). Its optional `openapi` feature
adds Utoipa schema implementations. It has no asynchronous runtime requirement.
Separating its release cadence or selecting a standalone package name requires
a later publication review; declaring it publishable keeps the SDK's dependency
graph packageable and does not publish it automatically.

## Security and verification

Scalar instance values and undeclared property names are redacted. Schema-owned
constraints may be retained and must be appropriate for the intended reader.
The caller owns compilation and validation admission budgets. Collection bounds
and explicitly reported omissions must survive serialization. Hubuum maintainers
own this crate. Unit tests cover precise array and escaped locations, local
references, redaction, alternative branches, and limits; storage integration
tests verify persisted use without revalidation.
