# `hubuum-events-core` Rust API Policy

Status: experimental public API in the storage SDK `0.1` release train.

## Purpose and Callers

`hubuum-events-core` defines Hubuum's backend-neutral event catalog, envelopes,
and mutation provenance. Applications, event integrations, and storage adapters
may use these types without depending on the server crate.

## Compatibility

The crate follows the lockstep versioning, exact dependency, MSRV, feature,
deprecation, and release rules in the
[Storage Adapter SDK Compatibility policy](../storage_adapter_sdk.md). The
release-train MSRV is Rust 1.88.

The default feature set is empty. The `schema` feature adds Utoipa schema
implementations and is supported. Serialized event values are compatible only
where their type documentation or Hubuum's persisted event format says so;
ordinary Rust helper representations are not independent wire protocols.

Event envelopes and subscription filters are intentional serialized
integration DTOs. Event envelopes keep their representation private, expose
typed accessors, and require a fallible builder that validates catalog pairs,
actor kind, UTC time, JSON payload shape, and schema version. Subscription
filters retain a serialized field representation and are validated when they
enter subscription boundary types. Mutation helpers keep their representation
private, and catalog parsing uses semantic `parse` methods; database column
terminology is not part of the public API.

## Errors, Runtime, and Security

The crate performs no I/O and requires no asynchronous runtime. Constructors
return explicit errors for invalid caller input and do not intentionally panic.
Debug implementations and event APIs must not expose credential material.
Cancellation is not applicable.

## Ownership and Verification

Hubuum maintainers own the crate. Storage and event-sink consumers verify its
behavior. CI packages it, builds rustdoc with warnings denied, and compares it
with the latest crates.io release when a baseline exists.
