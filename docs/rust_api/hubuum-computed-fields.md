# `hubuum-computed-fields` Rust API Policy

Status: experimental public API in the storage SDK `0.1` release train.

## Purpose and Callers

`hubuum-computed-fields` owns the validated definition language and deterministic
evaluator used by Hubuum storage adapters. External adapters may accept and retain
`Definition` values received through `hubuum-storage-core`; they do not need to
reconstruct definitions from strings and untyped JSON.

The crate is a source API for statically linked Rust code. It is not a wire-format
or dynamic-plugin compatibility promise.

## Compatibility

The crate follows the lockstep versioning, exact dependency, MSRV, deprecation,
and upgrade rules in the
[Storage Adapter SDK Compatibility policy](../storage_adapter_sdk.md). The
release-train MSRV is Rust 1.88.

The definition and operation vocabulary is closed. Adding, removing, renaming,
or reinterpreting an operation, result type, error category, or serialized field
is an incompatible change. Evaluation limits may become more restrictive only
in an incompatible release unless required for an urgent security fix.

## Invariants and Serialization

`FieldKey`, `JsonPointer`, and `Definition` have private representations and
fallible construction. Deserialization applies the same semantic validation as
their public constructors; successfully obtaining a `Definition` therefore
proves that its key, sizes, operation arity, pointer uniqueness, result type, and
semantics version are valid.

Serialized definitions are persisted Hubuum data. Their field names, snake-case
enum names, defaults, and unknown-field rejection are compatibility-sensitive.
Callers must not synthesize serialized definitions as a substitute for the typed
constructors.

## Evaluation

Evaluation is deterministic for the same validated definition, JSON input,
limits, and crate version. Expected per-field data failures are returned in the
result; invalid definition construction, definition-count overflow, and caller
input errors are returned as typed errors and must not panic.

The evaluator performs no I/O, reads no global configuration, and starts no
background work. It is safe to cancel by dropping the calling future or task
because evaluation itself is synchronous and has no external side effects.
