# `hubuum-regex` Rust API Policy

Status: experimental public API in the storage SDK `0.13` release train.

## Purpose and Callers

`hubuum-regex` defines Hubuum's bounded, portable boolean regex language. It is
independently usable and depends on no Hubuum crate, matching engine, storage
adapter, database, transport, async runtime, or application configuration.

`Pattern::new` parses and validates untrusted text once. The private-fielded
`Pattern` and `Expression` types preserve that proof. `ExpressionKind`,
`CharacterRange`, and `Assertion` expose an immutable, normalized vocabulary to
compiler implementations without exposing `regex-syntax` implementation types.
Flags, Unicode properties, class operations, and simple case folding are
resolved into explicit Unicode scalar ranges. Captures and greedy preferences
are discarded because the contract exposes boolean search only.

Adapters implement `Compiler` with their own output and error types. Every
adapter must support the entire accepted language, preserve its semantics, and
run the engine-independent `conformance` corpus through native execution. The
shared crate does not select adapters or provide backend-specific emitters.
The [querying contract](../querying.md#regular-expressions) specifies syntax,
matching rules, and limits. Adding a backend must not change that contract.

## Compatibility

The crate follows the lockstep versioning, exact dependency, MSRV, deprecation,
and release rules in the
[Storage Adapter SDK Compatibility policy](../storage_adapter_sdk.md).
The release-train MSRV is Rust 1.88. There are no feature flags.

Accepted syntax, limits, Unicode classification and folding, and the normalized
expression vocabulary are part of the supported contract. Changes to these
semantics require an incompatible release during `0.x`. Review parser and
Unicode-table upgrades against the conformance corpus. There is no serialized
AST or dynamic compiler ABI.

## Errors, Runtime, and Security

Invalid syntax, unsupported assertions, and exceeded bounds return crate-owned
`Error` values. Parsing is synchronous and performs no I/O. It uses no global
state. Source bytes, syntax nesting, explicit repetition bounds, and expanded
expression complexity are bounded. Text consists of non-NUL Unicode scalars;
normalization excludes NUL and surrogate code points from character ranges.

A validated pattern does not waive native execution budgets. Adapters retain
execution timeouts and allocation limits. Native compilation failure for an
accepted pattern is an adapter failure, not a second dialect validation step.

## Ownership and Verification

Hubuum maintainers own the language and its conformance cases. Query tests
verify validation before storage access; each adapter runs the shared cases.
PostgreSQL integration tests additionally compare generated expressions with
memory execution. Workspace boundary tests enforce the crate's independence.
