# `hubuum-storage-core` Future Rust API Design

Status: workspace-internal; candidate for a later publication review.

## Purpose and Callers

`hubuum-storage-core` is the complete backend-neutral storage extension
contract. It exposes capability traits, private-field DTOs, the bounded
`StorageError` taxonomy, and the aggregate `StorageBackend` compile-time check.
The interface is designed so a backend crate may later live outside this
workspace, but this change does not publish or support that packaging yet.
Hubuum uses static Cargo composition; this is not a dynamic plugin ABI and has
no runtime contract version handshake.

The normative semantics of that Rust surface are documented in the
[storage contract](../storage_boundary/contract.md). The Rust declarations,
semantic coverage inventory, and contract documentation must change together.

The crate also exposes the mandatory `TransactionStorage` unit of work.
Applications compose safe lifecycle semantics through the crate-owned
operation types returned by `StorageTransaction`; native connections and query
interfaces remain private to each adapter.

Ordinary resource mutations require `EventContext` and return
`StorageMutationOutcome`. A committed outcome includes a non-empty set of
non-sensitive `StorageAuditReceipt` values for the durable events written atomically
with the state change; a genuine no-op returns `Unchanged`. Imports and
restores use explicit `ImportStorage` and `RestoreStorage` capabilities and do
not weaken ordinary mutation signatures.

Application composition supplies `StorageObserver`, keeping metrics exporters
and global registries out of adapter-neutral contracts.

The root `hubuum` crate remains an internal application composition crate.
HTTP clients should use Hubuum's versioned API instead of this storage API.

## Compatibility

There is no current third-party SemVer promise or crates.io release. A separate
promotion change must define the initial supported surface, versioning, and
adapter migration policy. The workspace MSRV is Rust 1.88.

There are no feature flags. DTOs are in-memory Rust contracts, not durable or
wire formats unless their documentation explicitly says otherwise. Backend
implementations are expected to implement every supertrait of `StorageBackend`;
focused test doubles may implement only the traits under test.

Lifecycle operations, record metadata, principal projections, and revision
preconditions use `hubuum-domain` IDs and `ResourceRevision`. Revision targets
are a closed semantic enum rather than adapter-formatted owner keys. Query
methods accept validated `hubuum-query` values, and errors use storage-domain
classifications rather than HTTP status or database-driver names. Native table,
row, connection, query, and ETag representations are adapter or application
concerns.

Task execution uses distinct active and terminal update types. Claims validate
active status, lease presence, and task identity; completion construction
validates the documented task-kind/artifact matrix before an adapter receives
the request. Adapters must still compare the declared completion kind with the
claimed persisted task.

## Errors, Runtime, and Cancellation

Backend-specific errors must be classified into `StorageError` before returning
through a storage trait. Applications translate `StorageError` into their own
transport errors. Caller input must return an error rather than panic.

Async methods require a Send-capable executor but do not prescribe Tokio or an
I/O driver. Dropping a returned future requests cancellation; a backend must
document and test any operation that can continue or commit after cancellation.
Multi-step writes that promise atomicity must use the backend's transaction
mechanism. A `TransactionStorage` implementation commits when its callback
returns `Ok` and rolls back when it returns `Err`. Transaction-scoped mutations
inherit one required event context, and adapters must commit or roll back state
and audit side effects together.

## Security and Observability

Private fields and validating constructors preserve boundary invariants.
Implementations must enforce visibility and permission inputs rather than treat
them as hints. Debug implementations must remain bounded and redact identifiers,
credentials, payloads, filters, and tokens where documented. Application
composition wraps logical entrypoints with `StorageObserver`. Adapters may
accept separate application-supplied native observers for implementation-level
telemetry; both observer layers must use bounded capability and operation
labels.

## Ownership and Verification

Hubuum maintainers own the workspace crate. The PostgreSQL adapter is the
reference implementation, and shared compatibility tests exercise every
statically registered backend. The
workspace-internal `hubuum-storage-conformance` harness certifies durable
receipts, no-op behavior, rollback, outbox-to-sink delivery, telemetry, exact
revision conflicts, retention retry identity, delivery recovery, restore
coordination rollback, and lease-loss finalization, while each backend owns
native query, transaction, migration, connection-loss, and failure tests.
External-crate integration tests implement all 44 complete-backend traits,
compile every one of their 249 methods, exercise every transaction port, and
name public construction paths for all current adapter-returned values. This
prevents accidental reliance on crate-private adapter hooks before a later
promotion review.
