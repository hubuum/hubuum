# `hubuum-storage-core` Rust API Policy

Status: experimental public API.

## Purpose and Callers

`hubuum-storage-core` is the complete backend-neutral storage extension
contract. It exposes capability traits, private-field DTOs, the bounded
`StorageError` taxonomy, and the aggregate `StorageBackend` compile-time check.
Backend crates may live outside this workspace and depend through crates.io,
Git, or a path. Hubuum uses static Cargo composition; this is not a dynamic
plugin ABI and has no runtime contract version handshake.

The crate also exposes the mandatory `TransactionalStorage` unit of work.
Applications compose safe lifecycle semantics through the crate-owned
operation types returned by `StorageTransaction`; native connections and query
interfaces remain private to each adapter.

The root `hubuum` crate remains an internal application composition crate.
HTTP clients should use Hubuum's versioned API instead of this storage API.

## Compatibility

The crate follows SemVer from its first crates.io release. During the `0.x`
experimental period, adding a mandatory backend operation or changing a DTO is
an incompatible API change requiring a minor-version bump, a changelog entry,
and backend migration guidance. The MSRV is Rust 1.88.

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

## Errors, Runtime, and Cancellation

Backend-specific errors must be classified into `StorageError` before returning
through a storage trait. Applications translate `StorageError` into their own
transport errors. Caller input must return an error rather than panic.

Async methods require a Send-capable executor but do not prescribe Tokio or an
I/O driver. Dropping a returned future requests cancellation; a backend must
document and test any operation that can continue or commit after cancellation.
Multi-step writes that promise atomicity must use the backend's transaction
mechanism. A `TransactionalStorage` implementation commits when its callback
returns `Ok` and rolls back when it returns `Err`. Transaction-scoped mutations
inherit one required event context, and adapters must commit or roll back state
and audit side effects together.

## Security and Observability

Private fields and validating constructors preserve boundary invariants.
Implementations must enforce visibility and permission inputs rather than treat
them as hints. Debug implementations must remain bounded and redact identifiers,
credentials, payloads, filters, and tokens where documented. Storage entrypoint
logging and metrics belong to application composition or the adapter and must
use bounded family and operation labels.

## Ownership and Verification

Hubuum maintainers own releases. CI builds rustdoc with warnings denied, tests a
clean package, and compares SemVer compatibility with the latest crates.io
release. The PostgreSQL adapter is the pinned reference implementation. Shared
compatibility tests exercise every statically registered backend, while each
backend owns native query, transaction, migration, and failure tests.
An external-crate integration test also compiles representative transaction,
query, and typed DTO usage so accidental reliance on crate-private adapter
hooks fails before publication.
