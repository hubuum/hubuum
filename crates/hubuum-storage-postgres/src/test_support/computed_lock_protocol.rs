//! Test-only visibility for the actual adapter-private lock protocol. These
//! reexports do not exist in production builds or the backend-neutral API.
//!
//! The valid acquisition sequence must compile (without a database connection):
//!
//! ```no_run
//! use hubuum_storage_postgres::PostgresStorageError;
//! use hubuum_storage_postgres::test_support::computed_lock_protocol::{PostgresRuntime, with_computed_transaction};
//!
//! async fn repair(runtime: &PostgresRuntime) -> Result<(), PostgresStorageError> {
//!     with_computed_transaction(runtime, async |transaction| {
//!         transaction.lock_class(1).await?.lock_objects(&[1]).await?.repair().await?;
//!         Ok(())
//!     }).await
//! }
//! ```
//!
//! Protected object locks require the class capability:
//!
//! ```compile_fail,E0599
//! use hubuum_storage_postgres::test_support::computed_lock_protocol::ComputedTransaction;
//!
//! async fn objects_first(transaction: ComputedTransaction<'_>) {
//!     transaction.lock_objects(&[1]).await.unwrap();
//! }
//! ```
//!
//! The class capability cannot escape the transaction that acquired it:
//!
//! ```compile_fail
//! use hubuum_storage_postgres::test_support::computed_lock_protocol::{PostgresRuntime, with_computed_transaction};
//!
//! async fn use_after_commit(runtime: &PostgresRuntime) {
//!     let class = with_computed_transaction(runtime, async |transaction| {
//!         transaction.lock_class(1).await
//!     }).await.unwrap();
//!     class.lock_objects(&[1]).await.unwrap();
//! }
//! ```
//!
//! Callers cannot bypass the protocol through the borrowed connection:
//!
//! ```compile_fail,E0616
//! use hubuum_storage_postgres::test_support::computed_lock_protocol::ComputedTransaction;
//!
//! fn raw_connection(transaction: ComputedTransaction<'_>) {
//!     let _ = transaction.connection;
//! }
//! ```
//!
//! A capability cannot be invalidated through rollback or savepoints:
//!
//! ```compile_fail,E0599
//! use hubuum_storage_postgres::test_support::computed_lock_protocol::LockedComputedClass;
//!
//! async fn rollback_then_reuse(class: LockedComputedClass<'_>) {
//!     class.rollback().await.unwrap();
//!     class.lock_objects(&[1]).await.unwrap();
//! }
//! ```
//!
//! Dropping a class token does not restore the entry capability or allow a
//! second class acquisition while the transaction still holds earlier locks:
//!
//! ```compile_fail,E0382
//! use hubuum_storage_postgres::test_support::computed_lock_protocol::ComputedTransaction;
//!
//! async fn acquire_another_class(transaction: ComputedTransaction<'_>) {
//!     let class = transaction.lock_class(1).await.unwrap();
//!     drop(class.lock_objects(&[1]).await.unwrap());
//!     transaction.lock_class(2).await.unwrap();
//! }
//! ```

pub use crate::operations::computed_fields::locking::{
    ComputedTransaction, LockedComputedClass, LockedComputedObjects, with_computed_transaction,
};

pub use crate::operations::computed_materialization::acquire_computed_class_exclusive_lock;
pub use crate::runtime::PostgresRuntime;
