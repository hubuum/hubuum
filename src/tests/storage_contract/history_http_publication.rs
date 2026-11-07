//! The real HTTP middleware must settle the PostgreSQL publication after its
//! original API owner is gone, using the native exact-owner control entry.
use super::*;
use futures_util::FutureExt;
use hubuum_storage_core::{DeletionControlStorage, HistoryLifecycleStorage};
use std::panic::{AssertUnwindSafe, resume_unwind};

use crate::middlewares::history_publication::tests::{
    Observations, ResponseMode, verify_middleware,
};

#[actix_web::test]
async fn buffered_http_keeps_its_postgres_publication_beyond_api_settlement() {
    let observations = Arc::new(Observations::default());
    let adapter = AdapterPostgresStorage::new(
        pool().get_ref().clone(),
        Arc::new(RecordingPostgresObserver::default()),
    );
    let backend =
        StorageHandle::from_postgres_backend_with_storage_observer(adapter, observations.clone());
    let user = create_backend_user(&backend, &prefix("http_publication")).await;
    let registration = backend.register_history_runtime(None).await.unwrap();
    let storage = backend.clone().with_runtime_owner(registration.owner());
    let checked = AssertUnwindSafe(verify_middleware(
        storage,
        observations,
        user.principal_id,
        user.raw_token.clone(),
        ResponseMode::Buffered,
    ))
    .catch_unwind()
    .await;
    let cleanup = backend.unregister_runtime(registration).await;
    delete_backend_user(&backend, user).await;
    if let Err(panic) = checked {
        resume_unwind(panic);
    }
    cleanup.unwrap();
}
