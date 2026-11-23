use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_execution_context() {
    for backend in available_backends() {
        let evaluations = Arc::new(AtomicUsize::new(0));

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope(
                StorageExecutionScope::default().with_call_site(StorageCallSite::Readiness),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope_send(
                StorageExecutionScope::default().with_call_site(StorageCallSite::TaskLease),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope(
                StorageExecutionScope::default()
                    .with_mutation_provenance(Some(MutationProvenance::system())),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        let evaluated = Arc::clone(&evaluations);
        backend
            .run_in_scope(
                StorageExecutionScope::default().with_revision_precondition(Some(
                    StorageRevisionPrecondition::new(
                        StorageRevisionTarget::Collection(CollectionID::new(1).unwrap()),
                        vec![crate::models::ResourceRevision::INITIAL],
                    ),
                )),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                },
            )
            .await;

        assert_eq!(AtomicUsize::load(evaluations.as_ref(), Ordering::SeqCst), 4);
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_the_export_query_scope() {
    for backend in available_backends() {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::clone(&evaluations);
        let output = backend
            .run_in_scope(
                StorageExecutionScope::default()
                    .with_query_budget(StorageQueryBudget::from_millis(250)),
                async move {
                    evaluated.fetch_add(1, Ordering::SeqCst);
                    "complete"
                },
            )
            .await;

        assert_eq!(output, "complete");
        assert_eq!(AtomicUsize::load(evaluations.as_ref(), Ordering::SeqCst), 1);
    }
}
