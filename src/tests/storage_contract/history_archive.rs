use super::history_fixtures::owner_user;
use super::*;
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::{ResourceFields, ResourceKind};
use crate::services::archive::{
    HistoryArchiveAdmission, HistoryArchiveQuery, HistoryArchiveRequest,
};
use crate::services::history::segments::{
    HistorySegment, RetainedHistorySegment, SegmentRecord, SegmentScope, VersionInterval,
};
use crate::storage::storage_handle;
use chrono::{Duration as TimeDelta, Utc};
use hubuum_artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactReference, ArtifactWriter,
    Destination, DestinationId,
};
use hubuum_budget::ByteLimit;
use hubuum_domain::{PrincipalIdentity, ResourceIdentity, VersionIdentity};
use hubuum_storage_core::*;
use rstest::rstest;
use serde_json::json;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::LazyLock;

static ARCHIVE_METRICS: LazyLock<()> =
    LazyLock::new(|| crate::observability::metrics::init().unwrap());

// Deliberately oversized rows must never enter the shared application database:
// unrelated full-backup contracts enforce a smaller per-row limit. Cargo runs
// integration binaries after this library binary; only these two library cases
// use the runner's separately provisioned restore database.
#[cfg(feature = "integration-test-support")]
async fn with_overflow_backend(test: impl for<'a> AsyncFn(&'a mut TestContext)) {
    let _guard = crate::test_support::RESTORE_TEST_LOCK.lock().await;
    for kind in [StorageBackendKind::Memory, StorageBackendKind::Postgres] {
        let owner_url = crate::test_support::restore_test_migration_database_url();
        let target = hubuum_storage_postgres::test_support::IsolatedRestoreTarget::new(
            &owner_url,
            &hubuum_storage_postgres::test_support::integration_test_database_url(),
        )
        .unwrap();
        let owner_pool = crate::test_support::postgres_test_pool_with_timeout(
            &owner_url,
            8,
            crate::config::DEFAULT_DB_STATEMENT_TIMEOUT_MS,
        );
        let fixture = crate::test_support::RestoreContractFixture::new(kind, owner_pool.clone())
            .await
            .unwrap();
        let backend = match kind {
            StorageBackendKind::Memory => fixture.storage().clone(),
            StorageBackendKind::Postgres => {
                let runtime_url = crate::test_support::restore_test_runtime_database_url();
                let pool = crate::test_support::postgres_test_pool_with_timeout(
                    &runtime_url,
                    8,
                    crate::config::DEFAULT_DB_STATEMENT_TIMEOUT_MS,
                );
                crate::storage::StorageHandle::postgres(pool)
            }
        };
        let result =
            AssertUnwindSafe(TestContext::run(backend, async |ctx| {
                let outcome = AssertUnwindSafe(test(ctx)).catch_unwind().await;
                if kind == StorageBackendKind::Postgres {
                    hubuum_storage_postgres::test_support::delete_isolated_history_archive_captures(
                    &owner_pool, &target, ctx.permit.owner().unwrap().id(), ctx.resource,
                ).await.unwrap();
                }
                if let Err(panic) = outcome {
                    resume_unwind(panic);
                }
            }))
            .catch_unwind()
            .await;
        fixture.restore_original().await.unwrap();
        if let Err(panic) = result {
            resume_unwind(panic);
        }
    }
}

struct TestContext {
    storage: StorageHandle,
    context: AppContext,
    fixture: BackendObjectFixture,
    user: BackendUserFixture,
    subject: StorageAuthenticationPrincipal,
    resource: ResourceIdentity,
    principal: PrincipalIdentity,
    permit: StorageExecutionPermit,
    archive_permit: HistoryArchiveExecutionPermit,
    archive_work: Option<HistoryArchiveExecutionOwner>,
    registration: Option<RuntimeRegistration>,
}
impl TestContext {
    async fn run(storage: StorageHandle, test: impl for<'a> AsyncFnOnce(&'a mut Self)) {
        // Exercise the finite operation-label contract even in focused runs,
        // independently of whether another test initialized global metrics.
        LazyLock::force(&ARCHIVE_METRICS);
        let fixture =
            create_backend_object_fixture(&storage, &prefix("history_archive"), vec![]).await;
        let user = owner_user(&storage, &fixture).await;
        let subject = storage
            .get_authentication_identity(user.principal_id)
            .await
            .unwrap()
            .into_parts()
            .0;
        let evidence = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: fixture.collection.id().into(),
            })
            .await
            .unwrap();
        let resource = evidence
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap()
            .resource();
        let principal = match storage
            .resolve_logical_identity(StorageIdentityTarget::Principal(user.principal_id))
            .await
            .unwrap()
            .subject()
            .unwrap()
        {
            StorageIdentitySubject::Principal { identity } => identity.known_identity().unwrap(),
            _ => panic!("principal identity"),
        };
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let archive_work = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(permit.clone()),
                storage.admit_history_archive_execution(),
            )
            .await
            .unwrap();
        let archive_permit = archive_work.permit().clone();
        let bound = storage.clone().with_runtime_owner(registration.owner());
        let policy = Arc::new(MockTreetopBackend::new());
        policy.add_rule(MockAllowRule {
            group_id: fixture.owned_group.as_ref().unwrap().id().id(),
            action: crate::models::Permissions::ReadCollection,
            resource_kind: ResourceKind::Collection,
            resource_id: None,
            attrs: ResourceFields {
                collection_id: Some(fixture.collection.id().id()),
                ..Default::default()
            },
        });
        let context = AppContext::new(bound, policy);
        let mut context = Self {
            storage,
            context,
            fixture,
            user,
            subject,
            resource,
            principal,
            permit,
            archive_permit,
            archive_work: Some(archive_work),
            registration: Some(registration),
        };
        let result = AssertUnwindSafe(test(&mut context)).catch_unwind().await;
        if result.is_ok() {
            // Only this fixture's direct, fully awaited operations are covered.
            // No HTTP/transport/serialization consumer is issued here. Service
            // query owners are independent and are NOT retired by this cleanup.
            let work = context.archive_work.take().unwrap();
            let completed = work
                .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
                .await
                .unwrap();
            context
                .storage
                .settle_history_producer(completed.producer())
                .await
                .unwrap();
            context
                .storage
                .settle_history_publication(completed.into_publication())
                .await
                .unwrap();
        }
        // Panic/unknown work retains its own obligation; no registry reset.
        if let Some(registration) = context.registration.take() {
            context
                .storage
                .unregister_runtime(registration)
                .await
                .unwrap();
        }
        delete_backend_user(&context.storage, context.user).await;
        delete_backend_object_fixture(&context.storage, context.fixture).await;
        if let Err(panic) = result {
            resume_unwind(panic);
        }
    }
    async fn with_archive<F: std::future::Future>(&self, future: F) -> F::Output {
        let scope = self
            .archive_permit
            .reenter_scope(&self.storage.current_execution_scope())
            .expect("original fixture authority");
        self.storage.run_in_scope(scope, future).await
    }
    async fn capture(&self, point: HistoryArchivePoint) -> HistoryArchiveCapture {
        let catalog = self
            .with_archive(
                self.storage
                    .history_archive_catalog(self.resource, &self.archive_permit),
            )
            .await
            .unwrap();
        self.with_archive(self.storage.capture_history_archive(
            HistoryArchiveCaptureRequest::new(
                self.principal,
                HistoryArchiveSelection::new(
                    StorageBackupHistorySection::CollectionHistory,
                    self.resource,
                    point,
                ),
                catalog.generation(),
                Utc::now() + TimeDelta::hours(1),
            ),
            &self.archive_permit,
        ))
        .await
        .unwrap()
    }
    async fn query(&self, point: HistoryArchivePoint) -> HistoryArchiveQuery {
        match HistoryArchiveQuery::capture(
            &self.context,
            &self.subject,
            None,
            HistoryArchiveRequest::collection(
                self.fixture.collection.id(),
                point,
                Utc::now() + TimeDelta::hours(1),
            ),
        )
        .await
        .unwrap()
        {
            HistoryArchiveAdmission::Captured(query) => *query,
            HistoryArchiveAdmission::BudgetLimited(_) => panic!("bounded fixture"),
        }
    }
    async fn close(&self) {
        self.storage
            .collection_store()
            .update_collection(
                self.fixture.collection.id(),
                StorageCollectionUpdate::new(None, Some(prefix("updated"))),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
    }
    async fn closed(&self) -> HistoryArchiveRow {
        let initial = self
            .capture(HistoryArchivePoint::AsOf { at: Utc::now() })
            .await
            .hot()
            .unwrap()
            .identity();
        self.close().await;
        self.capture(HistoryArchivePoint::Exact {
            version: initial.version(),
        })
        .await
        .hot()
        .unwrap()
        .clone()
    }
    async fn descriptor(
        &self,
        row: &HistoryArchiveRow,
        corrupt: bool,
        dependencies: Vec<VersionIdentity>,
    ) -> HistoryArchiveDescriptor {
        self.with_archive(self.descriptor_in_scope(row, corrupt, dependencies))
            .await
    }
    async fn descriptor_in_scope(
        &self,
        row: &HistoryArchiveRow,
        corrupt: bool,
        dependencies: Vec<VersionIdentity>,
    ) -> HistoryArchiveDescriptor {
        let store = self.storage.artifact_store();
        let destination_id = DestinationId::generate();
        let destination = Destination::new(
            destination_id,
            format!("history-archive-{}", String::from(destination_id)),
        )
        .unwrap();
        store.register_destination(&destination).await.unwrap();
        let scope = SegmentScope::new(
            StorageBackupHistorySection::CollectionHistory,
            self.resource,
        );
        let payload = if corrupt {
            let mut payload = row.payload().clone();
            payload["description"] = json!("substituted");
            payload
        } else {
            row.payload().clone()
        };
        let record = SegmentRecord::new(
            scope.clone(),
            VersionInterval::new(
                self.resource,
                row.identity().version(),
                row.valid_from(),
                row.valid_to().unwrap(),
            )
            .unwrap(),
            payload,
        )
        .with_dependencies(dependencies.clone())
        .unwrap();
        let segment = HistorySegment::stage(
            store.as_ref(),
            ArtifactIdentity::new(destination.id()),
            scope,
            vec![record],
        )
        .await
        .unwrap();
        let reference = store.publish(segment.proof()).await.unwrap();
        HistoryArchiveDescriptor::new(row, dependencies, 1, reference).unwrap()
    }
    async fn malformed_descriptor(
        &self,
        row: &HistoryArchiveRow,
        overlap: bool,
    ) -> HistoryArchiveDescriptor {
        self.with_archive(self.malformed_descriptor_in_scope(row, overlap))
            .await
    }
    async fn malformed_descriptor_in_scope(
        &self,
        row: &HistoryArchiveRow,
        overlap: bool,
    ) -> HistoryArchiveDescriptor {
        let store = self.storage.artifact_store();
        let destination_id = DestinationId::generate();
        let destination = Destination::new(
            destination_id,
            format!("malformed-index-{}", String::from(destination_id)),
        )
        .unwrap();
        store.register_destination(&destination).await.unwrap();
        let scope = SegmentScope::new(
            StorageBackupHistorySection::CollectionHistory,
            self.resource,
        );
        let end = row.valid_to().unwrap();
        let records = vec![
            SegmentRecord::new(
                scope.clone(),
                VersionInterval::new(
                    self.resource,
                    row.identity().version(),
                    row.valid_from(),
                    end,
                )
                .unwrap(),
                row.payload().clone(),
            ),
            SegmentRecord::new(
                scope.clone(),
                VersionInterval::new(
                    self.resource,
                    VersionIdentity::generate(),
                    end,
                    end + TimeDelta::seconds(1),
                )
                .unwrap(),
                row.payload().clone(),
            ),
        ];
        let segment = HistorySegment::stage(
            store.as_ref(),
            ArtifactIdentity::new(destination.id()),
            scope,
            records,
        )
        .await
        .unwrap();
        let manifest = segment.proof().manifest();
        // Only this tiny synthetic fixture is materialized, never production data.
        assert!(manifest.byte_size() < 64 * 1024);
        let mut bytes = Vec::new();
        for ordinal in 0..manifest.block_count() {
            bytes.extend_from_slice(
                &store
                    .block(manifest.identity(), ordinal)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        store.abort(manifest.identity()).await.unwrap();
        let footer = bytes.len() - 9;
        let index_length =
            u32::from_le_bytes(bytes[footer..footer + 4].try_into().unwrap()) as usize;
        let index_start = footer - index_length;
        let mut index: serde_json::Value =
            serde_json::from_slice(&bytes[index_start..footer]).unwrap();
        let field = if overlap { "valid_from" } else { "version" };
        let replacement = index[0]["interval"][field].clone();
        index[1]["interval"][field] = replacement;
        let index = serde_json::to_vec(&index).unwrap();
        bytes.truncate(index_start);
        bytes.extend_from_slice(&index);
        bytes.extend_from_slice(&(index.len() as u32).to_le_bytes());
        bytes.extend_from_slice(b"HUBI\x01");
        let limits = ArtifactLimits::builder()
            .block_bytes(512 * 1024)
            .artifact_bytes(64 * 1024)
            .build()
            .unwrap();
        let mut writer = ArtifactWriter::new(
            store.as_ref(),
            ArtifactIdentity::new(destination.id()),
            limits,
        )
        .await
        .unwrap();
        writer.write(&bytes).await.unwrap();
        let reference = writer.finish().await.unwrap();
        let malformed = RetainedHistorySegment::open(
            store.as_ref(),
            &reference,
            &mut ByteLimit::new(16 * 1024 * 1024).start(),
        )
        .await;
        let reason = if overlap {
            "Overlapping history validity intervals"
        } else {
            "Duplicate history version identity"
        };
        assert!(matches!(malformed,Err(ArtifactError::Corrupt(message)) if message==reason));
        HistoryArchiveDescriptor::new(row, vec![], 1, reference).unwrap()
    }

    async fn publish(&self, descriptor: HistoryArchiveDescriptor) -> HistoryArchiveCatalog {
        let catalog = self
            .with_archive(
                self.storage
                    .history_archive_catalog(self.resource, &self.archive_permit),
            )
            .await
            .unwrap();
        self.with_archive(self.storage.publish_history_archive(
            catalog.generation(),
            vec![descriptor],
            &self.archive_permit,
        ))
        .await
        .unwrap()
    }
}

#[rstest]
#[case::exact(false)]
#[case::as_of(true)]
#[actix_web::test]
async fn catalog_persists_and_service_reads_retained_segment(#[case] as_of: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            let descriptor = ctx.descriptor(&row, false, vec![]).await;
            let published = ctx.publish(descriptor).await;
            let loaded = ctx
                .with_archive(
                    ctx.storage
                        .history_archive_catalog(ctx.resource, &ctx.archive_permit),
                )
                .await
                .unwrap();
            assert_eq!(loaded, published);
            let point = if as_of {
                HistoryArchivePoint::AsOf {
                    at: row.valid_from(),
                }
            } else {
                HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                }
            };
            let query = ctx.query(point).await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.row(), Some(&row));
            assert!(view.charged_read_bytes() > 0);
        })
        .await;
    }
}

#[actix_web::test]
async fn captured_open_version_is_unchanged_by_later_closure() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let expected = ctx
                .capture(HistoryArchivePoint::AsOf { at: Utc::now() })
                .await
                .hot()
                .unwrap()
                .clone();
            let query = ctx
                .query(HistoryArchivePoint::AsOf { at: Utc::now() })
                .await;
            ctx.close().await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.row(), Some(&expected));
        })
        .await;
    }
}

#[actix_web::test]
async fn catalog_replacement_keeps_admitted_location_and_generation() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            let first = ctx.publish(ctx.descriptor(&row, false, vec![]).await).await;
            let query = ctx
                .query(HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                })
                .await;
            let missing = ArtifactReference::from_catalog(
                ArtifactIdentity::new(DestinationId::generate()),
                [1; 32],
            );
            ctx.publish(HistoryArchiveDescriptor::new(&row, vec![], 1, missing).unwrap())
                .await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.generation(), first.generation());
            assert_eq!(view.row(), Some(&row));
        })
        .await;
    }
}

#[rstest]
#[case::missing(0, HistoryArchiveCoverageState::Unavailable)]
#[case::corrupt(1, HistoryArchiveCoverageState::Corrupt)]
#[case::dependencies(2, HistoryArchiveCoverageState::DependencyIncomplete)]
#[case::format(3, HistoryArchiveCoverageState::Unsupported)]
#[actix_web::test]
async fn incomplete_archives_never_claim_complete(
    #[case] kind: u8,
    #[case] expected: HistoryArchiveCoverageState,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            let descriptor = match kind {
                0 => HistoryArchiveDescriptor::new(
                    &row,
                    vec![],
                    1,
                    ArtifactReference::from_catalog(
                        ArtifactIdentity::new(DestinationId::generate()),
                        [1; 32],
                    ),
                )
                .unwrap(),
                1 => ctx.descriptor(&row, true, vec![]).await,
                2 => {
                    ctx.descriptor(&row, false, vec![VersionIdentity::generate()])
                        .await
                }
                _ => {
                    let descriptor = ctx.descriptor(&row, false, vec![]).await;
                    HistoryArchiveDescriptor::new(&row, vec![], 2, descriptor.location().clone())
                        .unwrap()
                }
            };
            ctx.publish(descriptor).await;
            let query = ctx
                .query(HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                })
                .await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.coverage(), expected);
            assert!(view.row().is_none());
        })
        .await;
    }
}

#[rstest]
#[case::complete_empty(true, HistoryArchiveCoverageState::Complete)]
#[case::unknown_version(false, HistoryArchiveCoverageState::Unavailable)]
#[actix_web::test]
async fn empty_coverage_is_distinct_from_missing_history(
    #[case] before: bool,
    #[case] expected: HistoryArchiveCoverageState,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let point = if before {
                HistoryArchivePoint::AsOf {
                    at: ctx.fixture.collection.created_at() - TimeDelta::seconds(1),
                }
            } else {
                HistoryArchivePoint::Exact {
                    version: VersionIdentity::generate(),
                }
            };
            let query = ctx.query(point).await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.coverage(), expected);
            assert!(view.row().is_none());
        })
        .await;
    }
}

#[actix_web::test]
async fn generation_race_has_one_catalog_winner() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            let descriptor = ctx.descriptor(&row, false, vec![]).await;
            let expected = HistoryArchiveCatalogGeneration::default();
            let (a, b) = tokio::join!(
                ctx.with_archive(ctx.storage.publish_history_archive(
                    expected,
                    vec![descriptor.clone()],
                    &ctx.archive_permit
                )),
                ctx.with_archive(ctx.storage.publish_history_archive(
                    expected,
                    vec![descriptor],
                    &ctx.archive_permit
                ))
            );
            assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        })
        .await;
    }
}

#[actix_web::test]
async fn changed_generation_rejects_capture_instead_of_refreshing() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            ctx.publish(ctx.descriptor(&row, false, vec![]).await).await;
            let result = ctx
                .with_archive(ctx.storage.capture_history_archive(
                    HistoryArchiveCaptureRequest::new(
                        ctx.principal,
                        HistoryArchiveSelection::new(
                            StorageBackupHistorySection::CollectionHistory,
                            ctx.resource,
                            HistoryArchivePoint::Exact {
                                version: row.identity().version(),
                            },
                        ),
                        HistoryArchiveCatalogGeneration::default(),
                        Utc::now() + TimeDelta::hours(1),
                    ),
                    &ctx.archive_permit,
                ))
                .await;
            assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Conflict);
        })
        .await;
    }
}

#[rstest]
#[case::execution(false)]
#[case::result(true)]
#[actix_web::test]
async fn authorization_is_rechecked_without_exposing_payload(#[case] result_access: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let query = ctx
                .query(HistoryArchivePoint::AsOf { at: Utc::now() })
                .await;
            let denied_context = AppContext::new(
                storage_handle(&ctx.context),
                Arc::new(MockTreetopBackend::new()),
            );
            let denied = if result_access {
                let result = query
                    .execute(&ctx.context, &ctx.subject, None)
                    .await
                    .unwrap();
                result
                    .access(&denied_context, &ctx.subject, None)
                    .await
                    .is_err()
            } else {
                query
                    .execute(&denied_context, &ctx.subject, None)
                    .await
                    .is_err()
            };
            assert!(denied);
        })
        .await;
    }
}

#[actix_web::test]
async fn retiring_runtime_invalidates_an_existing_result() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let query = ctx
                .query(HistoryArchivePoint::AsOf { at: Utc::now() })
                .await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            ctx.storage
                .unregister_runtime(ctx.registration.take().unwrap())
                .await
                .unwrap();
            assert!(
                result
                    .access(&ctx.context, &ctx.subject, None)
                    .await
                    .is_err()
            );
        })
        .await;
    }
}

#[actix_web::test]
async fn cumulative_read_reservations_do_not_reset_between_attempts() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let capture = ctx
                .capture(HistoryArchivePoint::AsOf { at: Utc::now() })
                .await;
            ctx.with_archive(ctx.storage.reserve_history_archive_read(
                capture.id(),
                ctx.principal,
                HISTORY_ARCHIVE_READ_BYTES,
                &ctx.archive_permit,
            ))
            .await
            .unwrap();
            let rejected = ctx
                .with_archive(ctx.storage.reserve_history_archive_read(
                    capture.id(),
                    ctx.principal,
                    1,
                    &ctx.archive_permit,
                ))
                .await;
            assert_eq!(
                rejected.unwrap_err().kind(),
                StorageErrorKind::InputTooLarge
            );
        })
        .await;
    }
}

#[derive(Clone, Copy)]
enum ArchiveAccessStage {
    Admission,
    Execution,
    Result,
}

#[rstest]
#[case::admission(ArchiveAccessStage::Admission, 2)]
#[case::execution(ArchiveAccessStage::Execution, 3)]
#[case::result(ArchiveAccessStage::Result, 1)]
#[actix_web::test]
async fn runtime_retirement_during_authorization_rejects_archive_access(
    #[case] stage: ArchiveAccessStage,
    #[case] authorization_calls_to_skip: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let point = HistoryArchivePoint::AsOf { at: Utc::now() };
            let mut query = match stage {
                ArchiveAccessStage::Admission => None,
                _ => Some(ctx.query(point.clone()).await),
            };
            let result = match stage {
                ArchiveAccessStage::Result => Some(
                    query
                        .take()
                        .unwrap()
                        .execute(&ctx.context, &ctx.subject, None)
                        .await
                        .unwrap(),
                ),
                _ => None,
            };
            let policy = Arc::new(MockTreetopBackend::new());
            policy.add_rule(MockAllowRule {
                group_id: ctx.fixture.owned_group.as_ref().unwrap().id().id(),
                action: crate::models::Permissions::ReadCollection,
                resource_kind: ResourceKind::Collection,
                resource_id: None,
                attrs: ResourceFields {
                    collection_id: Some(ctx.fixture.collection.id().id()),
                    ..Default::default()
                },
            });
            let storage = ctx.storage.clone();
            let registration = ctx.registration.take().unwrap();
            let retired = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = retired.clone();
            policy.set_authorization_hook_after_calls(
                authorization_calls_to_skip,
                move || async move {
                    storage.unregister_runtime(registration).await.unwrap();
                    observed.store(true, Ordering::SeqCst);
                },
            );
            let context = AppContext::new(storage_handle(&ctx.context), policy);
            let rejected = match stage {
                ArchiveAccessStage::Admission => HistoryArchiveQuery::capture(
                    &context,
                    &ctx.subject,
                    None,
                    HistoryArchiveRequest::collection(
                        ctx.fixture.collection.id(),
                        point,
                        Utc::now() + TimeDelta::hours(1),
                    ),
                )
                .await
                .is_err(),
                ArchiveAccessStage::Execution => query
                    .unwrap()
                    .execute(&context, &ctx.subject, None)
                    .await
                    .is_err(),
                ArchiveAccessStage::Result => result
                    .unwrap()
                    .access(&context, &ctx.subject, None)
                    .await
                    .is_err(),
            };
            assert!(
                retired.load(Ordering::SeqCst),
                "The final policy call must retire the runtime"
            );
            assert!(
                rejected,
                "Archive access must recheck its original runtime after policy work"
            );
        })
        .await;
    }
}

#[rstest]
#[case::no_owner(false)]
#[case::different_owner(true)]
#[actix_web::test]
async fn empty_catalog_and_pins_require_the_original_runtime(#[case] different: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let capture = ctx
                .capture(HistoryArchivePoint::AsOf { at: Utc::now() })
                .await;
            let registration = if different {
                Some(ctx.storage.register_runtime().await.unwrap())
            } else {
                None
            };
            let permit = StorageExecutionPermit::new(
                registration.as_ref().map(RuntimeRegistration::owner),
                ctx.permit.fence(),
            );
            let scope = ctx
                .archive_permit
                .execution_scope()
                .clone()
                .with_application_access(permit);
            let rejected = ctx
                .storage
                .run_in_scope(scope, async {
                    if different {
                        ctx.storage
                            .history_archive_capture(
                                capture.id(),
                                ctx.principal,
                                &ctx.archive_permit,
                            )
                            .await
                            .is_err()
                    } else {
                        ctx.storage
                            .history_archive_catalog(ctx.resource, &ctx.archive_permit)
                            .await
                            .is_err()
                    }
                })
                .await;
            if let Some(registration) = registration {
                ctx.storage.unregister_runtime(registration).await.unwrap();
            }
            assert!(rejected);
        })
        .await;
    }
}

#[actix_web::test]
async fn unsupported_family_is_not_complete_empty() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let admitted = HistoryArchiveQuery::capture(
                &ctx.context,
                &ctx.subject,
                None,
                HistoryArchiveRequest::collection(
                    ctx.fixture.collection.id(),
                    HistoryArchivePoint::AsOf { at: Utc::now() },
                    Utc::now() + TimeDelta::hours(1),
                )
                .with_family(StorageBackupHistorySection::ObjectHistory),
            )
            .await
            .unwrap();
            let HistoryArchiveAdmission::Captured(query) = admitted else {
                panic!("bounded request")
            };
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.coverage(), HistoryArchiveCoverageState::Unsupported);
            assert!(view.row().is_none());
        })
        .await;
    }
}

#[cfg(feature = "integration-test-support")]
#[actix_web::test]
async fn capture_overflow_has_a_typed_bounded_admission_outcome() {
    with_overflow_backend(async |ctx| {
        ctx.storage
            .collection_store()
            .update_collection(
                ctx.fixture.collection.id(),
                StorageCollectionUpdate::new(
                    None,
                    Some("\n".repeat(HISTORY_ARCHIVE_ROW_BYTES / 2)),
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let result = HistoryArchiveQuery::capture(
            &ctx.context,
            &ctx.subject,
            None,
            HistoryArchiveRequest::collection(
                ctx.fixture.collection.id(),
                HistoryArchivePoint::AsOf { at: Utc::now() },
                Utc::now() + TimeDelta::hours(1),
            ),
        )
        .await
        .unwrap();
        let HistoryArchiveAdmission::Captured(query) = result else {
            panic!("bounded metadata capture")
        };
        let result = query
            .execute(&ctx.context, &ctx.subject, None)
            .await
            .unwrap();
        let view = result
            .access(&ctx.context, &ctx.subject, None)
            .await
            .unwrap();
        assert_eq!(view.coverage(), HistoryArchiveCoverageState::BudgetLimited);
        assert!(view.row().is_none());
    })
    .await;
}

#[cfg(feature = "integration-test-support")]
#[actix_web::test]
async fn overflow_coverage_requires_historical_snapshot_authorization() {
    with_overflow_backend(async |ctx| {
        ctx.storage
            .collection_store()
            .update_collection(
                ctx.fixture.collection.id(),
                StorageCollectionUpdate::new(
                    None,
                    Some("\n".repeat(HISTORY_ARCHIVE_ROW_BYTES / 2)),
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let identity = ctx
            .storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: ctx.fixture.collection.id().into(),
            })
            .await
            .unwrap()
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap();
        let name = prefix("visible_current_name");
        ctx.storage
            .collection_store()
            .update_collection(
                ctx.fixture.collection.id(),
                StorageCollectionUpdate::new(
                    Some(name.clone()),
                    Some("small current payload".into()),
                ),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let policy = Arc::new(MockTreetopBackend::new());
        policy.add_rule(MockAllowRule {
            group_id: ctx.fixture.owned_group.as_ref().unwrap().id().id(),
            action: crate::models::Permissions::ReadCollection,
            resource_kind: ResourceKind::Collection,
            resource_id: None,
            attrs: ResourceFields {
                collection_id: Some(ctx.fixture.collection.id().id()),
                name: Some(name),
                ..Default::default()
            },
        });
        let context = AppContext::new(storage_handle(&ctx.context), policy);
        let result = HistoryArchiveQuery::capture(
            &context,
            &ctx.subject,
            None,
            HistoryArchiveRequest::collection(
                ctx.fixture.collection.id(),
                HistoryArchivePoint::Exact {
                    version: identity.version(),
                },
                Utc::now() + TimeDelta::hours(1),
            ),
        )
        .await;
        assert!(result.is_err());
    })
    .await;
}

#[rstest]
#[case::duplicate_version(false)]
#[case::overlapping_intervals(true)]
#[actix_web::test]
async fn published_malformed_indexes_have_corrupt_coverage(#[case] overlap: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            ctx.publish(ctx.malformed_descriptor(&row, overlap).await)
                .await;
            let query = ctx
                .query(HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                })
                .await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.coverage(), HistoryArchiveCoverageState::Corrupt);
            assert!(view.row().is_none());
        })
        .await;
    }
}

#[cfg(feature = "integration-test-support")]
#[rstest]
#[case::authorized(0)]
#[case::historical_denied(1)]
#[case::revoked_policy(2)]
#[case::retired_owner(3)]
#[actix_web::test]
async fn staging_exhaustion_requires_original_historical_authority(#[case] scenario: u8) {
    with_overflow_backend(async |ctx| {
        let identity = ctx
            .storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: ctx.fixture.collection.id().into(),
            })
            .await
            .unwrap()
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap();
        let current_name = prefix("exhaustion_current");
        ctx.storage
            .collection_store()
            .update_collection(
                ctx.fixture.collection.id(),
                StorageCollectionUpdate::new(Some(current_name.clone()), None),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        let selection = HistoryArchiveSelection::new(
            StorageBackupHistorySection::CollectionHistory,
            ctx.resource,
            HistoryArchivePoint::Exact {
                version: identity.version(),
            },
        );
        let available_until = Utc::now() + TimeDelta::hours(1);
        let request = HistoryArchiveCaptureRequest::new(
            ctx.principal,
            selection.clone(),
            HistoryArchiveCatalogGeneration::default(),
            available_until,
        );
        // Exhaust the actual fixed count budget, without a test-only lower limit.
        for _ in 0..HISTORY_ARCHIVE_MAX_CAPTURES {
            ctx.with_archive(
                ctx.storage
                    .capture_history_archive(request.clone(), &ctx.archive_permit),
            )
            .await
            .unwrap();
        }
        let error = ctx
            .with_archive(
                ctx.storage
                    .capture_history_archive(request.clone(), &ctx.archive_permit),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), StorageErrorKind::InputTooLarge);
        error
            .history_archive_budget_evidence()
            .expect("post-preparation evidence")
            .validate_request(&request, &ctx.permit, Utc::now())
            .unwrap();
        error
            .history_archive_budget_evidence()
            .unwrap()
            .validate_execution(&ctx.archive_permit)
            .unwrap();
        assert!(!format!("{error:?}").contains(ctx.fixture.collection.name()));

        let policy = Arc::new(MockTreetopBackend::new());
        policy.add_rule(MockAllowRule {
            group_id: ctx.fixture.owned_group.as_ref().unwrap().id().id(),
            action: crate::models::Permissions::ReadCollection,
            resource_kind: ResourceKind::Collection,
            resource_id: None,
            attrs: ResourceFields {
                collection_id: Some(ctx.fixture.collection.id().id()),
                name: (scenario == 1).then_some(current_name),
                ..Default::default()
            },
        });
        let hook_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if scenario == 2 {
            let changed = policy.clone();
            let observed = hook_ran.clone();
            policy.set_authorization_hook_after_calls(1, move || async move {
                changed.revoke_all_rules();
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
            });
        } else if scenario == 3 {
            let storage = ctx.storage.clone();
            let registration = ctx.registration.take().unwrap();
            let observed = hook_ran.clone();
            policy.set_authorization_hook_after_calls(2, move || async move {
                storage.unregister_runtime(registration).await.unwrap();
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
            });
        }
        let context = AppContext::new(storage_handle(&ctx.context), policy);
        let outcome = HistoryArchiveQuery::capture(
            &context,
            &ctx.subject,
            None,
            HistoryArchiveRequest::collection(
                ctx.fixture.collection.id(),
                selection.point().clone(),
                available_until,
            ),
        )
        .await;
        match scenario {
            0 => assert!(matches!(
                outcome,
                Ok(HistoryArchiveAdmission::BudgetLimited(_))
            )),
            1 | 2 => assert!(matches!(outcome, Err(ApiError::Forbidden(_)))),
            3 => assert!(outcome.is_err()),
            _ => unreachable!(),
        }
        if scenario >= 2 {
            assert!(hook_ran.load(std::sync::atomic::Ordering::SeqCst));
        }
    })
    .await;
}

mod supervision;
