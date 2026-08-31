#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use chrono::{Duration, Utc};
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use rstest::rstest;

    use crate::models::{
        BackupDocument, BackupManifest, BackupState, Permissions, RESTORE_CONFIRMATION_PHRASE,
        RestoreConfirmRequest, RestoreJobStatus, RestoreStageResponse,
    };
    use crate::schema::restore_jobs::dsl::{id, restore_jobs};
    use crate::tests::api_operations::{get_request_with_headers, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{TestContext, scoped_token, test_context};
    use hubuum_storage_core::{
        StorageBackupRow, StorageBackupStateSection, StorageBackupStateSections,
    };
    use hubuum_storage_postgres::with_connection;

    #[derive(Clone, Copy)]
    enum RejectedRestoreCaller {
        NormalUser,
        ScopedAdministrator,
    }

    #[derive(Clone, Copy)]
    enum MissingRestoreSeed {
        LocalIdentityScope,
        RootCollection,
        RootClosure,
    }

    fn empty_full_backup_document() -> BackupDocument {
        let sections = StorageBackupStateSection::ALL
            .iter()
            .copied()
            .map(|section| (section, Vec::new()))
            .collect::<StorageBackupStateSections>();

        BackupDocument {
            backup_version: crate::models::CURRENT_BACKUP_VERSION,
            created_at: Utc::now(),
            source_version: env!("CARGO_PKG_VERSION").to_string(),
            state: BackupState { sections },
            history: None,
            manifest: BackupManifest::default(),
        }
    }

    fn minimally_valid_full_backup_document() -> BackupDocument {
        let mut document = empty_full_backup_document();
        document
            .state
            .sections
            .get_mut(&StorageBackupStateSection::IdentityScopes)
            .unwrap()
            .push(
                StorageBackupRow::try_from_value(serde_json::json!({
                    "id": 1,
                    "name": "local",
                    "provider_kind": "local",
                    "revision": 1
                }))
                .unwrap(),
            );
        document
            .state
            .sections
            .get_mut(&StorageBackupStateSection::Collections)
            .unwrap()
            .push(
                StorageBackupRow::try_from_value(serde_json::json!({
                    "id": 1,
                    "name": "root",
                    "parent_collection_id": null,
                    "revision": 1
                }))
                .unwrap(),
            );
        document
            .state
            .sections
            .get_mut(&StorageBackupStateSection::CollectionAuthorization)
            .unwrap()
            .push(
                StorageBackupRow::try_from_value(serde_json::json!({
                    "collection_id": 1,
                    "revision": 1
                }))
                .unwrap(),
            );
        document
            .state
            .sections
            .get_mut(&StorageBackupStateSection::CollectionHierarchy)
            .unwrap()
            .push(
                StorageBackupRow::try_from_value(serde_json::json!({
                    "ancestor_collection_id": 1,
                    "descendant_collection_id": 1,
                    "depth": 0
                }))
                .unwrap(),
            );
        document
    }

    #[rstest]
    #[actix_web::test]
    async fn administrator_can_stage_and_inspect_restore_via_api(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let document = minimally_valid_full_backup_document();

        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/restores",
            &document,
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let stage_cache_control = header_value(&response, "Cache-Control");
        let staged: RestoreStageResponse = test::read_body_json(response).await;
        assert_eq!(
            (stage_cache_control.as_deref(), staged.status),
            (Some("no-store"), RestoreJobStatus::Validated)
        );
        let capability = staged
            .restore_capability
            .clone()
            .expect("staging should return a restore capability");

        let response = get_request_with_headers(
            &context.pool,
            "",
            &format!("/api/v1/restores/{}/status", staged.id),
            vec![(
                actix_web::http::header::HeaderName::from_static("x-hubuum-restore-capability"),
                capability,
            )],
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let status_cache_control = header_value(&response, "Cache-Control");
        let status: RestoreStageResponse = test::read_body_json(response).await;
        assert_eq!(
            (
                status_cache_control.as_deref(),
                status.status,
                status.sha256,
                status.restore_capability,
            ),
            (
                Some("no-store"),
                RestoreJobStatus::Validated,
                staged.sha256,
                None,
            )
        );

        with_connection(&context.pool, async |conn| {
            diesel::delete(restore_jobs.filter(id.eq(staged.id)))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
    }

    #[rstest]
    #[case::existing_stage(true)]
    #[case::missing_stage(false)]
    #[actix_web::test]
    async fn invalid_restore_capability_does_not_disclose_stage_existence(
        #[future(awt)] test_context: TestContext,
        #[case] stage_exists: bool,
    ) {
        let context = test_context;
        let restore_id = if stage_exists {
            let response = post_request(
                &context.pool,
                &context.admin_token,
                "/api/v1/restores",
                &minimally_valid_full_backup_document(),
            )
            .await;
            let response = assert_response_status(response, StatusCode::CREATED).await;
            test::read_body_json::<RestoreStageResponse, _>(response)
                .await
                .id
        } else {
            i64::MAX
        };

        let response = get_request_with_headers(
            &context.pool,
            "",
            &format!("/api/v1/restores/{restore_id}/status"),
            vec![(
                actix_web::http::header::HeaderName::from_static("x-hubuum-restore-capability"),
                "invalid-restore-capability".to_string(),
            )],
        )
        .await;
        let response = assert_response_status(response, StatusCode::FORBIDDEN).await;
        let body = test::read_body_json::<serde_json::Value, _>(response).await;

        assert_eq!(
            body,
            serde_json::json!({
                "error": "Forbidden",
                "message": "Restore capability is invalid"
            })
        );

        if stage_exists {
            with_connection(&context.pool, async |conn| {
                diesel::delete(restore_jobs.filter(id.eq(restore_id)))
                    .execute(conn)
                    .await
            })
            .await
            .unwrap();
        }
    }

    #[rstest]
    #[case::existing_stage(true)]
    #[case::missing_stage(false)]
    #[actix_web::test]
    async fn api_restore_confirmation_is_disabled_for_privilege_separation(
        #[future(awt)] test_context: TestContext,
        #[case] stage_exists: bool,
    ) {
        let context = test_context;
        let staged = if stage_exists {
            let response = post_request(
                &context.pool,
                &context.admin_token,
                "/api/v1/restores",
                &minimally_valid_full_backup_document(),
            )
            .await;
            let response = assert_response_status(response, StatusCode::CREATED).await;
            Some(test::read_body_json::<RestoreStageResponse, _>(response).await)
        } else {
            None
        };
        let restore_id = staged.as_ref().map(|stage| stage.id).unwrap_or(i64::MAX);

        let response = post_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/restores/{restore_id}/confirm"),
            &RestoreConfirmRequest {
                restore_capability: "invalid-restore-capability".to_string(),
                sha256: "irrelevant-sha256".to_string(),
                confirmation: RESTORE_CONFIRMATION_PHRASE.to_string(),
            },
        )
        .await;
        let response = assert_response_status(response, StatusCode::NOT_IMPLEMENTED).await;
        let body = test::read_body_json::<serde_json::Value, _>(response).await;

        assert_eq!(
            body,
            serde_json::json!({
                "error": "Not Implemented",
                "message": "API-driven destructive restore is disabled; run hubuum-admin --restore with HUBUUM_MIGRATION_DATABASE_URL"
            })
        );

        if let Some(staged) = staged {
            with_connection(&context.pool, async |conn| {
                diesel::delete(restore_jobs.filter(id.eq(staged.id)))
                    .execute(conn)
                    .await
            })
            .await
            .unwrap();
        }
    }

    #[rstest]
    #[case::local_identity_scope(MissingRestoreSeed::LocalIdentityScope)]
    #[case::root_collection(MissingRestoreSeed::RootCollection)]
    #[case::root_closure(MissingRestoreSeed::RootClosure)]
    #[actix_web::test]
    async fn restore_rejects_a_full_snapshot_without_required_seed_rows(
        #[future(awt)] test_context: TestContext,
        #[case] missing: MissingRestoreSeed,
    ) {
        let context = test_context;
        let mut document = minimally_valid_full_backup_document();
        let section = match missing {
            MissingRestoreSeed::LocalIdentityScope => StorageBackupStateSection::IdentityScopes,
            MissingRestoreSeed::RootCollection => StorageBackupStateSection::Collections,
            MissingRestoreSeed::RootClosure => StorageBackupStateSection::CollectionHierarchy,
        };
        document.state.sections.get_mut(&section).unwrap().clear();

        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/restores",
            &document,
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[case::partial_scope("scope")]
    #[case::embedded_import("import_request")]
    #[actix_web::test]
    async fn restore_rejects_legacy_or_partial_backup_fields(
        #[future(awt)] test_context: TestContext,
        #[case] field: &str,
    ) {
        let context = test_context;
        let mut document = serde_json::to_value(minimally_valid_full_backup_document()).unwrap();
        document
            .as_object_mut()
            .unwrap()
            .insert(field.to_string(), serde_json::json!({}));

        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/restores",
            &document,
        )
        .await;

        assert_response_status(response, StatusCode::BAD_REQUEST).await;
    }

    #[rstest]
    #[case::normal_user(RejectedRestoreCaller::NormalUser)]
    #[case::scoped_administrator(RejectedRestoreCaller::ScopedAdministrator)]
    #[actix_web::test]
    async fn restore_staging_requires_an_unscoped_administrator(
        #[future(awt)] test_context: TestContext,
        #[case] caller: RejectedRestoreCaller,
    ) {
        let context = test_context;
        let token = match caller {
            RejectedRestoreCaller::NormalUser => context.normal_token.clone(),
            RejectedRestoreCaller::ScopedAdministrator => {
                scoped_token(
                    &context.pool,
                    context.admin_user.id,
                    &[Permissions::ReadCollection],
                )
                .await
            }
        };

        let response = post_request(
            &context.pool,
            &token,
            "/api/v1/restores",
            &minimally_valid_full_backup_document(),
        )
        .await;

        assert_response_status(response, StatusCode::FORBIDDEN).await;
    }

    #[rstest]
    #[actix_web::test]
    async fn disabled_api_confirmation_does_not_change_an_expired_stage(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let response = post_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/restores",
            &minimally_valid_full_backup_document(),
        )
        .await;
        let response = assert_response_status(response, StatusCode::CREATED).await;
        let staged: RestoreStageResponse = test::read_body_json(response).await;
        let capability = staged
            .restore_capability
            .clone()
            .expect("staging should return a restore capability");

        with_connection(&context.pool, async |conn| {
            use crate::schema::restore_jobs::dsl::{expires_at, id, restore_jobs};
            diesel::update(restore_jobs.filter(id.eq(staged.id)))
                .set(expires_at.eq(Utc::now().naive_utc() - Duration::minutes(1)))
                .execute(conn)
                .await
        })
        .await
        .unwrap();

        let response = post_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/restores/{}/confirm", staged.id),
            &RestoreConfirmRequest {
                restore_capability: capability,
                sha256: staged.sha256.clone(),
                confirmation: RESTORE_CONFIRMATION_PHRASE.to_string(),
            },
        )
        .await;
        assert_response_status(response, StatusCode::NOT_IMPLEMENTED).await;

        let (status, document) = with_connection(&context.pool, async |conn| {
            use crate::schema::restore_jobs::dsl::{document, id, restore_jobs, status};
            restore_jobs
                .filter(id.eq(staged.id))
                .select((status, document))
                .first::<(String, Vec<u8>)>(conn)
                .await
        })
        .await
        .unwrap();
        assert_eq!(
            (status.as_str(), document.is_empty()),
            (RestoreJobStatus::Validated.as_str(), false)
        );

        with_connection(&context.pool, async |conn| {
            diesel::delete(restore_jobs.filter(id.eq(staged.id)))
                .execute(conn)
                .await
        })
        .await
        .unwrap();
    }
}
