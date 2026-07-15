#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use actix_web::{http::StatusCode, test};
    use chrono::{Duration, Utc};
    use diesel::{ExpressionMethods, QueryDsl};
    use diesel_async::RunQueryDsl;
    use rstest::rstest;

    use crate::db::with_connection;
    use crate::models::{
        BackupDocument, BackupManifest, BackupState, Permissions, RESTORE_CONFIRMATION_PHRASE,
        RestoreConfirmRequest, RestoreJobStatus, RestoreStageResponse,
    };
    use crate::schema::restore_jobs::dsl::{id, restore_jobs};
    use crate::tests::api_operations::{get_request_with_headers, post_request};
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{TestContext, scoped_token, test_context};

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
        let sections = [
            "identity_scopes",
            "groups",
            "principals",
            "users",
            "service_accounts",
            "group_memberships",
            "group_membership_sources",
            "collections",
            "collection_closure",
            "hubuumclass",
            "hubuumclass_relation",
            "hubuumobject",
            "hubuumobject_relation",
            "permissions",
            "export_templates",
            "remote_targets",
            "event_sinks",
            "event_subscriptions",
        ]
        .into_iter()
        .map(|name| (name.to_string(), Vec::new()))
        .collect::<BTreeMap<_, _>>();

        BackupDocument {
            backup_version: crate::models::CURRENT_BACKUP_VERSION,
            created_at: Utc::now().naive_utc(),
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
            .get_mut("identity_scopes")
            .unwrap()
            .push(serde_json::json!({
                "id": 1,
                "name": "local",
                "provider_kind": "local"
            }));
        document
            .state
            .sections
            .get_mut("collections")
            .unwrap()
            .push(serde_json::json!({
                "id": 1,
                "name": "root",
                "parent_collection_id": null
            }));
        document
            .state
            .sections
            .get_mut("collection_closure")
            .unwrap()
            .push(serde_json::json!({
                "ancestor_collection_id": 1,
                "descendant_collection_id": 1,
                "depth": 0
            }));
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
            MissingRestoreSeed::LocalIdentityScope => "identity_scopes",
            MissingRestoreSeed::RootCollection => "collections",
            MissingRestoreSeed::RootClosure => "collection_closure",
        };
        document.state.sections.get_mut(section).unwrap().clear();

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
    async fn expired_confirmation_expires_the_validated_stage(
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
        assert_response_status(response, StatusCode::GONE).await;

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
            (status.as_str(), document.as_slice()),
            (RestoreJobStatus::Expired.as_str(), b"".as_slice())
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
