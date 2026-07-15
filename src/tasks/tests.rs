use chrono::NaiveDate;
use diesel::{ExpressionMethods, QueryDsl};
use diesel_async::RunQueryDsl;
use rstest::rstest;
use std::sync::Arc;

use super::execution::{execute_import_best_effort, execute_import_strict, execute_planned_item};
use super::helpers::{
    class_to_resolution, planned_result, sanitize_error_for_storage,
    should_abort_best_effort_execution,
};
use super::planning::{
    plan_class, plan_collection, plan_import, plan_object, plan_runtime_admin_import,
};
use super::request_hash;
use super::resolution::{
    remember_class, remember_collection, resolve_class_planning, resolve_collection_by_id_planning,
    resolve_collection_planning, resolve_object_planning, resolve_object_runtime,
};
use super::types::{
    CollectionResolution, ExecutionAccumulator, FailureKind, PlannedExecution, PlannedItem,
    PlanningFailure, PlanningState, RuntimeState, WorkerLoopAction,
};
use super::worker::{background_worker_action, mark_claimed_task_failed, process_one_task};
use crate::db::traits::task::{TaskBackend, insert_import_results};
use crate::db::traits::task_import::{
    create_class_db, create_object_db, upsert_export_template_db, upsert_group_membership_db,
    upsert_identity_scope_db,
};
use crate::db::{capture_queries, with_connection};
use crate::errors::ApiError;
use crate::models::{
    CURRENT_IMPORT_VERSION, ClassKey, CollectionID, CollectionKey, ExportContentType,
    ExportScopeKind, ExportTemplateKind, ImportAtomicity, ImportClassInput, ImportCollectionInput,
    ImportCollisionPolicy, ImportExportTemplateInput, ImportGraph, ImportGroupMembershipInput,
    ImportIdentityScopeInput, ImportMembershipSourceInput, ImportMode, ImportObjectInput,
    ImportPermissionPolicy, ImportRemoteTargetInput, ImportRequest, NewCollectionWithAssignee,
    NewHubuumClass, NewHubuumObject, NewImportTaskResultRecord, NewTaskRecord, ObjectKey,
    RemoteAuthConfig, RemoteHttpMethod, RemoteTargetSubjectType, RestoreTimestamps, TaskKind,
    TaskStatus,
};
use crate::permissions::test_support::MockTreetopBackend;
use crate::permissions::{AppContext, PermissionBackend};
use crate::schema::collections::dsl::{collections, name as collection_name};
use crate::schema::hubuumclass::dsl::{hubuumclass, name as class_name};
use crate::schema::tasks::dsl::{created_at, id as task_id, tasks};
use crate::tests::{TestContext, create_test_group};
use crate::traits::CanSave;

#[tokio::test]
async fn import_planning_query_growth_is_bounded_per_object_in_one_class() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("query_budget_import_preload")
        .await;
    let class = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: context.scoped_name("query_budget_import_class"),
        description: "query budget import class".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&context.pool)
    .await
    .expect("import class should save");
    let mut objects = Vec::new();
    for index in 0..20 {
        objects.push(
            NewHubuumObject {
                collection_id: fixture.collection.id,
                hubuum_class_id: class.id,
                name: context.scoped_name(&format!("query_budget_import_object_{index:02}")),
                description: "existing import object".to_string(),
                data: serde_json::json!({"index": index}),
            }
            .save_without_events(&context.pool)
            .await
            .expect("import object should save"),
        );
    }

    let request_for = |count: usize| ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: Some(false),
        mode: Some(ImportMode {
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            ..ImportMode::default()
        }),
        graph: ImportGraph {
            objects: objects
                .iter()
                .take(count)
                .enumerate()
                .map(|(index, object)| ImportObjectInput {
                    ref_: Some(format!("object:query-budget-{index}")),
                    name: object.name.clone(),
                    description: "planned import update".to_string(),
                    data: serde_json::json!({"index": index, "planned": true}),
                    class_ref: None,
                    class_key: Some(ClassKey {
                        name: class.name.clone(),
                        collection_ref: None,
                        collection_key: Some(CollectionKey {
                            name: fixture.collection.name.clone(),
                            path: None,
                        }),
                    }),
                })
                .collect(),
            ..ImportGraph::default()
        },
    };

    let small_request = request_for(1);
    let (small_plan, small_queries) = capture_queries(plan_runtime_admin_import(
        &context.pool,
        &context.admin_user,
        &small_request,
    ))
    .await;
    assert!(!small_plan.aborted);
    assert!(small_plan.failures.is_empty());
    assert_eq!(small_plan.planned_items.len(), 1);

    let large_request = request_for(20);
    let (large_plan, large_queries) = capture_queries(plan_runtime_admin_import(
        &context.pool,
        &context.admin_user,
        &large_request,
    ))
    .await;
    assert!(!large_plan.aborted);
    assert!(large_plan.failures.is_empty());
    assert_eq!(large_plan.planned_items.len(), 20);

    // Current before-refactor shape: three fixed batch queries (collection,
    // class, object), plus three collection-name resolutions per object across
    // class preload, object preload, and object planning. Keep that slope
    // explicit so a storage-boundary rewrite cannot make it worse unnoticed.
    let fixed_queries = 3;
    let queries_per_object = 3;
    assert_eq!(
        small_queries.total_queries(),
        fixed_queries + queries_per_object,
        "{:#?}",
        small_queries.query_counts()
    );
    assert_eq!(
        large_queries.total_queries(),
        fixed_queries + queries_per_object * 20,
        "{:#?}",
        large_queries.query_counts()
    );
    assert_eq!(
        small_queries.domain_queries(),
        small_queries.total_queries()
    );
    assert_eq!(
        large_queries.domain_queries(),
        large_queries.total_queries()
    );
    assert_eq!(small_queries.control_queries(), 0);
    assert_eq!(large_queries.control_queries(), 0);
    assert_eq!(
        small_queries.connection_checkouts(),
        small_queries.total_queries()
    );
    assert_eq!(
        large_queries.connection_checkouts(),
        large_queries.total_queries()
    );
    assert_eq!(large_queries.queries_matching("FROM \"hubuumclass\""), 1);
    assert_eq!(large_queries.queries_matching("FROM \"hubuumobject\""), 1);
    assert_eq!(
        large_queries.queries_matching("FROM \"collections\""),
        queries_per_object * 20 + 1
    );

    fixture.cleanup().await.expect("import fixture cleanup");
}

#[tokio::test]
async fn import_planning_uses_the_task_execution_permission_backend() {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("external_task_authorization")
        .await;
    let permissions: Arc<dyn PermissionBackend> = Arc::new(MockTreetopBackend::new());
    let backend = AppContext::new(context.pool.get_ref().clone(), permissions);
    let request = ImportRequest {
        version: CURRENT_IMPORT_VERSION,
        dry_run: Some(true),
        mode: Some(ImportMode {
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            ..ImportMode::default()
        }),
        graph: ImportGraph {
            collections: vec![ImportCollectionInput {
                ref_: Some("collection:existing".to_string()),
                name: fixture.collection.name.clone(),
                description: "updated by import".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
            }],
            ..ImportGraph::default()
        },
    };

    let planning = plan_import(&backend, &context.admin_user, None, &request).await;

    assert!(planning.aborted);
    assert_eq!(planning.failures.len(), 1);
    assert!(matches!(planning.failures[0].kind, FailureKind::Permission));
}

fn extended_import_request(name: String) -> ImportRequest {
    ImportRequest {
        version: crate::models::CURRENT_IMPORT_VERSION,
        dry_run: Some(false),
        mode: None,
        graph: ImportGraph {
            identity_scopes: vec![ImportIdentityScopeInput {
                ref_: Some("identity:backend-test".to_string()),
                name,
                provider_kind: "local".to_string(),
                timestamps: None,
            }],
            ..ImportGraph::default()
        },
    }
}

#[derive(Clone, Copy, Debug)]
enum ClassBoundImport {
    ExportTemplate,
    RemoteTarget,
}

#[derive(Clone, Copy, Debug)]
enum TemplateDependency {
    Existing,
    SameImport,
    Missing,
}

#[tokio::test]
async fn test_extended_import_uses_backend_denial_for_sql_administrator() {
    let test = TestContext::new().await;
    let context = AppContext::new(
        test.pool.get_ref().clone(),
        Arc::new(MockTreetopBackend::new()),
    );
    let request = extended_import_request(test.scoped_name("backend_denied_import"));

    let planning = plan_import(&context, &test.admin_user, None, &request).await;

    assert!(planning.aborted);
    assert!(matches!(
        planning.failures.as_slice(),
        [failure] if matches!(failure.kind, FailureKind::Permission)
    ));
}

#[tokio::test]
async fn test_extended_import_uses_backend_grant_for_non_sql_administrator() {
    let test = TestContext::new().await;
    let policy_group = create_test_group(&test.pool).await;
    policy_group
        .add_member_without_events(&test.pool, &test.normal_user)
        .await
        .unwrap();
    let backend = MockTreetopBackend::new();
    backend.add_admin_rule(policy_group.id);
    let context = AppContext::new(test.pool.get_ref().clone(), Arc::new(backend));
    let request = extended_import_request(test.scoped_name("backend_allowed_import"));

    let planning = plan_import(&context, &test.normal_user, None, &request).await;

    assert!(!planning.aborted);
    assert!(planning.failures.is_empty());
    assert_eq!(planning.planned_items.len(), 1);
}

#[tokio::test]
async fn test_identity_scope_overwrite_preserves_imported_timestamps() {
    let context = (TestContext::new()).await;
    let name = context.scoped_name("identity_scope_timestamp_overwrite");
    let initial = RestoreTimestamps {
        created_at: NaiveDate::from_ymd_opt(2020, 1, 2)
            .unwrap()
            .and_hms_opt(3, 4, 5)
            .unwrap(),
        updated_at: NaiveDate::from_ymd_opt(2020, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
    };
    let restored = RestoreTimestamps {
        created_at: NaiveDate::from_ymd_opt(2019, 4, 5)
            .unwrap()
            .and_hms_opt(6, 7, 8)
            .unwrap(),
        updated_at: NaiveDate::from_ymd_opt(2021, 6, 7)
            .unwrap()
            .and_hms_opt(8, 9, 10)
            .unwrap(),
    };

    let id = with_connection(&context.pool, async |conn| {
        upsert_identity_scope_db(
            conn,
            &ImportIdentityScopeInput {
                ref_: None,
                name: name.clone(),
                provider_kind: "local".to_string(),
                timestamps: Some(initial),
            },
            false,
        )
        .await?;
        upsert_identity_scope_db(
            conn,
            &ImportIdentityScopeInput {
                ref_: None,
                name: name.clone(),
                provider_kind: "oidc".to_string(),
                timestamps: Some(restored.clone()),
            },
            true,
        )
        .await
    })
    .await
    .unwrap();

    let row = with_connection(&context.pool, async |conn| {
        use crate::schema::identity_scopes::dsl::{id as scope_id, identity_scopes};
        identity_scopes
            .filter(scope_id.eq(id))
            .first::<crate::models::IdentityScope>(conn)
            .await
    })
    .await
    .unwrap();
    assert_eq!(row.created_at, restored.created_at);
    assert_eq!(row.updated_at, restored.updated_at);

    with_connection(&context.pool, async |conn| {
        use crate::schema::identity_scopes::dsl::{id as scope_id, identity_scopes};
        diesel::delete(identity_scopes.filter(scope_id.eq(id)))
            .execute(conn)
            .await
    })
    .await
    .unwrap();
}

#[rstest]
#[case::export_template(ClassBoundImport::ExportTemplate)]
#[case::remote_target(ClassBoundImport::RemoteTarget)]
#[tokio::test]
async fn imported_class_binding_must_match_target_collection(#[case] kind: ClassBoundImport) {
    let context = TestContext::new().await;
    let target = context
        .collection_fixture("import_class_scope_target")
        .await;
    let class_owner = context.collection_fixture("import_class_scope_owner").await;
    let class = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("import_class_scope_class"),
                description: "Class in another collection".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: class_owner.collection.name.clone(),
                    path: None,
                }),
            },
            class_owner.collection.id,
        )
        .await
    })
    .await
    .unwrap();
    let collection_key = Some(CollectionKey {
        name: target.collection.name.clone(),
        path: None,
    });
    let class_key = Some(ClassKey {
        name: class.name,
        collection_ref: None,
        collection_key: Some(CollectionKey {
            name: class_owner.collection.name.clone(),
            path: None,
        }),
    });
    let execution = match kind {
        ClassBoundImport::ExportTemplate => PlannedExecution::UpsertExportTemplate {
            input: ImportExportTemplateInput {
                ref_: None,
                collection_ref: None,
                collection_key,
                class_ref: None,
                class_key,
                name: context.scoped_name("cross_collection_export_template"),
                description: "Invalid class binding".to_string(),
                content_type: ExportContentType::TextPlain,
                template: "{{ items|length }}".to_string(),
                kind: ExportTemplateKind::Export,
                scope_kind: Some(ExportScopeKind::ObjectsInClass),
                default_query: None,
                include: None,
                relation_context: None,
                default_missing_data_policy: None,
                default_limits: None,
                timestamps: None,
            },
            overwrite: false,
        },
        ClassBoundImport::RemoteTarget => PlannedExecution::UpsertRemoteTarget {
            input: ImportRemoteTargetInput {
                ref_: None,
                collection_ref: None,
                collection_key,
                class_ref: None,
                class_key,
                name: context.scoped_name("cross_collection_remote_target"),
                description: "Invalid class binding".to_string(),
                method: RemoteHttpMethod::Get,
                url_template: "https://example.test/{{ subject.id }}".to_string(),
                headers_template: serde_json::json!({}),
                body_template: None,
                auth_config: RemoteAuthConfig::None,
                allowed_subject_types: vec![RemoteTargetSubjectType::Object],
                timeout_ms: 1_000,
                enabled: true,
                timestamps: None,
            },
            overwrite: false,
        },
    };

    let result = with_connection(&context.pool, async |conn| {
        execute_planned_item(conn, &mut RuntimeState::default(), &execution).await
    })
    .await;

    assert!(matches!(
        result,
        Err(ApiError::BadRequest(message)) if message.contains("not target collection")
    ));
}

#[rstest]
#[case::existing(TemplateDependency::Existing, true)]
#[case::same_import(TemplateDependency::SameImport, true)]
#[case::missing(TemplateDependency::Missing, false)]
#[tokio::test]
async fn imported_templates_use_effective_collection_loader(
    #[case] dependency: TemplateDependency,
    #[case] expected_valid: bool,
) {
    let context = TestContext::new().await;
    let fixture = context
        .collection_fixture("import_template_composition")
        .await;
    let fragment_name = context.scoped_name("fragment.txt");
    let fragment = ImportExportTemplateInput {
        ref_: Some("template:fragment".to_string()),
        collection_ref: None,
        collection_key: Some(CollectionKey {
            name: fixture.collection.name.clone(),
            path: None,
        }),
        class_ref: None,
        class_key: None,
        name: fragment_name.clone(),
        description: "Reusable fragment".to_string(),
        content_type: ExportContentType::TextPlain,
        template: "fragment".to_string(),
        kind: ExportTemplateKind::Fragment,
        scope_kind: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
        timestamps: None,
    };
    let export = ImportExportTemplateInput {
        ref_: Some("template:export".to_string()),
        name: context.scoped_name("composed_export.txt"),
        description: "Composed export".to_string(),
        template: format!("{{% include \"{fragment_name}\" %}}"),
        kind: ExportTemplateKind::Export,
        scope_kind: Some(ExportScopeKind::Collections),
        ..fragment.clone()
    };

    let result = with_connection(&context.pool, async |conn| {
        if matches!(dependency, TemplateDependency::Existing) {
            upsert_export_template_db(conn, &fragment, fixture.collection.id, None, false).await?;
        }
        let import_export_templates = match dependency {
            TemplateDependency::SameImport => vec![export.clone(), fragment],
            TemplateDependency::Existing | TemplateDependency::Missing => vec![export.clone()],
        };
        let mut runtime = RuntimeState {
            import_export_templates,
            ..RuntimeState::default()
        };
        execute_planned_item(
            conn,
            &mut runtime,
            &PlannedExecution::UpsertExportTemplate {
                input: export,
                overwrite: false,
            },
        )
        .await
    })
    .await;

    assert_eq!(result.is_ok(), expected_valid);
}

#[rstest]
#[case::abort(false, "conflict")]
#[case::overwrite(true, "updated")]
#[tokio::test]
async fn membership_import_honors_collision_policy(
    #[case] overwrite: bool,
    #[case] expected_outcome: &str,
) {
    let context = TestContext::new().await;
    let group = create_test_group(&context.pool).await;
    let initial = RestoreTimestamps {
        created_at: NaiveDate::from_ymd_opt(2020, 1, 2)
            .unwrap()
            .and_hms_opt(3, 4, 5)
            .unwrap(),
        updated_at: NaiveDate::from_ymd_opt(2020, 2, 3)
            .unwrap()
            .and_hms_opt(4, 5, 6)
            .unwrap(),
    };
    let restored = RestoreTimestamps {
        created_at: NaiveDate::from_ymd_opt(2019, 4, 5)
            .unwrap()
            .and_hms_opt(6, 7, 8)
            .unwrap(),
        updated_at: NaiveDate::from_ymd_opt(2021, 6, 7)
            .unwrap()
            .and_hms_opt(8, 9, 10)
            .unwrap(),
    };
    let membership = |timestamps: RestoreTimestamps| ImportGroupMembershipInput {
        ref_: None,
        principal_ref: None,
        principal_key: None,
        group_ref: None,
        group_key: None,
        sources: vec![ImportMembershipSourceInput {
            source: "oidc".to_string(),
            source_scope_ref: None,
            source_scope_key: None,
            source_key: "operators".to_string(),
            timestamps: Some(timestamps.clone()),
        }],
        timestamps: Some(timestamps),
    };

    let result = with_connection(&context.pool, async |conn| {
        upsert_group_membership_db(
            conn,
            &membership(initial.clone()),
            context.admin_user.id,
            group.id,
            &[group.identity_scope_id],
            false,
        )
        .await?;
        let collision = upsert_group_membership_db(
            conn,
            &membership(restored.clone()),
            context.admin_user.id,
            group.id,
            &[group.identity_scope_id],
            overwrite,
        )
        .await;
        use crate::schema::group_membership_sources::dsl as s;
        use crate::schema::group_memberships::dsl as m;
        let stored_membership = m::group_memberships
            .filter(m::principal_id.eq(context.admin_user.id))
            .filter(m::group_id.eq(group.id))
            .select((m::created_at, m::updated_at))
            .first::<(chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
            .await?;
        let stored_source = s::group_membership_sources
            .filter(s::principal_id.eq(context.admin_user.id))
            .filter(s::group_id.eq(group.id))
            .filter(s::source.eq("oidc"))
            .filter(s::source_scope_id.eq(group.identity_scope_id))
            .filter(s::source_key.eq("operators"))
            .select((s::created_at, s::updated_at))
            .first::<(chrono::NaiveDateTime, chrono::NaiveDateTime)>(conn)
            .await?;
        Ok::<_, ApiError>((collision, stored_membership, stored_source))
    })
    .await
    .unwrap();
    let actual_outcome = match result.0 {
        Ok(()) => "updated",
        Err(ApiError::Conflict(_)) => "conflict",
        Err(error) => panic!("unexpected membership collision result: {error}"),
    };
    let expected_timestamps = if overwrite { restored } else { initial };

    assert_eq!(
        (actual_outcome, result.1, result.2),
        (
            expected_outcome,
            (
                expected_timestamps.created_at,
                expected_timestamps.updated_at
            ),
            (
                expected_timestamps.created_at,
                expected_timestamps.updated_at
            )
        )
    );
}

#[tokio::test]
async fn test_execute_import_strict_rolls_back_on_runtime_failure() {
    let context = (TestContext::new()).await;
    let collection = context.scoped_name("strict_rollback_collection");
    let class = context.scoped_name("strict_rollback_class");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:ok".to_string()),
                Some(collection.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:ok".to_string()),
                name: collection.clone(),
                description: "Rollback collection".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "class",
                "create",
                Some("class:bad".to_string()),
                Some(class.clone()),
            ),
            execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                ref_: Some("class:bad".to_string()),
                name: class.clone(),
                description: "Fails at runtime".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: Some("collection:missing".to_string()),
                collection_key: None,
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    let result = (execute_import_strict(&context.pool, 1, &planned_items, &mut accumulator)).await;
    assert!(result.is_err());

    let collection_exists = with_connection(&context.pool, async |conn| {
        collections
            .filter(collection_name.eq(&collection))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();
    let class_exists = with_connection(&context.pool, async |conn| {
        hubuumclass
            .filter(class_name.eq(&class))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();

    assert_eq!(collection_exists, 0);
    assert_eq!(class_exists, 0);
    assert_eq!(accumulator.processed, 0);
}

#[tokio::test]
async fn test_execute_import_best_effort_keeps_successful_items() {
    let context = (TestContext::new()).await;
    let collection_one = context.scoped_name("best_effort_collection_one");
    let collection_two = context.scoped_name("best_effort_collection_two");
    let class_bad = context.scoped_name("best_effort_class_bad");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:one".to_string()),
                Some(collection_one.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:one".to_string()),
                name: collection_one.clone(),
                description: "Best effort collection one".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "class",
                "create",
                Some("class:bad".to_string()),
                Some(class_bad),
            ),
            execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                ref_: Some("class:bad".to_string()),
                name: "bad".to_string(),
                description: "Fails at runtime".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: Some("collection:missing".to_string()),
                collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:two".to_string()),
                Some(collection_two.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:two".to_string()),
                name: collection_two.clone(),
                description: "Best effort collection two".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    (execute_import_best_effort(
        &context.pool,
        1,
        &planned_items,
        &ImportMode {
            atomicity: Some(ImportAtomicity::BestEffort),
            collision_policy: Some(ImportCollisionPolicy::Overwrite),
            permission_policy: Some(ImportPermissionPolicy::Continue),
        },
        &mut accumulator,
    ))
    .await
    .unwrap();

    let collection_count = with_connection(&context.pool, async |conn| {
        collections
            .filter(collection_name.eq_any([collection_one.clone(), collection_two.clone()]))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();

    assert_eq!(collection_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[tokio::test]
async fn test_execute_import_best_effort_continues_after_non_policy_runtime_error() {
    let context = (TestContext::new()).await;
    let collection_one = context.scoped_name("best_effort_runtime_collection_one");
    let collection_two = context.scoped_name("best_effort_runtime_collection_two");
    let planned_items = vec![
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:one".to_string()),
                Some(collection_one.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:one".to_string()),
                name: collection_one.clone(),
                description: "Best effort collection one".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "class",
                "create",
                Some("class:bad".to_string()),
                Some("bad".to_string()),
            ),
            execution: Some(PlannedExecution::CreateClass(ImportClassInput {
                ref_: Some("class:bad".to_string()),
                name: "bad".to_string(),
                description: "Fails at runtime".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: Some("collection:missing".to_string()),
                collection_key: None,
            })),
        },
        PlannedItem {
            result: planned_result(
                "collection",
                "create",
                Some("collection:two".to_string()),
                Some(collection_two.clone()),
            ),
            execution: Some(PlannedExecution::CreateCollection(ImportCollectionInput {
                ref_: Some("collection:two".to_string()),
                name: collection_two.clone(),
                description: "Best effort collection two".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
            })),
        },
    ];

    let mut accumulator = ExecutionAccumulator::default();
    (execute_import_best_effort(
        &context.pool,
        1,
        &planned_items,
        &ImportMode {
            atomicity: Some(ImportAtomicity::BestEffort),
            collision_policy: Some(ImportCollisionPolicy::Abort),
            permission_policy: Some(ImportPermissionPolicy::Abort),
        },
        &mut accumulator,
    ))
    .await
    .unwrap();

    let collection_count = with_connection(&context.pool, async |conn| {
        collections
            .filter(collection_name.eq_any([collection_one.clone(), collection_two.clone()]))
            .count()
            .get_result::<i64>(conn)
            .await
    })
    .await
    .unwrap();

    assert_eq!(collection_count, 2);
    assert_eq!(accumulator.processed, 3);
    assert_eq!(accumulator.success, 2);
    assert_eq!(accumulator.failed, 1);
}

#[tokio::test]
async fn test_execute_import_strict_preserves_underlying_error_variant() {
    let context = (TestContext::new()).await;
    let planned_items = vec![PlannedItem {
        result: planned_result(
            "collection",
            "update",
            Some("collection:missing".to_string()),
            Some("missing".to_string()),
        ),
        execution: Some(PlannedExecution::UpdateCollection {
            collection_id: -999,
            input: ImportCollectionInput {
                ref_: Some("collection:missing".to_string()),
                name: "missing".to_string(),
                description: "missing".to_string(),
                parent_collection_ref: None,
                parent_collection_key: None,
            },
        }),
    }];

    let mut accumulator = ExecutionAccumulator::default();
    let result = (execute_import_strict(&context.pool, 1, &planned_items, &mut accumulator)).await;

    assert!(matches!(result, Err(ApiError::NotFound(_))));
}

#[tokio::test]
async fn test_process_one_task_marks_claimed_task_failed_when_execution_setup_errors() {
    let context = (TestContext::new()).await;
    let task = (NewTaskRecord {
        kind: TaskKind::Import.as_str().to_string(),
        status: TaskStatus::Queued.as_str().to_string(),
        submitted_by: Some(context.admin_user.id),
        submitted_token_id: None,
        submitted_token_scoped: false,
        submitted_token_scopes: serde_json::json!([]),
        idempotency_key: Some(context.scoped_name("missing-payload-task")),
        request_hash: None,
        request_payload: None,
        summary: None,
        total_items: 1,
        processed_items: 0,
        success_items: 0,
        failed_items: 0,
        request_redacted_at: None,
        started_at: None,
        finished_at: None,
    }
    .create(&context.pool))
    .await
    .unwrap();

    let earliest = NaiveDate::from_ymd_opt(2000, 1, 1)
        .expect("valid date")
        .and_hms_opt(0, 0, 0)
        .expect("valid timestamp");
    with_connection(&context.pool, async |conn| {
        diesel::update(tasks.filter(task_id.eq(task.id)))
            .set(created_at.eq(earliest))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    for _ in 0..20 {
        let _ = (process_one_task(&context.pool, None)).await.unwrap();

        let stored = (task.find_record(&context.pool)).await.unwrap();
        if stored.status == TaskStatus::Failed.as_str() {
            assert!(stored.finished_at.is_some());
            assert!(stored.request_redacted_at.is_some());

            let (events, _) = (task.list_events_with_total_count(
                &context.pool,
                &crate::models::search::QueryOptions {
                    filters: Vec::new(),
                    sort: Vec::new(),
                    limit: None,
                    cursor: None,
                    include_total: true,
                },
            ))
            .await
            .unwrap();
            let event_types = events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>();
            assert!(event_types.contains(&"validating"));
            assert!(event_types.contains(&"failed"));
            return;
        }
    }

    let stored = (task.find_record(&context.pool)).await.unwrap();
    panic!(
        "Task {} did not reach failed state after repeated processing attempts; current status: {}",
        task.id, stored.status
    );
}

#[test]
fn test_background_worker_continues_immediately_after_processing_a_task() {
    let result = Ok(true);
    assert_eq!(
        background_worker_action(&result),
        WorkerLoopAction::Continue
    );
}

#[test]
fn test_remember_collection_populates_collection_id_index() {
    let mut state = PlanningState::new();
    let collection = CollectionResolution {
        id: -42,
        name: "planned".to_string(),
        description: "planned collection".to_string(),
        parent_collection_id: None,
        exists_in_db: false,
    };

    remember_collection(
        &mut state,
        Some("collection:planned".to_string()),
        collection.clone(),
    );

    assert_eq!(
        state.collections_by_id.get(&collection.id).unwrap().name,
        collection.name
    );
}

#[tokio::test]
async fn test_plan_collection_rejects_duplicate_name_within_request() {
    let context = (TestContext::new()).await;
    let mut state = PlanningState::new();
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportCollectionInput {
        ref_: Some("collection:one".to_string()),
        name: context.scoped_name("duplicate_collection"),
        description: "first".to_string(),
        parent_collection_ref: None,
        parent_collection_key: None,
    };

    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportCollectionInput {
        ref_: Some("collection:two".to_string()),
        ..input
    };
    let err = (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate collection name"));
}

#[tokio::test]
async fn test_plan_collection_allows_duplicate_names_under_different_parents() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("duplicate_import_parent_one")).await;
    let parent_two = (context.collection_fixture("duplicate_import_parent_two")).await;
    let mut state = PlanningState::new();
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let child_name = context.scoped_name("duplicate_import_child");
    let input_one = ImportCollectionInput {
        ref_: Some("collection:one".to_string()),
        name: child_name.clone(),
        description: "first".to_string(),
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent_one.collection.name.clone(),
            path: None,
        }),
    };
    let input_two = ImportCollectionInput {
        ref_: Some("collection:two".to_string()),
        name: child_name,
        description: "second".to_string(),
        parent_collection_ref: None,
        parent_collection_key: Some(CollectionKey {
            name: parent_two.collection.name.clone(),
            path: None,
        }),
    };

    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_one,
    ))
    .await
    .unwrap();
    (plan_collection(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input_two,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn test_plan_class_rejects_duplicate_name_against_virtual_planned_class() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_virtual_class")).await;
    let mut state = PlanningState::new();
    remember_collection(
        &mut state,
        Some("collection:existing".to_string()),
        CollectionResolution {
            id: fixture.collection.id,
            name: fixture.collection.name.clone(),
            description: fixture.collection.description.clone(),
            parent_collection_id: fixture.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportClassInput {
        ref_: Some("class:one".to_string()),
        name: context.scoped_name("duplicate_class"),
        description: "first".to_string(),
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportClassInput {
        ref_: Some("class:two".to_string()),
        ..input
    };
    let err = (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class name"));
}

#[tokio::test]
async fn test_plan_object_rejects_duplicate_name_against_virtual_planned_object() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_virtual_object")).await;
    let class = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_virtual_object_class"),
                description: "existing class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();
    let mut state = PlanningState::new();
    remember_class(
        &mut state,
        Some("class:existing".to_string()),
        class_to_resolution(class.clone()),
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportObjectInput {
        ref_: Some("object:one".to_string()),
        name: context.scoped_name("duplicate_object"),
        description: "first".to_string(),
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportObjectInput {
        ref_: Some("object:two".to_string()),
        ..input
    };
    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object name"));
}

#[tokio::test]
async fn test_plan_class_rejects_duplicate_ref_against_virtual_planned_class() {
    let context = (TestContext::new()).await;
    let fixture_one = (context.collection_fixture("duplicate_class_ref_one")).await;
    let fixture_two = (context.collection_fixture("duplicate_class_ref_two")).await;
    let mut state = PlanningState::new();
    remember_collection(
        &mut state,
        Some("collection:one".to_string()),
        CollectionResolution {
            id: fixture_one.collection.id,
            name: fixture_one.collection.name.clone(),
            description: fixture_one.collection.description.clone(),
            parent_collection_id: fixture_one.collection.parent_collection_id,
            exists_in_db: true,
        },
    );
    remember_collection(
        &mut state,
        Some("collection:two".to_string()),
        CollectionResolution {
            id: fixture_two.collection.id,
            name: fixture_two.collection.name.clone(),
            description: fixture_two.collection.description.clone(),
            parent_collection_id: fixture_two.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportClassInput {
        ref_: Some("class:shared".to_string()),
        name: context.scoped_name("duplicate_class_ref_one"),
        description: "first".to_string(),
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:one".to_string()),
        collection_key: None,
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportClassInput {
        name: context.scoped_name("duplicate_class_ref_two"),
        collection_ref: Some("collection:two".to_string()),
        ..input
    };
    let err = (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate class ref"));
}

#[tokio::test]
async fn test_plan_object_rejects_duplicate_ref_against_virtual_planned_object() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("duplicate_object_ref")).await;
    let class_one = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_object_ref_class_one"),
                description: "first class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();
    let class_two = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: context.scoped_name("duplicate_object_ref_class_two"),
                description: "second class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();
    let mut state = PlanningState::new();
    remember_class(
        &mut state,
        Some("class:one".to_string()),
        class_to_resolution(class_one.clone()),
    );
    remember_class(
        &mut state,
        Some("class:two".to_string()),
        class_to_resolution(class_two.clone()),
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };
    let input = ImportObjectInput {
        ref_: Some("object:shared".to_string()),
        name: context.scoped_name("duplicate_object_ref_one"),
        description: "first".to_string(),
        data: serde_json::json!({"hostname":"first"}),
        class_ref: Some("class:one".to_string()),
        class_key: None,
    };

    (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &input,
    ))
    .await
    .unwrap();

    let duplicate = ImportObjectInput {
        name: context.scoped_name("duplicate_object_ref_two"),
        class_ref: Some("class:two".to_string()),
        ..input
    };
    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &duplicate,
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
    assert!(err.message.contains("Duplicate object ref"));
}

#[tokio::test]
async fn test_resolve_collection_planning_backfills_caches_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_collection_cache")).await;
    let mut state = PlanningState::new();

    let resolved = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: fixture.collection.name.clone(),
            path: None,
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, fixture.collection.id);
    assert_eq!(
        state
            .collections_by_name
            .get(&fixture.collection.name)
            .unwrap()
            .iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>(),
        vec![fixture.collection.id]
    );
    assert_eq!(
        state
            .collections_by_id
            .get(&fixture.collection.id)
            .unwrap()
            .name,
        fixture.collection.name
    );
}

#[tokio::test]
async fn test_resolve_collection_planning_rejects_ambiguous_bare_name() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("ambiguous_parent_one")).await;
    let parent_two = (context.collection_fixture("ambiguous_parent_two")).await;
    let child_name = context.scoped_name("ambiguous_child");

    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "first ambiguous child".to_string(),
        group_id: parent_one.owner_group.id,
        parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();
    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "second ambiguous child".to_string(),
        group_id: parent_two.owner_group.id,
        parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();

    let mut state = PlanningState::new();
    let err = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: None,
        }),
    ))
    .await
    .unwrap_err();

    assert!(err.contains("ambiguous"));
    assert!(err.contains("collection_key.path"));
}

#[tokio::test]
async fn test_resolve_collection_planning_uses_path_to_disambiguate_name() {
    let context = (TestContext::new()).await;
    let parent_one = (context.collection_fixture("path_parent_one")).await;
    let parent_two = (context.collection_fixture("path_parent_two")).await;
    let child_name = context.scoped_name("path_child");

    ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "first path child".to_string(),
        group_id: parent_one.owner_group.id,
        parent_collection_id: Some(CollectionID::new(parent_one.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();
    let target_child = ((NewCollectionWithAssignee {
        name: child_name.clone(),
        description: "second path child".to_string(),
        group_id: parent_two.owner_group.id,
        parent_collection_id: Some(CollectionID::new(parent_two.collection.id).unwrap()),
    })
    .save_without_events(&context.pool))
    .await
    .unwrap();

    let mut state = PlanningState::new();
    let resolved = (resolve_collection_planning(
        &context.pool,
        &mut state,
        None,
        Some(&CollectionKey {
            name: child_name.clone(),
            path: Some(vec![parent_two.collection.name.clone(), child_name]),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, target_child.id);
}

#[tokio::test]
async fn test_resolve_collection_by_id_planning_backfills_caches_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_collection_id_cache")).await;
    let mut state = PlanningState::new();

    let resolved =
        (resolve_collection_by_id_planning(&context.pool, &mut state, fixture.collection.id))
            .await
            .unwrap();

    assert_eq!(resolved.name, fixture.collection.name);
    assert_eq!(
        state
            .collections_by_name
            .get(&fixture.collection.name)
            .unwrap()
            .iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>(),
        vec![fixture.collection.id]
    );
    assert_eq!(
        state
            .collections_by_id
            .get(&fixture.collection.id)
            .unwrap()
            .name,
        fixture.collection.name
    );
}

#[tokio::test]
async fn test_resolve_class_planning_backfills_cache_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_class_cache")).await;
    let class_name_value = context.scoped_name("planning_class_cache_value");
    let class = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "cached class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();
    let mut state = PlanningState::new();

    let resolved = (resolve_class_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ClassKey {
            name: class.name.clone(),
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, class.id);
    assert_eq!(
        state
            .classes_by_key
            .get(&(fixture.collection.id, class.name.clone()))
            .unwrap()
            .id,
        class.id
    );
}

#[tokio::test]
async fn test_resolve_object_planning_backfills_cache_after_db_lookup() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("planning_object_cache")).await;
    let class_name_value = context.scoped_name("planning_object_cache_class");
    let class = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "cached class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();
    let object_name_value = context.scoped_name("planning_object_cache_value");
    let object = with_connection(&context.pool, async |conn| {
        create_object_db(
            conn,
            &ImportObjectInput {
                ref_: None,
                name: object_name_value.clone(),
                description: "cached object".to_string(),
                data: serde_json::json!({"hostname":"cached"}),
                class_ref: None,
                class_key: Some(ClassKey {
                    name: class.name.clone(),
                    collection_ref: None,
                    collection_key: Some(CollectionKey {
                        name: fixture.collection.name.clone(),
                        path: None,
                    }),
                }),
            },
            &class,
        )
        .await
    })
    .await
    .unwrap();
    let mut state = PlanningState::new();

    let resolved = (resolve_object_planning(
        &context.pool,
        &mut state,
        None,
        Some(&ObjectKey {
            name: object.name.clone(),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            }),
        }),
    ))
    .await
    .unwrap();

    assert_eq!(resolved.id, object.id);
    assert_eq!(
        state
            .objects_by_key
            .get(&(class.id, object.name.clone()))
            .unwrap()
            .id,
        object.id
    );
}

#[tokio::test]
async fn test_update_collection_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_collection_ref")).await;
    let updated_description = context.scoped_name("updated_collection_description");
    let execution = PlannedExecution::UpdateCollection {
        collection_id: fixture.collection.id,
        input: ImportCollectionInput {
            ref_: Some("collection:existing".to_string()),
            name: fixture.collection.name.clone(),
            description: updated_description.clone(),
            parent_collection_ref: None,
            parent_collection_key: None,
        },
    };

    let class_input = ImportClassInput {
        ref_: Some("class:child".to_string()),
        name: context.scoped_name("class_after_collection_update"),
        description: "child".to_string(),
        json_schema: None,
        validate_schema: Some(false),
        collection_ref: Some("collection:existing".to_string()),
        collection_key: None,
    };

    let result = with_connection(&context.pool, async |conn| {
        let mut runtime = RuntimeState::default();
        execute_planned_item(conn, &mut runtime, &execution).await?;
        execute_planned_item(
            conn,
            &mut runtime,
            &PlannedExecution::CreateClass(class_input.clone()),
        )
        .await?;
        Ok::<_, ApiError>(
            runtime
                .collections_by_ref
                .get("collection:existing")
                .cloned(),
        )
    })
    .await
    .unwrap();

    let collection = result.expect("collection ref should be available after update");
    assert_eq!(collection.id, fixture.collection.id);
    assert_eq!(collection.description, updated_description);
}

#[tokio::test]
async fn test_update_class_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_class_ref")).await;
    let class_name_value = context.scoped_name("existing_class_for_update");
    let class = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "existing class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();

    let execution = PlannedExecution::UpdateClass {
        class_id: class.id,
        input: ImportClassInput {
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated class".to_string(),
            json_schema: None,
            validate_schema: Some(false),
            collection_ref: None,
            collection_key: Some(CollectionKey {
                name: fixture.collection.name.clone(),
                path: None,
            }),
        },
    };

    let object_input = ImportObjectInput {
        ref_: Some("object:child".to_string()),
        name: context.scoped_name("object_after_class_update"),
        description: "child".to_string(),
        data: serde_json::json!({"hostname":"child"}),
        class_ref: Some("class:existing".to_string()),
        class_key: None,
    };

    let result = with_connection(&context.pool, async |conn| {
        let mut runtime = RuntimeState::default();
        execute_planned_item(conn, &mut runtime, &execution).await?;
        execute_planned_item(
            conn,
            &mut runtime,
            &PlannedExecution::CreateObject(object_input.clone()),
        )
        .await?;
        Ok::<_, ApiError>(runtime.classes_by_ref.get("class:existing").cloned())
    })
    .await
    .unwrap();

    let updated = result.expect("class ref should be available after update");
    assert_eq!(updated.id, class.id);
    assert_eq!(updated.name, class.name);
}

#[tokio::test]
async fn test_plan_class_update_preserves_existing_schema_for_following_objects() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_class_schema_ref")).await;
    let schema = serde_json::json!({
        "type": "object",
        "required": ["hostname"],
        "properties": {
            "hostname": {"type": "string"}
        }
    });
    let class_name_value = context.scoped_name("existing_class_with_schema");
    let class = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "existing class".to_string(),
                json_schema: Some(schema.clone()),
                validate_schema: Some(true),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();

    let mut state = PlanningState::new();
    remember_collection(
        &mut state,
        Some("collection:existing".to_string()),
        CollectionResolution {
            id: fixture.collection.id,
            name: fixture.collection.name.clone(),
            description: fixture.collection.description.clone(),
            parent_collection_id: fixture.collection.parent_collection_id,
            exists_in_db: true,
        },
    );

    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Overwrite),
        permission_policy: Some(ImportPermissionPolicy::Continue),
    };

    (plan_class(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportClassInput {
            ref_: Some("class:existing".to_string()),
            name: class.name.clone(),
            description: "updated description".to_string(),
            json_schema: None,
            validate_schema: None,
            collection_ref: Some("collection:existing".to_string()),
            collection_key: None,
        },
    ))
    .await
    .unwrap();

    let err = (plan_object(
        &context.pool,
        &context.admin_user,
        &mode,
        &mut state,
        &ImportObjectInput {
            ref_: Some("object:invalid".to_string()),
            name: context.scoped_name("invalid_object_after_class_update"),
            description: "invalid".to_string(),
            data: serde_json::json!({"hostname": 42}),
            class_ref: Some("class:existing".to_string()),
            class_key: None,
        },
    ))
    .await
    .unwrap_err();

    assert!(matches!(err.kind, FailureKind::Validation));
}

#[tokio::test]
async fn test_update_object_refreshes_runtime_ref_for_following_items() {
    let context = (TestContext::new()).await;
    let fixture = (context.collection_fixture("update_object_ref")).await;
    let class_name_value = context.scoped_name("existing_class_for_object_update");
    let class = with_connection(&context.pool, async |conn| {
        create_class_db(
            conn,
            &ImportClassInput {
                ref_: None,
                name: class_name_value.clone(),
                description: "existing class".to_string(),
                json_schema: None,
                validate_schema: Some(false),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            },
            fixture.collection.id,
        )
        .await
    })
    .await
    .unwrap();

    let object_name_value = context.scoped_name("existing_object_for_update");
    let object = with_connection(&context.pool, async |conn| {
        create_object_db(
            conn,
            &ImportObjectInput {
                ref_: None,
                name: object_name_value.clone(),
                description: "existing object".to_string(),
                data: serde_json::json!({"hostname":"existing"}),
                class_ref: None,
                class_key: Some(ClassKey {
                    name: class.name.clone(),
                    collection_ref: None,
                    collection_key: Some(CollectionKey {
                        name: fixture.collection.name.clone(),
                        path: None,
                    }),
                }),
            },
            &class,
        )
        .await
    })
    .await
    .unwrap();

    let execution = PlannedExecution::UpdateObject {
        object_id: object.id,
        input: ImportObjectInput {
            ref_: Some("object:existing".to_string()),
            name: object.name.clone(),
            description: "updated object".to_string(),
            data: serde_json::json!({"hostname":"updated"}),
            class_ref: None,
            class_key: Some(ClassKey {
                name: class.name.clone(),
                collection_ref: None,
                collection_key: Some(CollectionKey {
                    name: fixture.collection.name.clone(),
                    path: None,
                }),
            }),
        },
    };

    let resolved = with_connection(&context.pool, async |conn| {
        let mut runtime = RuntimeState::default();
        execute_planned_item(conn, &mut runtime, &execution).await?;
        resolve_object_runtime(conn, &runtime, Some("object:existing"), None::<&ObjectKey>).await
    })
    .await
    .unwrap();

    assert_eq!(resolved.id, object.id);
    assert_eq!(resolved.description, "updated object");
}

#[test]
fn test_request_hash_is_stable_for_reordered_json_objects() {
    let first = serde_json::json!({
        "version": 1,
        "dry_run": true,
        "graph": {
            "objects": [{
                "ref": "object:one",
                "name": "server-1",
                "description": "server",
                "data": {"a": 1, "b": {"x": 1, "y": 2}},
                "class_ref": "class:one"
            }]
        }
    });
    let second = serde_json::json!({
        "graph": {
            "objects": [{
                "class_ref": "class:one",
                "description": "server",
                "name": "server-1",
                "ref": "object:one",
                "data": {"b": {"y": 2, "x": 1}, "a": 1}
            }]
        },
        "dry_run": true,
        "version": 1
    });

    assert_eq!(
        request_hash(&first).unwrap(),
        request_hash(&second).unwrap()
    );
}

#[test]
fn test_sanitize_error_for_storage_masks_database_details() {
    let sanitized = sanitize_error_for_storage(&ApiError::DatabaseError(
        "relation users does not exist".to_string(),
    ));
    assert_eq!(sanitized, "Database operation failed");
}

#[test]
fn test_runtime_planning_failures_are_sanitized_for_storage() {
    let failure = PlanningFailure {
        kind: FailureKind::Runtime,
        item: planned_result(
            "collection",
            "lookup",
            Some("collection:one".to_string()),
            None,
        ),
        message: "relation users does not exist".to_string(),
    };

    assert_eq!(failure.message_for_storage(), "An internal error occurred");

    let stored = failure.into_result(1);
    assert_eq!(stored.error.as_deref(), Some("An internal error occurred"));
}

#[test]
fn test_best_effort_execution_only_aborts_for_matching_policy_failures() {
    let mode = ImportMode {
        atomicity: Some(ImportAtomicity::BestEffort),
        collision_policy: Some(ImportCollisionPolicy::Abort),
        permission_policy: Some(ImportPermissionPolicy::Abort),
    };

    assert!(should_abort_best_effort_execution(
        &ApiError::Conflict("collision".to_string()),
        &mode,
    ));
    assert!(should_abort_best_effort_execution(
        &ApiError::Forbidden("permission".to_string()),
        &mode,
    ));
    assert!(!should_abort_best_effort_execution(
        &ApiError::NotFound("missing runtime ref".to_string()),
        &mode,
    ));
    assert!(!should_abort_best_effort_execution(
        &ApiError::DatabaseError("db error".to_string()),
        &mode,
    ));
}

#[tokio::test]
async fn test_process_one_task_export_failure_marks_single_failed_item() {
    let context = (TestContext::new()).await;
    let task = (NewTaskRecord {
        kind: TaskKind::Export.as_str().to_string(),
        status: TaskStatus::Queued.as_str().to_string(),
        submitted_by: Some(context.admin_user.id),
        submitted_token_id: None,
        submitted_token_scoped: false,
        submitted_token_scopes: serde_json::json!([]),
        idempotency_key: Some(context.scoped_name("unimplemented-export-task")),
        request_hash: None,
        request_payload: Some(serde_json::json!({"export": "demo"})),
        summary: None,
        total_items: 0,
        processed_items: 0,
        success_items: 0,
        failed_items: 0,
        request_redacted_at: None,
        started_at: None,
        finished_at: None,
    }
    .create(&context.pool))
    .await
    .unwrap();

    let earliest = NaiveDate::from_ymd_opt(2000, 1, 1)
        .expect("valid date")
        .and_hms_opt(0, 0, 0)
        .expect("valid timestamp");
    with_connection(&context.pool, async |conn| {
        diesel::update(tasks.filter(task_id.eq(task.id)))
            .set(created_at.eq(earliest))
            .execute(conn)
            .await
    })
    .await
    .unwrap();

    for _ in 0..20 {
        let _ = (process_one_task(&context.pool, None)).await.unwrap();
        let stored = (task.find_record(&context.pool)).await.unwrap();
        if stored.status == TaskStatus::Failed.as_str() {
            assert_eq!(stored.total_items, 0);
            assert_eq!(stored.processed_items, 1);
            assert_eq!(stored.failed_items, 1);
            return;
        }
    }

    let stored = (task.find_record(&context.pool)).await.unwrap();
    panic!(
        "Task {} did not reach failed state after repeated processing attempts; current status: {}",
        task.id, stored.status
    );
}

#[tokio::test]
async fn test_mark_claimed_task_failed_uses_recorded_result_counts() {
    let context = (TestContext::new()).await;
    let task = (NewTaskRecord {
        kind: TaskKind::Import.as_str().to_string(),
        status: TaskStatus::Queued.as_str().to_string(),
        submitted_by: Some(context.admin_user.id),
        submitted_token_id: None,
        submitted_token_scoped: false,
        submitted_token_scopes: serde_json::json!([]),
        idempotency_key: Some(context.scoped_name("fallback-count-task")),
        request_hash: None,
        request_payload: Some(serde_json::json!({"version": 1})),
        summary: None,
        total_items: 3,
        processed_items: 0,
        success_items: 0,
        failed_items: 0,
        request_redacted_at: None,
        started_at: None,
        finished_at: None,
    }
    .create(&context.pool))
    .await
    .unwrap();

    (insert_import_results(
        &context.pool,
        &[
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("a".to_string()),
                entity_kind: "collection".to_string(),
                action: "create".to_string(),
                identifier: Some("a".to_string()),
                outcome: "succeeded".to_string(),
                error: None,
                details: None,
            },
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("b".to_string()),
                entity_kind: "class".to_string(),
                action: "create".to_string(),
                identifier: Some("b".to_string()),
                outcome: "failed".to_string(),
                error: Some("failed".to_string()),
                details: None,
            },
        ],
    ))
    .await
    .unwrap();

    (mark_claimed_task_failed(
        &context.pool,
        &task,
        &ApiError::InternalServerError("boom".to_string()),
    ))
    .await
    .unwrap();

    let stored = (task.find_record(&context.pool)).await.unwrap();
    assert_eq!(stored.processed_items, 2);
    assert_eq!(stored.success_items, 1);
    assert_eq!(stored.failed_items, 1);
}

#[tokio::test]
async fn test_count_import_results_summary_counts_success_and_failure_rows() {
    let context = (TestContext::new()).await;
    let task = (NewTaskRecord {
        kind: TaskKind::Import.as_str().to_string(),
        status: TaskStatus::Queued.as_str().to_string(),
        submitted_by: Some(context.admin_user.id),
        submitted_token_id: None,
        submitted_token_scoped: false,
        submitted_token_scopes: serde_json::json!([]),
        idempotency_key: Some(context.scoped_name("aggregate-count-task")),
        request_hash: None,
        request_payload: Some(serde_json::json!({"version": 1})),
        summary: None,
        total_items: 4,
        processed_items: 0,
        success_items: 0,
        failed_items: 0,
        request_redacted_at: None,
        started_at: None,
        finished_at: None,
    }
    .create(&context.pool))
    .await
    .unwrap();

    (insert_import_results(
        &context.pool,
        &[
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("one".to_string()),
                entity_kind: "collection".to_string(),
                action: "create".to_string(),
                identifier: Some("one".to_string()),
                outcome: "succeeded".to_string(),
                error: None,
                details: None,
            },
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("two".to_string()),
                entity_kind: "class".to_string(),
                action: "create".to_string(),
                identifier: Some("two".to_string()),
                outcome: "failed".to_string(),
                error: Some("failed".to_string()),
                details: None,
            },
            NewImportTaskResultRecord {
                task_id: task.id,
                item_ref: Some("three".to_string()),
                entity_kind: "object".to_string(),
                action: "update".to_string(),
                identifier: Some("three".to_string()),
                outcome: "planned".to_string(),
                error: None,
                details: None,
            },
        ],
    ))
    .await
    .unwrap();

    let counts = (task.count_import_results(&context.pool)).await.unwrap();

    assert_eq!(counts.processed, 3);
    assert_eq!(counts.success, 2);
    assert_eq!(counts.failed, 1);
}
