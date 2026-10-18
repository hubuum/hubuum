use actix_web::{http::StatusCode, test};
use rstest::rstest;
use serde_json::{Value, json};

use crate::models::{HubuumClass, HubuumObject, NewHubuumClass, NewHubuumObject};
use crate::tests::api_operations::{delete_request, get_request, patch_request};
use crate::tests::asserts::{assert_response_status, header_value};
use crate::tests::{CollectionFixture, TestContext};
use crate::traits::CanSave;
use hubuum_storage_postgres::{PostgresStorageError, diesel_async_prelude::*, with_transaction};

struct Fixture {
    context: TestContext,
    collection: CollectionFixture,
    class: HubuumClass,
    object: HubuumObject,
}

impl Fixture {
    async fn new() -> Self {
        let context = TestContext::new().await;
        let collection = context.collection_fixture("history_navigation").await;
        collection
            .owner_group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let class = NewHubuumClass {
            collection_id: collection.collection.id,
            name: context.scoped_name("history_class"),
            description: "initial".into(),
            json_schema: Some(json!({"description": "x".repeat(100_000)})),
            validate_schema: Some(false),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        let object = NewHubuumObject {
            collection_id: collection.collection.id,
            hubuum_class_id: class.id,
            name: context.scoped_name("history_object"),
            description: "initial".into(),
            data: json!({"blob": "x".repeat(100_000)}),
        }
        .save_without_events(&context.pool)
        .await
        .unwrap();
        Self {
            context,
            collection,
            class,
            object,
        }
    }

    fn resource_path(&self, object: bool) -> String {
        if object {
            format!("/api/v1/classes/{}/{}", self.class.id, self.object.id)
        } else {
            format!("/api/v1/classes/{}", self.class.id)
        }
    }

    async fn change(&self, object: bool, description: &str) {
        let response = patch_request(
            &self.context.pool,
            &self.context.admin_token,
            &self.resource_path(object),
            json!({"description":description}),
        )
        .await;
        assert_response_status(response, StatusCode::OK).await;
    }

    async fn timeline(&self, object: bool, query: &str) -> Vec<Value> {
        let response = get_request(
            &self.context.pool,
            &self.context.admin_token,
            &format!("{}/history/timeline?{query}", self.resource_path(object)),
        )
        .await;
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await
    }

    async fn cleanup(self) {
        self.collection.cleanup().await.unwrap();
    }
}

#[rstest]
#[actix_web::test]
async fn timeline_omits_snapshot_payloads(#[values(false, true)] object: bool) {
    let fixture = Fixture::new().await;
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.normal_token,
        &format!("{}/history/timeline", fixture.resource_path(object)),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, "cache-control").as_deref(),
        Some("private, no-store")
    );
    assert!(header_value(&response, "x-total-count").is_none());
    let rows: Vec<Value> = test::read_body_json(response).await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get("data").is_none());
    assert!(rows[0].get("json_schema").is_none());
    assert!(rows[0].get("provenance").is_some());
    assert!(serde_json::to_vec(&rows).unwrap().len() < 2_000);
    fixture.cleanup().await;
}

#[rstest]
#[case::counted(true)]
#[case::uncounted(false)]
#[actix_web::test]
async fn timeline_queries_avoid_snapshots_and_optional_counts(
    #[values(false, true)] object: bool,
    #[case] include_total: bool,
) {
    use hubuum_storage_postgres::capture_queries;
    let fixture = Fixture::new().await;
    let (response, queries) = capture_queries(get_request(
        &fixture.context.pool,
        &fixture.context.normal_token,
        &format!(
            "{}/history/timeline?include_total={include_total}",
            fixture.resource_path(object)
        ),
    ))
    .await;
    assert_response_status(response, StatusCode::OK).await;
    let table = if object {
        "hubuumobject_history"
    } else {
        "hubuumclass_history"
    };
    assert_eq!(
        queries.queries_matching(table),
        1 + usize::from(include_total)
    );
    for sql in queries.query_counts().keys() {
        assert!(
            !sql.contains("\"data\"") && !sql.contains("\"json_schema\""),
            "{sql}"
        );
    }
    fixture.cleanup().await;
}

#[rstest]
#[case::ordinary_user(false)]
#[case::scoped_administrator(true)]
#[actix_web::test]
async fn deleted_history_requires_an_unscoped_administrator(
    #[values(false, true)] object: bool,
    #[values(false, true)] entry: bool,
    #[case] scoped_administrator: bool,
) {
    use crate::models::Permissions;
    use crate::tests::scoped_token;
    let fixture = Fixture::new().await;
    let rows = fixture.timeline(object, "").await;
    let token = if scoped_administrator {
        scoped_token(
            &fixture.context.pool,
            fixture.context.admin_user.id,
            &[Permissions::ReadClass, Permissions::ReadObject],
        )
        .await
    } else {
        fixture.context.normal_token.clone()
    };
    let response = delete_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &fixture.resource_path(object),
    )
    .await;
    assert_response_status(response, StatusCode::NO_CONTENT).await;
    let suffix = if entry {
        format!("entries/{}", rows[0]["history_id"])
    } else {
        "timeline?include_total=true".to_string()
    };
    let response = get_request(
        &fixture.context.pool,
        &token,
        &format!("{}/history/{suffix}", fixture.resource_path(object)),
    )
    .await;
    assert_response_status(response, StatusCode::NOT_FOUND).await;
    fixture.cleanup().await;
}

#[rstest]
#[actix_web::test]
async fn exact_history_entry_restores_full_snapshot(#[values(false, true)] object: bool) {
    let fixture = Fixture::new().await;
    fixture.change(object, "later").await;
    let rows = fixture.timeline(object, "direction=newer").await;
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.normal_token,
        &format!(
            "{}/history/entries/{}",
            fixture.resource_path(object),
            rows[0]["history_id"]
        ),
    )
    .await;
    let row: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(row["history_id"], rows[0]["history_id"]);
    assert_eq!(row["description"], "initial");
    assert!(
        row.get(if object { "data" } else { "json_schema" })
            .is_some()
    );
    fixture.cleanup().await;
}

#[rstest]
#[case::older("older", 0)]
#[case::newer("newer", 2)]
#[actix_web::test]
async fn anchor_seeks_adjacent_entries(
    #[values(false, true)] object: bool,
    #[case] direction: &str,
    #[case] expected: usize,
) {
    let fixture = Fixture::new().await;
    fixture.change(object, "middle").await;
    fixture.change(object, "latest").await;
    let rows = fixture.timeline(object, "direction=newer").await;
    let page = fixture
        .timeline(
            object,
            &format!(
                "anchor={}&direction={direction}&limit=1",
                rows[1]["history_id"]
            ),
        )
        .await;
    assert_eq!(page[0]["history_id"], rows[expected]["history_id"]);
    fixture.cleanup().await;
}

#[rstest]
#[case::older("older", 1)]
#[case::newer("newer", 2)]
#[actix_web::test]
async fn timestamp_seek_preserves_exact_boundary(
    #[values(false, true)] object: bool,
    #[case] direction: &str,
    #[case] expected: usize,
) {
    let fixture = Fixture::new().await;
    fixture.change(object, "middle").await;
    fixture.change(object, "latest").await;
    let rows = fixture.timeline(object, "direction=newer").await;
    let at = rows[1]["valid_from"].as_str().unwrap().replace('+', "%2B");
    let page = fixture
        .timeline(object, &format!("at={at}&direction={direction}&limit=1"))
        .await;
    assert_eq!(page[0]["history_id"], rows[expected]["history_id"]);
    fixture.cleanup().await;
}

#[rstest]
#[actix_web::test]
async fn timeline_window_is_half_open(#[values(false, true)] object: bool) {
    let fixture = Fixture::new().await;
    fixture.change(object, "middle").await;
    fixture.change(object, "latest").await;
    let rows = fixture.timeline(object, "direction=newer").await;
    let from = rows[1]["valid_from"].as_str().unwrap().replace('+', "%2B");
    let until = rows[2]["valid_from"].as_str().unwrap().replace('+', "%2B");
    let page = fixture
        .timeline(object, &format!("from={from}&until={until}"))
        .await;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["history_id"], rows[1]["history_id"]);
    fixture.cleanup().await;
}

#[rstest]
#[case::inclusive_start("from", 0)]
#[case::exclusive_end("until", 1)]
#[actix_web::test]
async fn timeline_window_preserves_submicrosecond_boundaries(
    #[values(false, true)] object: bool,
    #[case] parameter: &str,
    #[case] expected: usize,
) {
    use chrono::{DateTime, Duration, Utc};
    let fixture = Fixture::new().await;
    let rows = fixture.timeline(object, "").await;
    let at: DateTime<Utc> = rows[0]["valid_from"].as_str().unwrap().parse().unwrap();
    let bound = (at + Duration::nanoseconds(1))
        .to_rfc3339()
        .replace('+', "%2B");
    let page = fixture
        .timeline(object, &format!("{parameter}={bound}"))
        .await;
    assert_eq!(page.len(), expected);
    fixture.cleanup().await;
}

#[rstest]
#[actix_web::test]
async fn timeline_cursor_survives_new_versions(#[values(false, true)] object: bool) {
    let fixture = Fixture::new().await;
    fixture.change(object, "middle").await;
    fixture.change(object, "latest").await;
    let expected = fixture.timeline(object, "").await;
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &format!("{}/history/timeline?limit=1", fixture.resource_path(object)),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let cursor = header_value(&response, "x-next-cursor").unwrap();
    fixture.change(object, "concurrent change").await;
    let page = fixture.timeline(object, &format!("cursor={cursor}")).await;
    assert_eq!(
        page.iter()
            .map(|row| &row["history_id"])
            .collect::<Vec<_>>(),
        expected[1..]
            .iter()
            .map(|row| &row["history_id"])
            .collect::<Vec<_>>()
    );
    fixture.cleanup().await;
}

#[rstest]
#[case::entry("entries")]
#[case::anchor("timeline?anchor=")]
#[actix_web::test]
async fn history_identity_is_resource_scoped(
    #[values(false, true)] object: bool,
    #[case] endpoint: &str,
) {
    let fixture = Fixture::new().await;
    let other = Fixture::new().await;
    let rows = other.timeline(object, "").await;
    let suffix = if endpoint == "entries" {
        format!("entries/{}", rows[0]["history_id"])
    } else {
        format!("{endpoint}{}", rows[0]["history_id"])
    };
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &format!("{}/history/{suffix}", fixture.resource_path(object)),
    )
    .await;
    assert_response_status(response, StatusCode::NOT_FOUND).await;
    fixture.cleanup().await;
    other.cleanup().await;
}

#[rstest]
#[case::entry("entries/9223372036854775807")]
#[case::anchor("timeline?anchor=9223372036854775807")]
#[actix_web::test]
async fn missing_history_identity_is_unavailable(
    #[values(false, true)] object: bool,
    #[case] suffix: &str,
) {
    let fixture = Fixture::new().await;
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &format!("{}/history/{suffix}", fixture.resource_path(object)),
    )
    .await;
    assert_response_status(response, StatusCode::NOT_FOUND).await;
    fixture.cleanup().await;
}

#[rstest]
#[actix_web::test]
async fn known_resource_has_an_empty_available_window(#[values(false, true)] object: bool) {
    let fixture = Fixture::new().await;
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.normal_token,
        &format!(
            "{}/history/timeline?until=1970-01-01T00:00:00Z&include_total=true",
            fixture.resource_path(object)
        ),
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, "x-total-count").as_deref(),
        Some("0")
    );
    assert!(header_value(&response, "x-next-cursor").is_none());
    let rows: Vec<Value> = test::read_body_json(response).await;
    assert!(rows.is_empty());
    fixture.cleanup().await;
}

#[rstest]
#[actix_web::test]
async fn deleted_entry_is_directly_addressable(#[values(false, true)] object: bool) {
    let fixture = Fixture::new().await;
    let response = delete_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &fixture.resource_path(object),
    )
    .await;
    assert_response_status(response, StatusCode::NO_CONTENT).await;
    let rows = fixture.timeline(object, "").await;
    assert_eq!(rows[0]["op"], "D");
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &format!(
            "{}/history/entries/{}",
            fixture.resource_path(object),
            rows[0]["history_id"]
        ),
    )
    .await;
    let entry: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(entry["op"], "D");
    assert_eq!(entry["revision"], rows[1]["revision"]);
    fixture.cleanup().await;
}

#[rstest]
#[case("at=invalid")]
#[case("anchor=0")]
#[case("anchor=1&at=2026-01-01T00:00:00Z")]
#[case("from=2026-01-02T00:00:00Z&until=2026-01-01T00:00:00Z")]
#[case("direction=sideways")]
#[case("limit=0")]
#[case("cursor=invalid")]
#[case("sort=name")]
#[actix_web::test]
async fn timeline_rejects_invalid_queries(#[case] query: &str) {
    let fixture = Fixture::new().await;
    let response = get_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &format!("{}/history/timeline?{query}", fixture.resource_path(false)),
    )
    .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    fixture.cleanup().await;
}

#[rstest]
#[case::older("older")]
#[case::newer("newer")]
#[actix_web::test]
async fn timestamp_ties_have_stable_pages(
    #[values(false, true)] object: bool,
    #[case] direction: &str,
) {
    use chrono::{DateTime, Utc};
    use diesel::sql_types::{Integer, Text, Timestamptz};
    use hubuum_storage_postgres::test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool,
    };
    let fixture = Fixture::new().await;
    fixture.change(object, "middle").await;
    fixture.change(object, "latest").await;
    let at: DateTime<Utc> = "2026-01-01T12:34:56.123456Z".parse().unwrap();
    let migration_pool = integration_test_migration_pool(1);
    with_transaction(&migration_pool, async |connection| {
        // Historical rows are immutable to the runtime role. Only this isolated
        // fixture uses the migrator to manufacture identical timestamps.
        if database_role_tests_enabled() {
            let roles = integration_test_database_roles();
            diesel::sql_query("SELECT set_config('role', $1, true)")
                .bind::<Text, _>(roles.owner().as_str())
                .execute(connection)
                .await?;
        }
        let (table, id) = if object {
            ("hubuumobject_history", fixture.object.id)
        } else {
            ("hubuumclass_history", fixture.class.id)
        };
        diesel::sql_query(format!(
            "UPDATE {table} SET valid_from = $1, \
             valid_to = CASE WHEN valid_to IS NULL THEN NULL ELSE $1 END WHERE id = $2"
        ))
        .bind::<Timestamptz, _>(at)
        .bind::<Integer, _>(id)
        .execute(connection)
        .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    let expected = fixture
        .timeline(object, &format!("direction={direction}"))
        .await;
    let mut cursor = String::new();
    let mut ids = Vec::new();
    loop {
        let query = format!("direction={direction}&limit=1{cursor}");
        let response = get_request(
            &fixture.context.pool,
            &fixture.context.admin_token,
            &format!("{}/history/timeline?{query}", fixture.resource_path(object)),
        )
        .await;
        let response = assert_response_status(response, StatusCode::OK).await;
        let next = header_value(&response, "x-next-cursor");
        let rows: Vec<Value> = test::read_body_json(response).await;
        ids.extend(rows.into_iter().map(|row| row["history_id"].clone()));
        if let Some(next) = next {
            cursor = format!("&cursor={next}");
        } else {
            break;
        }
        assert!(ids.len() < 4, "cursor must advance over ties");
    }
    assert_eq!(
        ids,
        expected
            .iter()
            .map(|row| row["history_id"].clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(expected[0]["valid_from"], "2026-01-01T12:34:56.123456Z");
    fixture.cleanup().await;
}

#[rstest]
#[case::timeline("timeline?include_total=true&limit=1")]
#[case::entry("entry")]
#[case::anchor("anchor")]
#[actix_web::test]
async fn history_navigation_hides_inaccessible_historical_collections(
    #[values(false, true)] object: bool,
    #[case] endpoint: &str,
) {
    use hubuum_storage_postgres::capture_queries;
    let fixture = Fixture::new().await;
    let destination = fixture
        .context
        .collection_fixture("history_visible_destination")
        .await;
    destination
        .owner_group
        .add_member_without_events(&fixture.context.pool, &fixture.context.normal_user)
        .await
        .unwrap();
    fixture
        .collection
        .owner_group
        .remove_member_without_events(&fixture.context.normal_user, &fixture.context.pool)
        .await
        .unwrap();
    let previous = fixture.timeline(object, "").await;
    let response = patch_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &fixture.resource_path(object),
        json!({"collection_id":destination.collection.id}),
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;
    let suffix = match endpoint {
        "entry" => format!("entries/{}", previous[0]["history_id"]),
        "anchor" => format!("timeline?anchor={}", previous[0]["history_id"]),
        _ => endpoint.to_string(),
    };
    let (response, queries) = capture_queries(get_request(
        &fixture.context.pool,
        &fixture.context.normal_token,
        &format!("{}/history/{suffix}", fixture.resource_path(object)),
    ))
    .await;
    for sql in queries.query_counts().keys() {
        assert!(
            !sql.contains("\"data\"") && !sql.contains("\"json_schema\""),
            "unauthorized snapshots must not be loaded: {sql}"
        );
    }
    if endpoint.starts_with("timeline") {
        let response = assert_response_status(response, StatusCode::OK).await;
        assert_eq!(
            header_value(&response, "x-total-count").as_deref(),
            Some("1")
        );
        assert!(header_value(&response, "x-next-cursor").is_none());
        let rows: Vec<Value> = test::read_body_json(response).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["collection_id"], destination.collection.id);
    } else {
        assert_response_status(response, StatusCode::NOT_FOUND).await;
    }
    fixture.cleanup().await;
    destination.cleanup().await.unwrap();
}

#[rstest]
#[case::counted(true)]
#[case::uncounted(false)]
#[actix_web::test]
async fn timeline_external_policy_filters_before_paging(
    #[values(false, true)] object: bool,
    #[case] include_total: bool,
) {
    use crate::models::Permissions;
    use crate::permissions::test_support::mock_treetop::{MockAllowRule, MockTreetopBackend};
    use crate::permissions::{ResourceFields, ResourceKind};
    use crate::tests::api_operations::get_request_with_permission_backend;
    use std::sync::Arc;
    let fixture = Fixture::new().await;
    let visible_name = fixture.context.scoped_name("visible_version");
    let response = patch_request(
        &fixture.context.pool,
        &fixture.context.admin_token,
        &fixture.resource_path(object),
        json!({"name":visible_name}),
    )
    .await;
    assert_response_status(response, StatusCode::OK).await;
    let policy = Arc::new(MockTreetopBackend::new());
    policy.add_rule(MockAllowRule {
        group_id: fixture.collection.owner_group.id,
        action: if object {
            Permissions::ReadObject
        } else {
            Permissions::ReadClass
        },
        resource_kind: if object {
            ResourceKind::Object
        } else {
            ResourceKind::Class
        },
        resource_id: Some(if object {
            fixture.object.id
        } else {
            fixture.class.id
        }),
        attrs: ResourceFields {
            name: Some(visible_name.clone()),
            collection_id: Some(fixture.collection.collection.id),
            ..Default::default()
        },
    });
    let response = get_request_with_permission_backend(
        &fixture.context.pool,
        &fixture.context.normal_token,
        &format!(
            "{}/history/timeline?direction=newer&limit=1&include_total={include_total}",
            fixture.resource_path(object)
        ),
        policy,
    )
    .await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        header_value(&response, "x-total-count"),
        include_total.then(|| "1".to_string())
    );
    assert!(header_value(&response, "x-next-cursor").is_none());
    let rows: Vec<Value> = test::read_body_json(response).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["name"], visible_name);
    fixture.cleanup().await;
}
