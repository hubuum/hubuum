//! Exact catalog totals should not repeat an already exhausted first-page scan.

use hubuum_query::{CursorValue, QueryContinuation, encode_cursor_values};
use hubuum_storage_postgres::capture_queries;
use rstest::rstest;

use crate::models::search::parse_query_parameter;
use crate::models::{
    HubuumObject, HubuumObjectID, NewHubuumClass, NewHubuumObject, TokenResourceScope, TokenScope,
};
use crate::pagination::prepare_db_pagination;
use crate::services::catalog;
use crate::tests::{ObjectFixture, TestScope};

async fn fixture(scope: &TestScope) -> ObjectFixture {
    scope
        .object_fixture(
            "catalog_total",
            NewHubuumClass {
                collection_id: 0,
                name: scope.scoped_name("catalog_total_class"),
                description: "exact catalog total fixture".to_string(),
                json_schema: None,
                validate_schema: None,
            },
            (0..4)
                .map(|index| NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: scope.scoped_name(&format!("catalog_total_object_{index}")),
                    description: "exact catalog total fixture".to_string(),
                    data: serde_json::json!({"facts": {"operating_system": {
                        "major_version": "9",
                        "version": "9.8",
                        "distribution": if index < 3 { "RedHat" } else { "Ubuntu" },
                    }}}),
                })
                .collect(),
        )
        .await
        .expect("catalog fixture should save")
}

fn object_query(fixture: &ObjectFixture, limit: usize, distribution: &str) -> String {
    format!(
        "classes={}&sort=id&limit={limit}\
         &json_data__icontains=facts,operating_system,major_version=9\
         &json_data__icontains=facts,operating_system,version=9.8\
         &json_data__icontains=facts,operating_system,distribution={distribution}",
        fixture.class.id
    )
}

#[rstest]
#[case::empty(10, "Missing", 0, 0)]
#[case::short(10, "RedHat", 3, 0)]
#[case::exact_page(3, "RedHat", 3, 0)]
#[case::lookahead(2, "RedHat", 3, 1)]
#[case::multiple_pages(1, "RedHat", 3, 1)]
#[actix_web::test]
async fn object_first_page_counts_only_when_needed(
    #[case] limit: usize,
    #[case] distribution: &str,
    #[case] expected_total: i64,
    #[case] expected_count_queries: usize,
    #[values(true, false)] include_total: bool,
) {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let mut options = parse_query_parameter(&object_query(&fixture, limit, distribution)).unwrap();
    options.set_include_total(include_total);
    let options = prepare_db_pagination::<HubuumObject>(&options).unwrap();

    let (result, queries) =
        capture_queries(catalog::list_objects(&scope.pool, 1, true, None, options)).await;
    let (rows, total) = result.expect("filtered object page should load");

    assert_eq!(rows.len(), (expected_total as usize).min(limit + 1));
    assert_eq!(total, include_total.then_some(expected_total));
    assert_eq!(
        queries.queries_matching("SELECT COUNT("),
        if include_total {
            expected_count_queries
        } else {
            0
        },
        "{:#?}",
        queries.query_counts()
    );
    assert_eq!(queries.connection_checkouts(), 1);
    fixture.cleanup().await.expect("catalog fixture cleanup");
}

#[rstest]
#[case::last_page(1, 1)]
#[case::empty_page(2, 0)]
#[actix_web::test]
async fn object_cursor_page_keeps_the_total_for_all_matches(
    #[case] after_object: usize,
    #[case] expected_rows: usize,
    #[values(true, false)] internal_continuation: bool,
) {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let mut options = parse_query_parameter(&object_query(&fixture, 10, "RedHat")).unwrap();
    let values = vec![CursorValue::Integer(i64::from(
        fixture.objects[after_object].id,
    ))];
    if internal_continuation {
        options.set_continuation(QueryContinuation::new(options.sort(), values).unwrap());
    } else {
        options
            .set_cursor(Some(encode_cursor_values(options.sort(), values).unwrap()))
            .unwrap();
    }
    let options = prepare_db_pagination::<HubuumObject>(&options).unwrap();

    let (result, queries) =
        capture_queries(catalog::list_objects(&scope.pool, 1, true, None, options)).await;
    let (rows, total) = result.expect("cursor page should load");

    assert_eq!(rows.len(), expected_rows);
    assert_eq!(total, Some(3));
    assert_eq!(queries.queries_matching("SELECT COUNT("), 1);
    fixture.cleanup().await.expect("catalog fixture cleanup");
}

#[actix_web::test]
async fn inferred_object_total_respects_token_resource_visibility() {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let token_scope = TokenScope::from_stored_parts(
        None,
        Some(vec![TokenResourceScope::Object(
            HubuumObjectID::new(fixture.objects[0].id).unwrap(),
        )]),
    )
    .unwrap();
    let options = parse_query_parameter(&object_query(&fixture, 10, "RedHat")).unwrap();
    let options = prepare_db_pagination::<HubuumObject>(&options).unwrap();

    let (result, queries) = capture_queries(catalog::list_objects(
        &scope.pool,
        1,
        true,
        Some(&token_scope),
        options,
    ))
    .await;
    let (rows, total) = result.expect("resource-scoped page should load");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, fixture.objects[0].id);
    assert_eq!(total, Some(1));
    assert_eq!(queries.queries_matching("SELECT COUNT("), 0);
    fixture.cleanup().await.expect("catalog fixture cleanup");
}

enum CatalogKind {
    Collection,
    Class,
}

#[rstest]
#[case::collection(CatalogKind::Collection)]
#[case::class(CatalogKind::Class)]
#[actix_web::test]
async fn other_catalog_first_pages_infer_exact_totals(
    #[case] kind: CatalogKind,
    #[values(Some(2), None)] limit: Option<usize>,
) {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let id = match kind {
        CatalogKind::Collection => fixture.collection_id(),
        CatalogKind::Class => fixture.class.id,
    };
    let mut options = parse_query_parameter(&format!("id={id}")).unwrap();
    options.set_limit(limit).unwrap();

    let ((rows, total), queries) = capture_queries(async {
        match kind {
            CatalogKind::Collection => {
                let (rows, total) = catalog::list_collections(&scope.pool, 1, true, None, options)
                    .await
                    .expect("collection page should load");
                (rows.len(), total)
            }
            CatalogKind::Class => {
                let (rows, total) = catalog::list_classes(&scope.pool, 1, true, None, options)
                    .await
                    .expect("class page should load");
                (rows.len(), total)
            }
        }
    })
    .await;

    assert_eq!(rows, 1);
    assert_eq!(total, Some(1));
    assert_eq!(queries.queries_matching("SELECT COUNT("), 0);
    fixture.cleanup().await.expect("catalog fixture cleanup");
}
