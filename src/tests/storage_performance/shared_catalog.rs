//! Shared filtering must preserve rows, global totals, and visibility.

use hubuum_query::{CursorValue, FilterField, QueryContinuation, SortParam, encode_cursor_values};
use hubuum_storage_postgres::capture_queries;
use rstest::rstest;

use crate::models::search::parse_query_parameter;
use crate::models::{
    HubuumObject, HubuumObjectID, NewHubuumClass, NewHubuumObject, TokenResourceScope, TokenScope,
};
use crate::pagination::prepare_db_pagination;
use crate::services::catalog;
use crate::tests::{ObjectFixture, TestScope, create_test_user};
use crate::traits::CursorPaginated;

async fn fixture(scope: &TestScope) -> ObjectFixture {
    scope
        .object_fixture(
            "shared_catalog",
            NewHubuumClass {
                collection_id: 0,
                name: scope.scoped_name("shared_class"),
                description: String::new(),
                json_schema: None,
                validate_schema: None,
            },
            (0..4)
                .map(|index| NewHubuumObject {
                    collection_id: 0,
                    hubuum_class_id: 0,
                    name: scope.scoped_name(&format!("shared_object_{index}")),
                    description: if index % 2 == 0 { "z" } else { "a" }.to_string(),
                    data: serde_json::json!({"facts": {"operating_system": {
                        "major_version": "9",
                        "version": "9.8",
                        "distribution": if index < 3 { "RedHat" } else { "Ubuntu" },
                    }}}),
                })
                .collect(),
        )
        .await
        .unwrap()
}

fn query(fixture: &ObjectFixture, filters: usize, distribution: &str) -> String {
    let mut query = format!("classes={}&sort=id&limit=1", fixture.class.id);
    for filter in [
        format!("distribution={distribution}"),
        "version=9.8".to_string(),
        "major_version=9".to_string(),
    ]
    .into_iter()
    .take(filters)
    {
        query.push_str("&json_data__icontains=facts,operating_system,");
        query.push_str(&filter);
    }
    query
}

#[rstest]
#[case::indexed(0, true, true, false)]
#[case::single_json(1, true, true, false)]
#[case::multiple_json(2, true, true, true)]
#[case::three_json(3, true, true, true)]
#[case::first_page(3, true, false, false)]
#[case::without_total(3, false, true, false)]
#[actix_web::test]
async fn shared_filtering_avoids_repeating_json_predicates(
    #[case] filters: usize,
    #[case] include_total: bool,
    #[case] continued: bool,
    #[case] shared: bool,
) {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let mut options = parse_query_parameter(&query(&fixture, filters, "RedHat")).unwrap();
    options.set_include_total(include_total);
    if continued {
        options
            .set_cursor(Some(
                encode_cursor_values(
                    options.sort(),
                    vec![CursorValue::Integer(i64::from(fixture.objects[0].id))],
                )
                .unwrap(),
            ))
            .unwrap();
    }
    let options = prepare_db_pagination::<HubuumObject>(&options).unwrap();
    let (result, queries) =
        capture_queries(catalog::list_objects(&scope.pool, 1, true, None, options)).await;
    let (rows, total) = result.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        total,
        include_total.then_some(if filters == 0 { 4 } else { 3 })
    );
    assert_eq!(
        queries.queries_matching("WITH catalog_matches"),
        usize::from(shared)
    );
    let json_evaluations: usize = queries
        .query_counts()
        .iter()
        .map(|(sql, count)| sql.matches(" ILIKE ").count() * count)
        .sum();
    assert_eq!(
        json_evaluations,
        filters * if include_total && !shared { 2 } else { 1 }
    );
    assert_eq!(queries.connection_checkouts(), 1);
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case::id(vec![SortParam::new(FilterField::Id, false)], vec![0, 1, 2])]
#[case::descending(vec![SortParam::new(FilterField::Name, true), SortParam::new(FilterField::Id, false)], vec![2, 1, 0])]
#[case::mixed(vec![SortParam::new(FilterField::Description, true), SortParam::new(FilterField::Id, false)], vec![0, 2, 1])]
#[actix_web::test]
async fn cursor_pages_keep_order_and_global_totals(
    #[case] sorts: Vec<SortParam>,
    #[case] expected: Vec<usize>,
    #[values(false, true)] internal: bool,
) {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let mut options = parse_query_parameter(&query(&fixture, 3, "RedHat")).unwrap();
    options.set_sort(sorts.try_into().unwrap());
    // Visit the first, middle, final, and empty page. The lookahead row is kept
    // here because this test targets storage rather than response finalization.
    for position in 0..=expected.len() {
        if position > 0 {
            let object = &fixture.objects[expected[position - 1]];
            let values = options
                .sort()
                .iter()
                .map(|sort| object.cursor_value(&sort.field).unwrap())
                .collect();
            if internal {
                options.set_continuation(QueryContinuation::new(options.sort(), values).unwrap());
            } else {
                options
                    .set_cursor(Some(encode_cursor_values(options.sort(), values).unwrap()))
                    .unwrap();
            }
        }
        let prepared = prepare_db_pagination::<HubuumObject>(&options).unwrap();
        let (result, queries) =
            capture_queries(catalog::list_objects(&scope.pool, 1, true, None, prepared)).await;
        let (rows, total) = result.unwrap();
        assert_eq!(total, Some(3));
        assert_eq!(
            rows.iter().map(|row| row.id).collect::<Vec<_>>(),
            expected
                .iter()
                .skip(position)
                .take(2)
                .map(|index| fixture.objects[*index].id)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            queries.queries_matching("WITH catalog_matches"),
            usize::from(position > 0)
        );
    }
    fixture.cleanup().await.unwrap();
}

#[rstest]
#[case::missing("Missing")]
#[case::quoted("x'%20OR%20TRUE%20--")]
#[actix_web::test]
async fn no_matches_return_zero_and_no_placeholder_object(#[case] distribution: &str) {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let mut options = parse_query_parameter(&query(&fixture, 3, distribution)).unwrap();
    options
        .set_cursor(Some(
            encode_cursor_values(
                options.sort(),
                vec![CursorValue::Integer(i64::from(fixture.objects[0].id))],
            )
            .unwrap(),
        ))
        .unwrap();
    let options = prepare_db_pagination::<HubuumObject>(&options).unwrap();
    let (rows, total) = catalog::list_objects(&scope.pool, 1, true, None, options)
        .await
        .unwrap();
    assert!(rows.is_empty());
    assert_eq!(total, Some(0));
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn shared_total_respects_token_resource_visibility() {
    let scope = TestScope::new();
    let fixture = fixture(&scope).await;
    let token_scope = TokenScope::from_stored_parts(
        None,
        Some(vec![TokenResourceScope::Object(
            HubuumObjectID::new(fixture.objects[0].id).unwrap(),
        )]),
    )
    .unwrap();
    let mut options = parse_query_parameter(&query(&fixture, 3, "RedHat")).unwrap();
    options
        .set_cursor(Some(
            encode_cursor_values(
                options.sort(),
                vec![CursorValue::Integer(i64::from(fixture.objects[0].id) - 1)],
            )
            .unwrap(),
        ))
        .unwrap();
    let options = prepare_db_pagination::<HubuumObject>(&options).unwrap();
    let (rows, total) = catalog::list_objects(&scope.pool, 1, true, Some(&token_scope), options)
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![fixture.objects[0].id]
    );
    assert_eq!(total, Some(1));
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn shared_total_excludes_collections_the_user_cannot_read() {
    let scope = TestScope::new();
    let visible = fixture(&scope).await;
    let hidden_scope = TestScope::new();
    let hidden = fixture(&hidden_scope).await;
    let user = create_test_user(&scope.pool).await;
    visible
        .collection
        .owner_group
        .add_member_without_events(&scope.pool, &user)
        .await
        .unwrap();
    let query = query(&visible, 3, "RedHat").replace(
        &format!("classes={}", visible.class.id),
        &format!("classes={},{}", visible.class.id, hidden.class.id),
    );
    let mut options = parse_query_parameter(&query).unwrap();
    options
        .set_cursor(Some(
            encode_cursor_values(
                options.sort(),
                vec![CursorValue::Integer(i64::from(visible.objects[0].id) - 1)],
            )
            .unwrap(),
        ))
        .unwrap();
    let options = prepare_db_pagination::<HubuumObject>(&options).unwrap();
    let (rows, total) = catalog::list_objects(&scope.pool, user.id, false, None, options)
        .await
        .unwrap();
    assert_eq!(total, Some(3));
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        visible.objects[..2]
            .iter()
            .map(|row| row.id)
            .collect::<Vec<_>>()
    );
    visible.cleanup().await.unwrap();
    hidden.cleanup().await.unwrap();
    user.delete_without_events(&scope.pool).await.unwrap();
}
