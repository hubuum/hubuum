use super::*;
use hubuum_query::parse_query_parameter;
use rstest::rstest;
use serde_json::{Value, json};

#[rstest]
#[case("json_data=value=router", json!("router"), json!("switch"))]
#[case("json_data__not_equals=value=router", json!("switch"), json!("router"))]
#[case("json_data__iequals=value=ROUTER", json!("router"), json!("switch"))]
#[case("json_data__contains=value=out", json!("router"), json!("switch"))]
#[case("json_data__startswith=value=r%t", json!("router"), json!("switch"))]
#[case("json_data__regex=value=^r.*r$", json!("router"), json!("switch"))]
#[case("json_data=value=42", json!(42), json!(43))]
#[case("json_data=value=42", json!("42"), json!("invalid"))]
#[case("json_data__gt=value=42", json!(42.01), json!(42))]
#[case("json_data__gte=value=42", json!(42), json!(41))]
#[case("json_data__lt=value=42", json!(41), json!(42))]
#[case("json_data__lte=value=42", json!(42), json!(43))]
#[case("json_data__between=value=40,42", json!(41), json!(43))]
#[case("json_data__not_gt=value=42", json!(41), json!("invalid"))]
#[case("json_data=value=true", json!(true), json!(false))]
#[case("json_data=value=true", json!("yes"), json!("invalid"))]
#[case("json_data__not_equals=value=true", json!(false), Value::Null)]
#[case("json_data=value=2026-01-01", json!("2026-01-01"), json!("2026-01-02"))]
#[case("json_data__gt=value=2026-01-01", json!("2026-01-02T00:00:00Z"), json!("invalid"))]
#[case("json_data__is_null=value", Value::Null, json!(false))]
#[case("json_data__not_is_null=value", json!(false), Value::Null)]
#[case("json_data__in=value=red,blue", json!("red"), json!("green"))]
#[case("json_data__in=value=red,blue", json!(["red"]), json!(["green"]))]
#[case("json_data__all=value=red,blue", json!(["red", "blue"]), json!(["red"]))]
#[case("json_data__array_length=value=2", json!([1, 2]), json!([1]))]
#[case("json_data__has_key=value=red", json!({"red": 1}), json!({"blue": 1}))]
#[case("json_data__within_network=value=192.0.2.0/24", json!("192.0.2.5"), json!("198.51.100.1"))]
#[case("json_data__contains_ip=value=192.0.2.5", json!("192.0.2.0/24"), json!("198.51.100.0/24"))]
#[actix_web::test]
async fn direct_json_filters_agree_across_backends(
    #[case] query: &str,
    #[case] matching: Value,
    #[case] other: Value,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("json_filter_contract"),
            vec![json!({"value": matching}), json!({"value": other})],
        )
        .await;
        let options =
            parse_query_parameter(&format!("class_id={}&{query}", fixture.class.id().id()))
                .unwrap();
        let (rows, _) = backend
            .list_objects(StorageCatalogListQuery::new(
                options,
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            ))
            .await
            .unwrap()
            .into_parts();
        let expected = vec![fixture.objects[0].id()];
        assert_eq!(
            rows.iter().map(StorageObject::id).collect::<Vec<_>>(),
            expected,
            "{}: {query}",
            backend.descriptor().kind().as_str()
        );
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case("class_id")]
#[case("classes")]
#[actix_web::test]
async fn class_filters_exclude_other_classes_before_counting(#[case] field: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("class_scope"),
            vec![json!({}), json!({})],
        )
        .await;
        let other =
            create_backend_object_fixture(&backend, &prefix("other_class"), vec![json!({})]).await;
        let options = parse_query_parameter(&format!(
            "{field}={}&limit=1&include_total=true",
            fixture.class.id().id()
        ))
        .unwrap();
        let (rows, total) = backend
            .list_objects(StorageCatalogListQuery::new(
                options,
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            ))
            .await
            .unwrap()
            .into_parts();
        assert_eq!(
            (rows.len(), total, rows[0].class_id()),
            (1, Some(2), fixture.class.id())
        );
        delete_backend_object_fixture(&backend, other).await;
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[rstest]
#[case("hardware,serial")]
#[case("hardware,items,0,serial")]
#[actix_web::test]
async fn nested_json_paths_match_across_backends(#[case] path: &str) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("nested_filter"),
            vec![
                json!({"hardware":{"serial":"abc", "items":[{"serial":"abc"}]}}),
                json!({"hardware":{"serial":"def", "items":[]}}),
                json!({}),
            ],
        )
        .await;
        let options = parse_query_parameter(&format!(
            "class_id={}&json_data={path}=abc",
            fixture.class.id().id()
        ))
        .unwrap();
        let (rows, _) = backend
            .list_objects(StorageCatalogListQuery::new(
                options,
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            ))
            .await
            .unwrap()
            .into_parts();
        assert_eq!(
            rows.iter().map(StorageObject::id).collect::<Vec<_>>(),
            vec![fixture.objects[0].id()]
        );
        delete_backend_object_fixture(&backend, fixture).await;
    }
}
