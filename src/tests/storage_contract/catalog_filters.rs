use super::*;
use hubuum_query::parse_query_parameter;
use rstest::rstest;
use serde_json::{Value, json};

#[rstest]
#[case("json_data=value=router", json!("router"), json!("switch"))]
#[case("json_data__not_equals=value=router", json!("switch"), json!("router"))]
#[case("json_data__iequals=value=ROUTER", json!("router"), json!("switch"))]
#[case("json_data__contains=value=out", json!("router"), json!("switch"))]
#[case("json_data__like=value=out", json!("router"), json!("switch"))]
#[case("json_data__not_like=value=out", json!("switch"), json!("router"))]
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
#[case::numeric_integer_limit("json_data__gt=value=42", json!("1e131071"), json!("1e131072"))]
#[case::numeric_overflow("json_data__gt=value=42", json!(43), json!("1e100000000"))]
#[case::numeric_overflow_negated("json_data__not_gt=value=42", json!(41), json!("1e100000000"))]
#[case::numeric_scale_limit("json_data__lt=value=42", json!("1e-16383"), json!("1e-16384"))]
#[case::numeric_scale_limit_negated("json_data__not_lt=value=42", json!(43), json!("1e-16384"))]
#[case::numeric_trailing_scale("json_data__lt=value=42", json!("1.0e-16382"), json!("1.0e-16383"))]
#[case::numeric_zero_exponent("json_data=value=0", json!("0e100000000"), json!("0e-100000000"))]
#[case::numeric_exponent_limit("json_data=value=0", json!("0e1073741823"), json!("0e1073741824"))]
#[case::numeric_nan("json_data__gt=value=42", json!("NaN"), json!(42))]
#[case::numeric_nan_negated("json_data__not_equals=value=42", json!(" nAn "), json!(42))]
#[case::numeric_nan_range("json_data__between=value=40,42", json!(41), json!("NaN"))]
#[case::numeric_nan_negated_range("json_data__not_between=value=40,42", json!("NaN"), json!(41))]
#[case::numeric_infinity("json_data__gt=value=42", json!("Infinity"), json!(42))]
#[case::numeric_signed_infinity("json_data__gte=value=42", json!(" +iNf "), json!(41))]
#[case::numeric_negative_infinity("json_data__lt=value=42", json!("-Infinity"), json!(42))]
#[case::numeric_negative_infinity_negated("json_data__not_gte=value=42", json!("-inf"), json!(42))]
#[case::numeric_signed_nan_invalid("json_data__not_equals=value=42", json!(41), json!("+NaN"))]
#[case("json_data=value=true", json!(true), json!(false))]
#[case("json_data=value=true", json!("yes"), json!("invalid"))]
#[case("json_data__not_equals=value=true", json!(false), Value::Null)]
#[case("json_data=value=2026-01-01", json!("2026-01-01"), json!("2026-01-02"))]
#[case("json_data=value=2026-01-01", json!("2026-01-01"), json!("2026-01-01,2026-01-02"))]
#[case("json_data__not_equals=value=2026-01-01", json!("2026-01-02"), json!("2026-01-02,2026-01-03"))]
#[case("json_data=value=2026-01-01", json!("2026-01-01 00:00:00"), json!("2026-01-02 00:00:00"))]
#[case("json_data__gt=value=2026-01-01", json!("2026-01-02T00:00:00Z"), json!("invalid"))]
#[case::sql_timestamp_offset("json_data=value=2026-01-01", json!("2026-01-01 00:00:00+00"), json!("2026-01-02 00:00:00+00"))]
#[case::sql_timestamp_negated("json_data__not_equals=value=2026-01-01", json!("2026-01-02 00:00:00+00"), json!("2026-01-01 00:00:00+00"))]
#[case::sql_timestamp_positive_offset("json_data=value=2026-01-01", json!("2026-01-01 01:00:00+01"), json!("2026-01-01 00:00:00+01"))]
#[case::sql_timestamp_negative_offset("json_data=value=2026-01-01", json!("2025-12-31 23:00:00-01"), json!("2026-01-01 00:00:00-01"))]
#[case::sql_timestamp_offset_minutes("json_data=value=2026-01-01", json!("2026-01-01 05:30:00+0530"), json!("2026-01-01 05:30:00+0500"))]
#[case::sql_timestamp_spaced_offset("json_data=value=2026-01-01", json!(" 2026-01-01 05:30:00 +05:30 "), json!("2026-01-01 00:00:00 +05:30"))]
#[case::iso_timestamp_short_offset("json_data=value=2026-01-01", json!("2026-01-01T01:00:00+01"), json!("2026-01-01T00:00:00+01"))]
#[case::sql_timestamp_fractional("json_data__gt=value=2026-01-01", json!("2026-01-01 00:00:00.123456+00"), json!("2026-01-01 00:00:00+00"))]
#[case::sql_timestamp_malformed_negated("json_data__not_equals=value=2026-01-01", json!("2026-01-02 00:00:00+00"), json!("2026-01-02 00:00:00+00,2026-01-03"))]
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
#[case("name", "like", "object", 0)]
#[case("name", "like", "%object%", 2)]
#[case("name", "not_like", "object", 2)]
#[case("description", "like", "compatibility", 0)]
#[case("description", "like", "%compatibility%", 2)]
#[case("description", "not_like", "compatibility", 2)]
#[actix_web::test]
async fn ordinary_like_filters_require_explicit_wildcards(
    #[case] field: &str,
    #[case] operator: &str,
    #[case] pattern: &str,
    #[case] expected: usize,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("ordinary_like"),
            vec![json!({}), json!({})],
        )
        .await;
        let options = parse_query_parameter(&format!(
            "class_id={}&{field}__{operator}={pattern}",
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
            rows.len(),
            expected,
            "{}",
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
