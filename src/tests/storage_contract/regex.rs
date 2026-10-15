use super::*;
use hubuum_storage_core::{StorageObjectAggregateDimension, StorageObjectAggregateScalarField};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use rstest::rstest;
use serde_json::json;

#[derive(Clone, Copy, Debug)]
enum QueryPath {
    Object,
    Computed,
    Aggregate,
}

fn visibility() -> StorageVisibility {
    StorageVisibility::new(
        principal_id(i32::MAX),
        true,
        None::<Vec<StorageAuthorizationPermission>>,
        None,
    )
}

// None means the backend rejects this dialect's pattern. Null/missing values
// never match, including under negation, in any of these query paths.
#[rstest]
#[case::substring("out", "router", Some(true), Some(true))]
#[case::anchored("^router$", "a router", Some(false), Some(false))]
#[case::case_sensitive("ROUTER", "router", Some(false), Some(false))]
#[case::case_insensitive("(?i)^ROUTER$", "router", Some(true), Some(true))]
#[case::unicode_literal("^røuter$", "røuter", Some(true), Some(true))]
#[case::postgres_word_boundary(r"\yrouter\y", "router", Some(true), None)]
#[case::newline_dot("router.$", "router\n", Some(true), Some(false))]
#[case::explicit_dotall("(?s)router.$", "router\n", Some(true), Some(true))]
#[case::final_newline("^router$", "router\n", Some(false), Some(false))]
#[case::multiline("(?m)^router$", "switch\nrouter\nswitch", Some(true), Some(true))]
#[case::backreference(r"^(router)\1$", "routerrouter", Some(true), None)]
#[case::lookahead(r"router(?=$)", "router", Some(true), None)]
#[case::rust_unicode_class(r"^\p{Greek}+$", "αβ", None, Some(true))]
#[case::invalid("[", "router", None, None)]
#[case::repetition_limit("a{1000000000}", "router", None, None)]
#[actix_web::test]
async fn regex_uses_the_selected_backends_native_dialect(
    #[case] pattern: &str,
    #[case] value: &str,
    #[case] postgres: Option<bool>,
    #[case] memory: Option<bool>,
    #[values(QueryPath::Object, QueryPath::Computed, QueryPath::Aggregate)] path: QueryPath,
    #[values(false, true)] negated: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("regex"),
            vec![json!({"value": value}), json!({"value": null}), json!({})],
        )
        .await;
        let owner = if matches!(path, QueryPath::Computed) {
            let owner = create_backend_user(&backend, &prefix("regex_owner")).await;
            let _ = backend
                .create_shared_computed_field(StorageSharedComputedFieldCreate::new(
                    fixture.class.id(),
                    fixture.collection.id(),
                    owner.principal_id,
                    StorageComputedFieldDefinitionInput::new(
                        Definition::new(
                            FieldKey::new("value").unwrap(),
                            "Value",
                            "",
                            Operation::FirstNonNull {
                                paths: vec![JsonPointer::new("/value").unwrap()],
                            },
                            ResultType::String,
                            true,
                        )
                        .unwrap(),
                    ),
                    EventContext::system(),
                ))
                .await
                .unwrap();
            Some(owner)
        } else {
            None
        };
        let field = if matches!(path, QueryPath::Computed) {
            "computed.shared.value"
        } else {
            "json_data"
        };
        let operand = if matches!(path, QueryPath::Computed) {
            pattern.to_string()
        } else {
            format!("value={pattern}")
        };
        let encoded = format!(
            "class_id={}&{field}__{}regex={}&sort=id",
            fixture.class.id(),
            if negated { "not_" } else { "" },
            utf8_percent_encode(&operand, NON_ALPHANUMERIC),
        );
        let (options, _) =
            parse_query_parameter_with_computed_filters_and_passthrough(&encoded, &[]).unwrap();
        let result = match path {
            QueryPath::Object => backend
                .list_objects(StorageCatalogListQuery::new(options, visibility()))
                .await
                .map(|page| {
                    let (rows, total) = page.into_parts();
                    (rows.len(), total)
                }),
            QueryPath::Computed => {
                let execution =
                    prepare_db_pagination::<crate::models::HubuumObject>(&options).unwrap();
                let limit = crate::pagination::effective_page_limit(&options).unwrap();
                backend
                    .list_computed_objects(StorageComputedObjectListQuery::new(
                        fixture.class.id(),
                        None,
                        StorageComputedObjectQueryOptions::try_new(options, execution, limit)
                            .unwrap(),
                        StorageComputedObjectVisibility::storage(visibility()),
                        StorageComputedObjectProjection::None,
                    ))
                    .await
                    .map(|page| {
                        let (rows, total, _, _) = page.into_parts();
                        (rows.len(), total)
                    })
            }
            QueryPath::Aggregate => backend
                .aggregate_objects(
                    StorageObjectAggregateQuery::builder(
                        StorageObjectAggregateTarget::new(
                            fixture.class.id(),
                            fixture.class.name().to_string(),
                            fixture.collection.id(),
                        ),
                        options,
                        StorageObjectAggregateSpec::try_new(
                            [StorageObjectAggregateDimension::Scalar(
                                StorageObjectAggregateScalarField::Name,
                            )],
                            [],
                            StorageObjectAggregateSort::DimensionsAscending,
                        )
                        .unwrap(),
                        visibility(),
                    )
                    .required_permissions([
                        StorageAuthorizationPermission::ReadObject,
                        StorageAuthorizationPermission::ReadCollection,
                    ])
                    .page_limit(50)
                    .cursor_max_encoded_bytes(4096)
                    .try_build()
                    .unwrap(),
                    StorageObjectAggregateAuthorization::Storage,
                )
                .await
                .map(|page| {
                    let (rows, total, _) = page.into_parts();
                    (rows.len(), total)
                }),
        };
        // Clean up before asserting so failures do not strand native fixtures.
        delete_backend_object_fixture(&backend, fixture).await;
        if let Some(owner) = owner {
            delete_backend_user(&backend, owner).await;
        }
        let expected = match backend.descriptor().kind() {
            StorageBackendKind::Postgres => postgres,
            StorageBackendKind::Memory => memory,
        };
        match expected {
            Some(matched) => {
                let count = usize::from(matched != negated);
                assert_eq!(
                    result.unwrap(),
                    (count, Some(count as i64)),
                    "{path:?}: {pattern}"
                );
            }
            None => assert_eq!(
                result.unwrap_err().kind(),
                StorageErrorKind::InvalidInput,
                "{path:?}: {pattern}"
            ),
        }
    }
}

#[rstest]
#[case::matching("_class$", true)]
#[case::nonmatching("^never_matches$", false)]
#[case::native_word_boundary(r"\ybackend\y", true)]
#[actix_web::test]
async fn catalog_string_regex_filters_are_applied_before_counting(
    #[case] pattern: &str,
    #[case] matches: bool,
    #[values("class", "collection")] target: &str,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture =
            create_backend_object_fixture(&backend, &prefix("regex_catalog"), vec![]).await;
        let pattern = pattern.replace("_class", &format!("_{target}"));
        let field = if pattern.starts_with(r"\y") {
            FilterField::Description
        } else {
            FilterField::Name
        };
        let id = if target == "class" {
            fixture.class.id().id()
        } else {
            fixture.collection.id().id()
        };
        let options = QueryOptions::new(
            vec![
                ParsedQueryParam {
                    field: FilterField::Id,
                    operator: SearchOperator::Equals { is_negated: false },
                    value: id.to_string(),
                },
                ParsedQueryParam {
                    field,
                    operator: SearchOperator::Regex { is_negated: false },
                    value: pattern.clone(),
                },
            ],
            vec![],
            Some(1),
            None,
            true,
        )
        .unwrap();
        let query = StorageCatalogListQuery::new(options, visibility());
        let result = if target == "class" {
            backend.list_classes(query).await.map(|page| {
                let (rows, total) = page.into_parts();
                (rows.len(), total)
            })
        } else {
            backend.list_collections(query).await.map(|page| {
                let (rows, total) = page.into_parts();
                (rows.len(), total)
            })
        };
        delete_backend_object_fixture(&backend, fixture).await;
        if pattern.starts_with(r"\y") && backend.descriptor().kind() == StorageBackendKind::Memory {
            assert_eq!(result.unwrap_err().kind(), StorageErrorKind::InvalidInput);
        } else {
            let count = usize::from(matches);
            assert_eq!(result.unwrap(), (count, Some(count as i64)));
        }
    }
}
