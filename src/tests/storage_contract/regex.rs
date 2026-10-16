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

// Null/missing values never match, including under negation, in any query path.
#[rstest]
#[case::empty("", "router", true)]
#[case::substring("out", "router", true)]
#[case::anchored("^router$", "a router", false)]
#[case::case_sensitive("ROUTER", "router", false)]
#[case::case_insensitive("(?i)^ROUTER$", "router", true)]
#[case::unicode_literal("^røuter$", "røuter", true)]
#[case::newline_dot("router.$", "router\n", false)]
#[case::explicit_dotall("(?s)router.$", "router\n", true)]
#[case::final_newline("^router$", "router\n", false)]
#[case::multiline("(?m)^router$", "switch\nrouter\nswitch", true)]
#[case::unicode_class(r"^\p{Greek}+$", "αβ", true)]
#[actix_web::test]
async fn regex_has_portable_results_across_query_paths(
    #[case] pattern: &str,
    #[case] value: &str,
    #[case] matched: bool,
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
        let count = usize::from(matched != negated);
        assert_eq!(
            result.unwrap(),
            (count, Some(count as i64)),
            "{path:?}: {pattern}"
        );
    }
}

#[rstest]
#[case::matching("_class$", true)]
#[case::nonmatching("^never_matches$", false)]
#[case::description("(?i)BACKEND", true)]
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
        let field = if pattern.starts_with("(?i)") {
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
                ParsedQueryParam::from_parts(
                    FilterField::Id,
                    SearchOperator::Equals { is_negated: false },
                    id.to_string(),
                )
                .unwrap(),
                ParsedQueryParam::from_parts(
                    field,
                    SearchOperator::Regex { is_negated: false },
                    pattern.clone(),
                )
                .unwrap(),
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
        let count = usize::from(matches);
        assert_eq!(result.unwrap(), (count, Some(count as i64)));
    }
}

#[rstest]
#[case::matching(FilterField::Name, "_match_[12]$", false, &[1, 2][..])]
#[case::nonmatching(FilterField::Name, "^never_matches$", false, &[][..])]
#[case::negated(FilterField::Name, "_match_[12]$", true, &[0][..])]
#[case::alias_matching(FilterField::Groupname, "_match_[12]$", false, &[1, 2][..])]
#[case::alias_nonmatching(FilterField::Groupname, "^never_matches$", false, &[][..])]
#[case::alias_negated(FilterField::Groupname, "_match_[12]$", true, &[0][..])]
#[case::scope(FilterField::IdentityScope, "^local$", false, &[0, 1, 2][..])]
#[case::scope_negated(FilterField::IdentityScope, "^local$", true, &[][..])]
#[actix_web::test]
async fn authorization_group_regex_filters_apply_before_paging(
    #[case] field: FilterField,
    #[case] pattern: &str,
    #[case] negated: bool,
    #[case] indices: &[usize],
    #[values(false, true)] direct_list: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let name = prefix("regex_group");
        let mut groups = Vec::new();
        for suffix in ["excluded", "match_1", "match_2"] {
            let group = backend
                .create_group(
                    StorageGroupCreate::new(None, format!("{name}_{suffix}"), None),
                    &EventContext::system(),
                )
                .await
                .unwrap()
                .into_value();
            groups.push(group);
        }
        let options = QueryOptions::new(
            vec![
                ParsedQueryParam::from_parts(
                    FilterField::Id,
                    SearchOperator::In { is_negated: false },
                    groups
                        .iter()
                        .map(|group| group.id().to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                )
                .unwrap(),
                ParsedQueryParam::from_parts(
                    field.clone(),
                    SearchOperator::Regex {
                        is_negated: negated,
                    },
                    pattern.to_string(),
                )
                .unwrap(),
            ],
            vec![],
            Some(1),
            None,
            true,
        )
        .unwrap();
        let result = if direct_list {
            backend
                .list_groups(StorageGroupListQuery::new(options))
                .await
                .map(|page| {
                    let (rows, total) = page.into_parts();
                    (
                        rows.iter().map(|group| group.id()).collect::<Vec<_>>(),
                        total,
                        false,
                    )
                })
        } else {
            backend
                .load_authorization_group_candidates(StorageAuthorizationGroupCandidateQuery::new(
                    options,
                    StorageCandidatePageLimit::try_new(1).unwrap(),
                ))
                .await
                .map(|page| {
                    let (rows, has_more) = page.into_parts();
                    (
                        rows.iter().map(|group| group.id()).collect::<Vec<_>>(),
                        None,
                        has_more,
                    )
                })
        };
        for group in &groups {
            backend
                .delete_group(group.id(), &EventContext::system())
                .await
                .unwrap()
                .into_value();
        }
        let first_page = indices
            .iter()
            .take(1)
            .map(|index| groups[*index].id())
            .collect::<Vec<_>>();
        assert_eq!(
            result.unwrap(),
            (
                first_page,
                direct_list.then_some(indices.len() as i64),
                !direct_list && indices.len() > 1
            )
        );
    }
}

#[rstest]
#[case::name(FilterField::Name, "_user$", true)]
#[case::username(FilterField::Username, "_user$", true)]
#[case::proper_name(FilterField::ProperName, "(?i)^storage contract user$", true)]
#[case::email(FilterField::Email, "@example[.]invalid$", true)]
#[case::identity_scope(FilterField::IdentityScope, "^local$", true)]
#[case::empty(FilterField::Name, "", true)]
#[case::nonmatching(FilterField::Name, "^never_matches$", false)]
#[actix_web::test]
async fn user_string_regex_filters_apply_before_counting(
    #[case] field: FilterField,
    #[case] pattern: &str,
    #[case] matched: bool,
    #[values(false, true)] negated: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user =
            create_backend_user(&backend, &format!("{}_user", prefix("regex_identity"))).await;
        let options = QueryOptions::new(
            vec![
                ParsedQueryParam::from_parts(
                    FilterField::Id,
                    SearchOperator::Equals { is_negated: false },
                    user.id.to_string(),
                )
                .unwrap(),
                ParsedQueryParam::from_parts(
                    field.clone(),
                    SearchOperator::Regex {
                        is_negated: negated,
                    },
                    pattern,
                )
                .unwrap(),
            ],
            vec![],
            Some(1),
            None,
            true,
        )
        .unwrap();
        let result = backend
            .list_users(StorageUserListQuery::new(options))
            .await
            .map(|page| {
                let (rows, total) = page.into_parts();
                (rows.len(), total)
            });
        delete_backend_user(&backend, user).await;
        let count = usize::from(matched != negated);
        assert_eq!(result.unwrap(), (count, Some(count as i64)));
    }
}

#[rstest]
#[case::name(FilterField::Name, "_(two|1)$", true)]
#[case::from_name(FilterField::NameFrom, "_(one|0)$", true)]
#[case::to_name(FilterField::NameTo, "_(two|1)$", true)]
#[case::description(FilterField::Description, "(?i)^backend-local", true)]
#[case::from_description(FilterField::DescriptionFrom, "(?i)^backend-local", true)]
#[case::to_description(FilterField::DescriptionTo, "(?i)^backend-local", true)]
#[case::nonmatching(FilterField::Name, "^never_matches$", false)]
#[case::empty(FilterField::Name, "", true)]
#[actix_web::test]
async fn graph_string_regex_filters_apply_before_counting(
    #[case] field: FilterField,
    #[case] pattern: &str,
    #[case] matched: bool,
    #[values(false, true)] objects: bool,
    #[values(false, true)] negated: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("regex_graph")).await;
        let options = QueryOptions::new(
            vec![
                ParsedQueryParam::from_parts(
                    field.clone(),
                    SearchOperator::Regex {
                        is_negated: negated,
                    },
                    pattern,
                )
                .unwrap(),
            ],
            vec![],
            Some(1),
            None,
            true,
        )
        .unwrap();
        let root = if objects {
            ResourceId::new(fixture.objects[0].id().id()).unwrap()
        } else {
            ResourceId::new(fixture.classes[0].id().id()).unwrap()
        };
        let query = StorageRelationGraphQuery::new(
            root,
            hubuum_query::TraversalBudget::new(1, 100).unwrap(),
            options,
            visibility(),
        );
        let result = if objects {
            backend.list_related_objects(query).await.map(|page| {
                let (rows, total) = page.into_parts();
                (rows.len(), total)
            })
        } else {
            backend.list_related_classes(query).await.map(|page| {
                let (rows, total) = page.into_parts();
                (rows.len(), total)
            })
        };
        delete_backend_relation_fixture(&backend, fixture).await;
        let count = usize::from(matched != negated);
        assert_eq!(result.unwrap(), (count, Some(count as i64)));
    }
}

#[rstest]
#[actix_web::test]
async fn nullable_user_values_do_not_match_regex(
    #[values(FilterField::ProperName, FilterField::Email)] field: FilterField,
    #[values(false, true)] negated: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let user = backend
            .create_user(StorageUserCreate::new(
                None,
                prefix("regex_nullable_user"),
                "storage-contract-password-hash",
                None,
                None,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value()
            .into_parts();
        let options = QueryOptions::new(
            vec![
                ParsedQueryParam::from_parts(
                    FilterField::Id,
                    SearchOperator::Equals { is_negated: false },
                    user.id().to_string(),
                )
                .unwrap(),
                ParsedQueryParam::from_parts(
                    field.clone(),
                    SearchOperator::Regex {
                        is_negated: negated,
                    },
                    "",
                )
                .unwrap(),
            ],
            vec![],
            Some(1),
            None,
            true,
        )
        .unwrap();
        let result = backend.list_users(StorageUserListQuery::new(options)).await;
        backend
            .delete_user(StorageUserDelete::new(user.id(), EventContext::system()))
            .await
            .unwrap()
            .into_value();
        let (rows, total) = result.unwrap().into_parts();
        assert_eq!((rows.len(), total), (0, Some(0)));
    }
}

#[rstest]
#[case::matching("_owner$", true)]
#[case::nonmatching("^never_matches$", false)]
#[actix_web::test]
async fn collection_permission_group_regex_filters_apply_before_counting(
    #[case] pattern: &str,
    #[case] matched: bool,
    #[values(false, true)] negated: bool,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_object_fixture(&backend, &prefix("regex_grant"), vec![]).await;
        backend
            .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                StorageAuthorizationGrantKey::new(
                    fixture.collection.id(),
                    fixture.owned_group.as_ref().unwrap().id(),
                ),
                [StorageAuthorizationPermission::ReadCollection],
                false,
                EventContext::system(),
            ))
            .await
            .unwrap()
            .into_value();
        let options = QueryOptions::new(
            vec![
                ParsedQueryParam::from_parts(
                    FilterField::Groupname,
                    SearchOperator::Regex {
                        is_negated: negated,
                    },
                    pattern,
                )
                .unwrap(),
            ],
            vec![],
            Some(1),
            None,
            true,
        )
        .unwrap();
        let result = backend
            .list_groups_with_collection_permission(
                StorageAuthorizationCollectionGroupsPageQuery::new(
                    StorageAuthorizationCollectionGroupsQuery::new(
                        fixture.collection.id(),
                        StorageAuthorizationPermission::ReadCollection,
                    ),
                    options,
                ),
            )
            .await;
        delete_backend_object_fixture(&backend, fixture).await;
        let (rows, total) = result.unwrap().into_parts();
        let count = usize::from(matched != negated);
        assert_eq!((rows.len(), total), (count, Some(count as i64)));
    }
}
