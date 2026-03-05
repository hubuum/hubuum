#[cfg(test)]
mod test {
    use rstest::rstest;

    use crate::models::class::NewHubuumClass;
    use crate::models::group::GroupID;
    use crate::models::search::{ParsedQueryParam, QueryOptions, SearchOperator};
    use crate::models::{HubuumClass, Namespace, NewNamespace};
    use crate::tests::constants::{SchemaType, get_schema};
    use crate::tests::{TestContext, ensure_admin_group, test_context};
    use crate::traits::{CanDelete, CanSave, Search};

    struct TestCase {
        query: Vec<ParsedQueryParam>,
        expected: usize,
    }

    async fn setup_test_structure(
        context: &TestContext,
        prefix: &str,
    ) -> (Vec<Namespace>, Vec<HubuumClass>) {
        let pretty_prefix = prefix.replace("_", " ");
        let admin_group = ensure_admin_group(&context.pool).await;

        let mut namespaces = vec![];
        let mut classes = vec![];

        for i in 0..3 {
            let padded_i = format!("{i:02}");
            let namespace_name = format!("{prefix}_namespace_{padded_i}");
            let namespace_description = format!("{pretty_prefix} namespace {padded_i}");

            namespaces.push(
                NewNamespace {
                    name: namespace_name,
                    description: namespace_description,
                }
                .save_and_grant_all_to(&context.pool, GroupID(admin_group.id))
                .await
                .unwrap(),
            );
        }

        let blog_schema = get_schema(SchemaType::Blog);
        let address_schema = get_schema(SchemaType::Address);
        let geo_schema = get_schema(SchemaType::Geo);

        for i in 0..10 {
            let padded_i = format!("{i:02}");
            let mut nid = namespaces[0].id;
            let mut schema = blog_schema.clone();
            if i > 8 {
                nid = namespaces[2].id; // We'll get one class in this namespace (9)
                schema = geo_schema.clone();
            } else if i > 5 {
                nid = namespaces[1].id; // We'll get three classes in this namespace (6,7,8)
                schema = address_schema.clone();
            }

            classes.push(
                NewHubuumClass {
                    name: format!("{prefix}_class_{padded_i}"),
                    description: format!("{pretty_prefix} class {padded_i}"),
                    json_schema: Some(schema),
                    validate_schema: Some(false),
                    namespace_id: nid,
                }
                .save(&context.pool)
                .await
                .unwrap(),
            );
        }

        (namespaces, classes)
    }

    async fn check_test_cases(context: &TestContext, testcases: Vec<TestCase>) {
        for tc in testcases {
            let query_options = QueryOptions {
                filters: tc.query.clone(),
                sort: vec![],
                limit: None,
                cursor: None,
            };

            let hits = context
                .admin_user
                .search_classes(&context.pool, query_options.clone())
                .await
                .unwrap();
            assert_eq!(
                hits.len(),
                tc.expected,
                "Query: {:?}, Hits: {:?}",
                tc.query,
                hits
            );
        }
    }

    async fn cleanup(context: &TestContext, namespaces: Vec<Namespace>) {
        for ns in namespaces {
            ns.delete(&context.pool).await.unwrap();
        }
    }

    #[rstest]
    #[actix_rt::test]
    async fn test_equals(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (namespaces, classes) = setup_test_structure(&context, "test_user_class_equals").await;

        // Set which namespaces we want to search in
        let comma_separated_namespaces = namespaces
            .iter()
            .map(|ns| ns.id.to_string())
            .collect::<Vec<String>>()
            .join(",");

        let namespace_pgp = ParsedQueryParam::new(
            "namespaces",
            Some(SearchOperator::Equals { is_negated: false }),
            &comma_separated_namespaces,
        )
        .unwrap();

        let testcases = vec![
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "id",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &classes[0].id.to_string(),
                    )
                    .unwrap(),
                    namespace_pgp.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &classes[0].name,
                    )
                    .unwrap(),
                    namespace_pgp.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &namespaces[2].id.to_string(),
                    )
                    .unwrap(),
                    namespace_pgp.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "validate_schema",
                        Some(SearchOperator::Equals { is_negated: false }),
                        "true",
                    )
                    .unwrap(),
                    namespace_pgp.clone(),
                ],
                expected: 0,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "validate_schema",
                        Some(SearchOperator::Equals { is_negated: true }), // so true becomes false
                        "true",
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &namespaces[2].id.to_string(),
                    )
                    .unwrap(),
                ],
                expected: 1,
            },
        ];

        check_test_cases(&context, testcases).await;
        cleanup(&context, namespaces).await;
    }

    #[rstest]
    #[actix_rt::test]
    async fn test_class_search(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (namespaces, classes) = setup_test_structure(&context, "test_user_class_search").await;

        let nspqp = ParsedQueryParam::new(
            "namespaces",
            Some(SearchOperator::Equals { is_negated: false }),
            &namespaces
                .iter()
                .map(|ns| ns.id.to_string())
                .collect::<Vec<String>>()
                .join(","),
        )
        .unwrap();

        let testcases = vec![
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "id",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &classes[0].id.to_string(),
                    )
                    .unwrap(),
                    nspqp.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &namespaces[2].id.to_string(),
                    )
                    .unwrap(),
                    nspqp.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "id",
                        Some(SearchOperator::Gt { is_negated: false }),
                        &classes[1].id.to_string(),
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "id",
                        Some(SearchOperator::Lt { is_negated: false }),
                        &classes[3].id.to_string(),
                    )
                    .unwrap(),
                    nspqp.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "class_search",
                    )
                    .unwrap(),
                    nspqp.clone(),
                ],
                expected: 10,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::IContains { is_negated: false }),
                        "CLASS_search",
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &namespaces[1].id.to_string(),
                    )
                    .unwrap(),
                    nspqp.clone(),
                ],
                expected: 3,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "description",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "class search",
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "validate_schema",
                        Some(SearchOperator::Equals { is_negated: false }),
                        "true",
                    )
                    .unwrap(),
                    nspqp.clone(),
                ],
                expected: 0,
            },
        ];

        check_test_cases(&context, testcases).await;
        cleanup(&context, namespaces).await;
    }

    #[rstest]
    #[actix_rt::test]
    async fn test_search_int_ranges(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let (namespaces, _) = setup_test_structure(&context, "test_user_class_int_ranges").await;

        let testcases = vec![
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        format!("{}-{}", namespaces[1].id, namespaces[2].id).as_str(),
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "test_user_class_int_ranges",
                    )
                    .unwrap(),
                ],
                expected: 4,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        format!("{},{}", namespaces[0].id, namespaces[2].id).as_str(),
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "test_user_class_int_ranges",
                    )
                    .unwrap(),
                ],
                expected: 7,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        format!(
                            "{},{},{}",
                            namespaces[0].id, namespaces[1].id, namespaces[2].id
                        )
                        .as_str(),
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "test_user_class_int_ranges",
                    )
                    .unwrap(),
                ],
                expected: 10,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        format!("{}-{}", namespaces[0].id, namespaces[2].id).as_str(),
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "test_user_class_int_ranges",
                    )
                    .unwrap(),
                ],
                expected: 10,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        format!(
                            "{}-{},{}",
                            namespaces[0].id, namespaces[1].id, namespaces[2].id
                        )
                        .as_str(),
                    )
                    .unwrap(),
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "test_user_class_int_ranges",
                    )
                    .unwrap(),
                ],
                expected: 10,
            },
        ];

        check_test_cases(&context, testcases).await;
        cleanup(&context, namespaces).await;
    }

    fn generate_test_case_for_json_schema(
        operator: SearchOperator,
        value: &str,
        namespaces: Vec<Namespace>,
        expected_hits: usize,
    ) -> TestCase {
        // To ensure we're only searching within our namespaces, we bind the namespaces to the query (or
        // vice versa, depending on how you look at it). This is required as we run async tests and use the
        // same test data for a number of tests.
        let binding_pgp_to_our_namespace = ParsedQueryParam::new(
            "namespaces",
            Some(SearchOperator::Equals { is_negated: false }),
            &namespaces
                .iter()
                .map(|ns| ns.id.to_string())
                .collect::<Vec<String>>()
                .join(","),
        )
        .unwrap();

        TestCase {
            query: vec![
                ParsedQueryParam::new("json_schema", Some(operator), value).unwrap(),
                binding_pgp_to_our_namespace.clone(),
            ],
            expected: expected_hits,
        }
    }

    #[rstest]
    #[case::search_contains(
        SearchOperator::Contains { is_negated: false },
        "title=Geographical",
        1
    )]
    #[case::search_lt(
        SearchOperator::Lt { is_negated: false },
        "properties,latitude,minimum=0",
        1
    )]
    #[case::search_gte(
        SearchOperator::Gte { is_negated: false },
        "properties,latitude,maximum=90",
        1
    )]
    #[case::search_equals(
        SearchOperator::Equals { is_negated: false },
        "properties,latitude,minimum=0",
        0
    )]
    #[case::search_icontains(
        SearchOperator::IContains { is_negated: false },
        "description=address",
        3
    )]
    #[case::search_like(
        SearchOperator::Like { is_negated: false },
        "description=blog",
        6
    )]
    #[case::search_regex(
        SearchOperator::Regex { is_negated: false },
        "$id=.*ple\\.com.*loc",
        1
    )]
    #[actix_rt::test]
    async fn test_class_search_json_schema(
        #[case] op: SearchOperator,
        #[case] value: &str,
        #[case] hits: usize,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let prefix = format!("class_json_schema_{op}_{value}_{hits}");
        let (namespaces, _) = setup_test_structure(&context, &prefix).await;

        let testcases = vec![generate_test_case_for_json_schema(
            op,
            value,
            namespaces.clone(),
            hits,
        )];

        check_test_cases(&context, testcases).await;

        cleanup(&context, namespaces).await;
    }
}
