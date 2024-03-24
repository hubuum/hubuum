#[cfg(test)]

mod test {
    use futures::join;

    use crate::models::class::NewHubuumClass;
    use crate::models::group::GroupID;
    use crate::models::search::{ParsedQueryParam, SearchOperator};
    use crate::models::{HubuumClass, Namespace, NewNamespace};
    use crate::tests::{ensure_admin_group, ensure_admin_user, setup_pool_and_tokens};
    use crate::traits::{CanDelete, CanSave, SearchClasses};

    struct TestCase {
        query: Vec<ParsedQueryParam>,
        expected: usize,
    }

    async fn setup_test_structure(prefix: &str) -> (Vec<Namespace>, Vec<HubuumClass>) {
        let pretty_prefix = prefix.replace("_", " ");

        let (pool, _, _) = setup_pool_and_tokens().await;
        let admin_group = ensure_admin_group(&pool).await;

        let mut namespaces = vec![];
        let mut classes = vec![];

        for i in 0..3 {
            let padded_i = format!("{:02}", i);
            let namespace_name = format!("{}_namespace_{}", prefix, padded_i);
            let namespace_description = format!("{} namespace {}", pretty_prefix, padded_i);

            namespaces.push(
                NewNamespace {
                    name: namespace_name,
                    description: namespace_description,
                }
                .save_and_grant_all_to(&pool, GroupID(admin_group.id))
                .await
                .unwrap(),
            );
        }

        let blog_schema = serde_json::json!(
        {
            "$id": "https://example.com/blog-post.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "description": "A representation of a blog post",
            "type": "object",
            "required": ["title", "content", "author"],
            "properties": {
              "title": {
                "type": "string"
              },
              "content": {
                "type": "string"
              },
              "publishedDate": {
                "type": "string",
                "format": "date-time"
              },
              "author": {
                "$ref": "https://example.com/user-profile.schema.json"
              },
              "tags": {
                "type": "array",
                "items": {
                  "type": "string"
                }
              }
            }
        });

        let address_schema = serde_json::json!(
            {
                "$id": "https://example.com/address.schema.json",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "description": "An address similar to http://microformats.org/wiki/h-card",
                "type": "object",
                "properties": {
                  "postOfficeBox": {
                    "type": "string"
                  },
                  "extendedAddress": {
                    "type": "string"
                  },
                  "streetAddress": {
                    "type": "string"
                  },
                  "locality": {
                    "type": "string"
                  },
                  "region": {
                    "type": "string"
                  },
                  "postalCode": {
                    "type": "string"
                  },
                  "countryName": {
                    "type": "string"
                  }
                },
                "required": [ "locality", "region", "countryName" ],
                "dependentRequired": {
                  "postOfficeBox": [ "streetAddress" ],
                  "extendedAddress": [ "streetAddress" ]
                }
              }
        );

        let geo_schema = serde_json::json!(
            {
                "$id": "https://example.com/geographical-location.schema.json",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Geographical Location",
                "description": "A geographical location",
                "required": [ "latitude", "longitude" ],
                "type": "object",
                "properties": {
                  "latitude": {
                    "type": "number",
                    "minimum": -90,
                    "maximum": 90
                  },
                  "longitude": {
                    "type": "number",
                    "minimum": -180,
                    "maximum": 180
                  }
                },
                "required": [ "latitude", "longitude" ]
              }
        );

        for i in 0..10 {
            let padded_i = format!("{:02}", i);
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
                    name: format!("{}_class_{}", prefix, padded_i),
                    description: format!("{} class {}", pretty_prefix, padded_i),
                    json_schema: schema,
                    validate_schema: false,
                    namespace_id: nid,
                }
                .save(&pool)
                .await
                .unwrap(),
            );
        }

        (namespaces, classes)
    }

    async fn check_test_cases(testcases: Vec<TestCase>) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        let admin_user = ensure_admin_user(&pool).await;

        for tc in testcases {
            let hits = admin_user
                .search_classes(&pool, tc.query.clone())
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

    async fn cleanup(namespaces: Vec<Namespace>) {
        let (pool, _, _) = setup_pool_and_tokens().await;
        for ns in namespaces {
            ns.delete(&pool).await.unwrap();
        }
    }

    #[actix_rt::test]
    async fn test_equals() {
        let (namespaces, classes) = setup_test_structure("test_user_class_equals").await;

        let testcases = vec![
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "id",
                    Some(SearchOperator::Equals { is_negated: false }),
                    &classes[0].id.to_string(),
                )],
                expected: 1,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "name",
                    Some(SearchOperator::Equals { is_negated: false }),
                    &classes[0].name,
                )],
                expected: 1,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "namespaces",
                    Some(SearchOperator::Equals { is_negated: false }),
                    &namespaces[2].id.to_string(),
                )],
                expected: 1,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "validate_schema",
                    Some(SearchOperator::Equals { is_negated: false }),
                    "true",
                )],
                expected: 0,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "validate_schema",
                        Some(SearchOperator::Equals { is_negated: true }), // so true becomes false
                        "true",
                    ),
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &namespaces[2].id.to_string(),
                    ),
                ],
                expected: 1,
            },
        ];

        check_test_cases(testcases).await;
        cleanup(namespaces).await;
    }

    #[actix_rt::test]
    async fn test_class_search() {
        let (namespaces, classes) = setup_test_structure("test_user_class_search").await;

        let testcases = vec![
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "id",
                    Some(SearchOperator::Equals { is_negated: false }),
                    &classes[0].id.to_string(),
                )],
                expected: 1,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "namespaces",
                    Some(SearchOperator::Equals { is_negated: false }),
                    &namespaces[2].id.to_string(),
                )],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "id",
                        Some(SearchOperator::Gt { is_negated: false }),
                        &classes[1].id.to_string(),
                    ),
                    ParsedQueryParam::new(
                        "id",
                        Some(SearchOperator::Lt { is_negated: false }),
                        &classes[3].id.to_string(),
                    ),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "name",
                    Some(SearchOperator::Contains { is_negated: false }),
                    "class_search",
                )],
                expected: 10,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "name",
                        Some(SearchOperator::IContains { is_negated: false }),
                        "CLASS_search",
                    ),
                    ParsedQueryParam::new(
                        "namespaces",
                        Some(SearchOperator::Equals { is_negated: false }),
                        &namespaces[1].id.to_string(),
                    ),
                ],
                expected: 3,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "description",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "class search",
                    ),
                    ParsedQueryParam::new(
                        "validate_schema",
                        Some(SearchOperator::Equals { is_negated: false }),
                        "true",
                    ),
                ],
                expected: 0,
            },
        ];

        check_test_cases(testcases).await;
        cleanup(namespaces).await;
    }

    #[actix_rt::test]
    async fn test_search_int_ranges() {
        let (namespaces, _) = setup_test_structure("test_user_class_int_ranges").await;

        let testcases = vec![
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "namespaces",
                    Some(SearchOperator::Equals { is_negated: false }),
                    format!("{}-{}", namespaces[1].id, namespaces[2].id).as_str(),
                )],
                expected: 4,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "namespaces",
                    Some(SearchOperator::Equals { is_negated: false }),
                    format!("{},{}", namespaces[0].id, namespaces[2].id).as_str(),
                )],
                expected: 7,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "namespaces",
                    Some(SearchOperator::Equals { is_negated: false }),
                    format!(
                        "{},{},{}",
                        namespaces[0].id, namespaces[1].id, namespaces[2].id
                    )
                    .as_str(),
                )],
                expected: 10,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "namespaces",
                    Some(SearchOperator::Equals { is_negated: false }),
                    format!("{}-{}", namespaces[0].id, namespaces[2].id).as_str(),
                )],
                expected: 10,
            },
            TestCase {
                query: vec![ParsedQueryParam::new(
                    "namespaces",
                    Some(SearchOperator::Equals { is_negated: false }),
                    format!(
                        "{}-{},{}",
                        namespaces[0].id, namespaces[1].id, namespaces[2].id
                    )
                    .as_str(),
                )],
                expected: 10,
            },
        ];

        check_test_cases(testcases).await;
        cleanup(namespaces).await;
    }

    #[actix_rt::test]
    async fn test_class_search_json_schema() {
        let (namespaces, _) = setup_test_structure("test_user_class_json_schema").await;

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
        );

        let testcases = vec![
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "json_schema",
                        Some(SearchOperator::Contains { is_negated: false }),
                        "title=Geographical",
                    ),
                    binding_pgp_to_our_namespace.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "json_schema",
                        Some(SearchOperator::Lt { is_negated: false }),
                        "properties,latitude,minimum=0",
                    ),
                    binding_pgp_to_our_namespace.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "json_schema",
                        Some(SearchOperator::Equals { is_negated: false }),
                        "properties,latitude,maximum=90",
                    ),
                    binding_pgp_to_our_namespace.clone(),
                ],
                expected: 1,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "json_schema",
                        Some(SearchOperator::IContains { is_negated: false }),
                        "description=address",
                    ),
                    binding_pgp_to_our_namespace.clone(),
                ],
                expected: 3,
            },
            TestCase {
                query: vec![
                    ParsedQueryParam::new(
                        "json_schema",
                        Some(SearchOperator::Like { is_negated: false }),
                        "description=blog",
                    ),
                    binding_pgp_to_our_namespace.clone(),
                ],
                expected: 6,
            },
        ];

        check_test_cases(testcases).await;

        cleanup(namespaces).await;
    }
}
