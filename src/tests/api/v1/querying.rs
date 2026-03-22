#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test as actix_test};
    use chrono::{NaiveDate, NaiveDateTime};
    use diesel::prelude::*;
    use rstest::rstest;

    use crate::db::with_connection;
    use crate::models::search::{DataType, SearchOperator};
    use crate::models::{
        HubuumClass, HubuumClassExpanded, HubuumObject, HubuumObjectWithPath, Namespace,
        NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
        NewNamespaceWithAssignee,
    };
    use crate::schema::hubuumobject::dsl::{
        created_at as object_created_at, hubuumobject, id as hubuumobject_id,
        updated_at as object_updated_at,
    };
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::assert_response_status;
    use crate::tests::{NamespaceFixture, TestContext, ensure_admin_group, test_context};
    use crate::traits::{CanDelete, CanSave};

    const STRING_OPERATORS: &[&str] = &[
        "equals",
        "iequals",
        "contains",
        "icontains",
        "startswith",
        "istartswith",
        "endswith",
        "iendswith",
        "like",
        "regex",
        "in",
        "is_null",
    ];
    const NUMERIC_DATE_OPERATORS: &[&str] =
        &["equals", "gt", "gte", "lt", "lte", "between", "in", "is_null"];
    const ARRAY_OPERATORS: &[&str] = &["equals", "contains", "is_null"];
    const BOOLEAN_OPERATORS: &[&str] = &["equals", "is_null"];
    const IP_NETWORK_JSON_OPERATORS: &[&str] = &[
        "within_network",
        "contains_network",
        "contains_ip",
        "overlaps_network",
        "inet_equals",
    ];
    const JSON_STRUCTURE_OPERATORS: &[&str] =
        &["in", "all", "array_length", "has_key", "is_null"];

    fn objects_in_class_endpoint(class_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/")
    }

    fn related_objects_endpoint(class_id: i32, root_object_id: i32) -> String {
        format!("/api/v1/classes/{class_id}/objects/{root_object_id}/related/objects")
    }

    fn documented_operators(section: &str) -> Vec<String> {
        let mut in_section = false;
        let mut operators = Vec::new();

        for line in include_str!("../../../../docs/querying.md").lines() {
            if let Some(heading) = line.strip_prefix("### ") {
                if in_section {
                    break;
                }
                in_section = heading == section;
                continue;
            }

            if in_section {
                if let Some(operator) = line
                    .strip_prefix("- `")
                    .and_then(|line| line.strip_suffix('`'))
                {
                    operators.push(operator.to_string());
                } else if !line.trim().is_empty() {
                    break;
                }
            }
        }

        operators
    }

    async fn create_objects_fixture(
        context: &TestContext,
        label: &str,
        names: &[&str],
    ) -> (NamespaceFixture, HubuumClass, Vec<HubuumObject>) {
        let namespace = context.namespace_fixture(label).await;
        let class = NewHubuumClass {
            namespace_id: namespace.namespace.id,
            name: format!("{label}_class"),
            description: format!("{label}_class"),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let mut objects = Vec::new();
        for name in names {
            objects.push(
                NewHubuumObject {
                    namespace_id: namespace.namespace.id,
                    hubuum_class_id: class.id,
                    data: serde_json::json!({ "name": name }),
                    name: (*name).to_string(),
                    description: (*name).to_string(),
                }
                .save(&context.pool)
                .await
                .unwrap(),
            );
        }

        (namespace, class, objects)
    }

    fn set_object_created_at(
        context: &TestContext,
        object: &HubuumObject,
        created_at: NaiveDateTime,
    ) {
        with_connection(&context.pool, |conn| {
            diesel::update(hubuumobject.filter(hubuumobject_id.eq(object.id)))
                .set((
                    object_created_at.eq(created_at),
                    object_updated_at.eq(created_at),
                ))
                .execute(conn)
        })
        .unwrap();
    }

    async fn create_boolean_class_fixture(context: &TestContext, label: &str) -> NamespaceFixture {
        let namespace = context.namespace_fixture(label).await;

        for (name, validate_schema) in [
            ("bool-true-a", true),
            ("bool-false", false),
            ("bool-true-b", true),
        ] {
            NewHubuumClass {
                namespace_id: namespace.namespace.id,
                name: format!("{label}-{name}"),
                description: format!("{label}-{name}"),
                json_schema: None,
                validate_schema: Some(validate_schema),
            }
            .save(&context.pool)
            .await
            .unwrap();
        }

        namespace
    }

    async fn create_related_objects_fixture(
        context: &TestContext,
        label: &str,
    ) -> (NamespaceFixture, Vec<HubuumClass>, Vec<HubuumObject>) {
        let namespace = context.namespace_fixture(label).await;

        let mut classes = Vec::new();
        for suffix in ["a", "b", "c"] {
            classes.push(
                NewHubuumClass {
                    namespace_id: namespace.namespace.id,
                    name: format!("{label}-class-{suffix}"),
                    description: format!("{label}-class-{suffix}"),
                    json_schema: None,
                    validate_schema: Some(false),
                }
                .save(&context.pool)
                .await
                .unwrap(),
            );
        }

        let relation_ab = NewHubuumClassRelation {
            from_hubuum_class_id: classes[0].id,
            to_hubuum_class_id: classes[1].id,
        }
        .save(&context.pool)
        .await
        .unwrap();
        let relation_bc = NewHubuumClassRelation {
            from_hubuum_class_id: classes[1].id,
            to_hubuum_class_id: classes[2].id,
        }
        .save(&context.pool)
        .await
        .unwrap();

        let mut objects = Vec::new();
        for (index, class) in classes.iter().enumerate() {
            objects.push(
                NewHubuumObject {
                    namespace_id: namespace.namespace.id,
                    hubuum_class_id: class.id,
                    data: serde_json::json!({ "index": index }),
                    name: format!("{label}-object-{index}"),
                    description: format!("{label}-object-{index}"),
                }
                .save(&context.pool)
                .await
                .unwrap(),
            );
        }

        NewHubuumObjectRelation {
            from_hubuum_object_id: objects[0].id,
            to_hubuum_object_id: objects[1].id,
            class_relation_id: relation_ab.id,
        }
        .save(&context.pool)
        .await
        .unwrap();
        NewHubuumObjectRelation {
            from_hubuum_object_id: objects[1].id,
            to_hubuum_object_id: objects[2].id,
            class_relation_id: relation_bc.id,
        }
        .save(&context.pool)
        .await
        .unwrap();

        (namespace, classes, objects)
    }

    #[rstest]
    #[case::string("String fields", STRING_OPERATORS)]
    #[case::numeric_date("Numeric and date fields", NUMERIC_DATE_OPERATORS)]
    #[case::array("Array fields", ARRAY_OPERATORS)]
    #[case::boolean("Boolean fields", BOOLEAN_OPERATORS)]
    #[case::ip_network_json("IP/network JSON fields", IP_NETWORK_JSON_OPERATORS)]
    #[case::json_structure("JSON array and structure operators", JSON_STRUCTURE_OPERATORS)]
    fn test_querying_docs_operator_lists(
        #[case] section: &str,
        #[case] expected_operators: &[&str],
    ) {
        let documented = documented_operators(section);
        let expected = expected_operators
            .iter()
            .map(|operator| operator.to_string())
            .collect::<Vec<_>>();
        assert_eq!(documented, expected);
    }

    #[rstest]
    #[case::string("String fields", DataType::String)]
    #[case::numeric_date("Numeric and date fields", DataType::NumericOrDate)]
    #[case::array("Array fields", DataType::Array)]
    #[case::boolean("Boolean fields", DataType::Boolean)]
    fn test_documented_operators_parse_for_documented_data_types(
        #[case] section: &str,
        #[case] data_type: DataType,
    ) {
        for operator in documented_operators(section) {
            let parsed = SearchOperator::new_from_string(&operator).unwrap();
            assert!(
                parsed.is_applicable_to(data_type),
                "operator '{operator}' from section '{section}' should apply to {data_type:?}",
            );
        }
    }

    #[test]
    fn test_any_is_alias_for_in() {
        let from_in = SearchOperator::new_from_string("in").unwrap();
        let from_any = SearchOperator::new_from_string("any").unwrap();
        assert_eq!(from_in, from_any);
        assert_eq!(from_in.to_string(), "in");

        let neg_in = SearchOperator::new_from_string("not_in").unwrap();
        let neg_any = SearchOperator::new_from_string("not_any").unwrap();
        assert_eq!(neg_in, neg_any);
        assert_eq!(neg_in.to_string(), "not_in");
    }

    #[test]
    fn test_documented_json_structure_operators_parse() {
        let documented = documented_operators("JSON array and structure operators");
        let expected = JSON_STRUCTURE_OPERATORS
            .iter()
            .map(|operator| operator.to_string())
            .collect::<Vec<_>>();

        assert_eq!(documented, expected);

        for operator in documented {
            let parsed = SearchOperator::new_from_string(&operator).unwrap();
            let rendered = parsed.to_string();
            assert_eq!(
                rendered, operator,
                "operator '{operator}' should round-trip"
            );

            let negated = SearchOperator::new_from_string(&format!("not_{operator}")).unwrap();
            assert_eq!(
                negated.to_string(),
                format!("not_{operator}"),
                "operator '{operator}' should support not_ round-trip"
            );
        }
    }

    #[test]
    fn test_documented_ip_network_json_operators_parse() {
        let documented = documented_operators("IP/network JSON fields");
        let expected = IP_NETWORK_JSON_OPERATORS
            .iter()
            .map(|operator| operator.to_string())
            .collect::<Vec<_>>();

        assert_eq!(documented, expected);

        for operator in documented {
            let parsed = SearchOperator::new_from_string(&operator).unwrap();
            let rendered = parsed.to_string();
            assert_eq!(
                rendered, operator,
                "operator '{operator}' should round-trip"
            );

            let negated = SearchOperator::new_from_string(&format!("not_{operator}")).unwrap();
            assert_eq!(
                negated.to_string(),
                format!("not_{operator}"),
                "operator '{operator}' should support not_ round-trip"
            );
        }
    }

    #[rstest]
    #[case::equals("name__equals=alpha-two", vec!["alpha-two"])]
    #[case::iequals("name__iequals=alpha-one", vec!["Alpha-One"])]
    #[case::contains("name__contains=ha-O", vec!["Alpha-One"])]
    #[case::icontains("name__icontains=ALPHA", vec!["Alpha-One", "alpha-two"])]
    #[case::startswith("name__startswith=Alpha", vec!["Alpha-One"])]
    #[case::istartswith("name__istartswith=alpha", vec!["Alpha-One", "alpha-two"])]
    #[case::endswith("name__endswith=ONE", vec!["Beta-ONE"])]
    #[case::iendswith("name__iendswith=one", vec!["Alpha-One", "Beta-ONE"])]
    #[case::like("name__like=Alpha-%", vec!["Alpha-One"])]
    #[case::regex("name__regex=^(Alpha|Beta)-.*$", vec!["Alpha-One", "Beta-ONE"])]
    #[case::in_op("name__in=Alpha-One,alpha-two", vec!["Alpha-One", "alpha-two"])]
    #[case::not_in_op("name__not_in=Alpha-One,alpha-two", vec!["Beta-ONE", "Gamma-Three"])]
    #[case::is_null_false("name__is_null=false", vec!["Alpha-One", "alpha-two", "Beta-ONE", "Gamma-Three"])]
    #[case::is_null_true("name__is_null=true", vec![])]
    #[case::not_is_null_true("name__not_is_null=true", vec!["Alpha-One", "alpha-two", "Beta-ONE", "Gamma-Three"])]
    #[actix_web::test]
    async fn test_documented_string_operators_on_objects(
        #[case] query: &str,
        #[case] expected_names: Vec<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let namespace_name = format!(
            "querying_strings_{}",
            query
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        let (namespace, class, _) = create_objects_fixture(
            &context,
            &namespace_name,
            &["Alpha-One", "alpha-two", "Beta-ONE", "Gamma-Three"],
        )
        .await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?{}&sort=id.asc",
                objects_in_class_endpoint(class.id),
                query
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = actix_test::read_body_json(resp).await;

        let object_names: Vec<&str> = objects.iter().map(|object| object.name.as_str()).collect();
        assert_eq!(object_names, expected_names);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::equals("id__equals=<0>", vec![0])]
    #[case::gt("id__gt=<0>", vec![1, 2])]
    #[case::gte("id__gte=<1>", vec![1, 2])]
    #[case::lt("id__lt=<2>", vec![0, 1])]
    #[case::lte("id__lte=<1>", vec![0, 1])]
    #[case::between("id__between=<0>,<1>", vec![0, 1])]
    #[case::in_op("id__in=<0>,<2>", vec![0, 2])]
    #[case::not_in_op("id__not_in=<0>,<2>", vec![1])]
    #[actix_web::test]
    async fn test_documented_numeric_operators_on_objects(
        #[case] query_template: &str,
        #[case] expected_indexes: Vec<usize>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let label = format!(
            "querying_numeric_{}",
            query_template
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        let (namespace, class, objects) =
            create_objects_fixture(&context, &label, &["n0", "n1", "n2"]).await;

        let query = objects
            .iter()
            .enumerate()
            .fold(query_template.to_string(), |acc, (index, object)| {
                acc.replace(&format!("<{index}>"), &object.id.to_string())
            });

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?{}&sort=id.asc",
                objects_in_class_endpoint(class.id),
                query
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let response_objects: Vec<HubuumObject> = actix_test::read_body_json(resp).await;

        let expected_ids: Vec<i32> = expected_indexes
            .iter()
            .map(|index| objects[*index].id)
            .collect();
        let fetched_ids: Vec<i32> = response_objects.iter().map(|object| object.id).collect();
        assert_eq!(fetched_ids, expected_ids);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::equals("created_at__equals=2024-01-02", vec!["dated-1"])]
    #[case::gt("created_at__gt=2024-01-01", vec!["dated-1", "dated-2"])]
    #[case::gte("created_at__gte=2024-01-02", vec!["dated-1", "dated-2"])]
    #[case::lt("created_at__lt=2024-01-03", vec!["dated-0", "dated-1"])]
    #[case::lte("created_at__lte=2024-01-02", vec!["dated-0", "dated-1"])]
    #[case::between("created_at__between=2024-01-02,2024-01-03", vec!["dated-1", "dated-2"])]
    #[case::in_op("created_at__in=2024-01-01,2024-01-03", vec!["dated-0", "dated-2"])]
    #[case::not_in_op("created_at__not_in=2024-01-01,2024-01-03", vec!["dated-1"])]
    #[actix_web::test]
    async fn test_documented_date_operators_on_objects(
        #[case] query: &str,
        #[case] expected_names: Vec<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let label = format!(
            "querying_dates_{}",
            query
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        let (namespace, class, objects) =
            create_objects_fixture(&context, &label, &["dated-0", "dated-1", "dated-2"]).await;

        for (object, (year, month, day)) in
            objects
                .iter()
                .zip([(2024, 1, 1), (2024, 1, 2), (2024, 1, 3)])
        {
            let created_at = NaiveDate::from_ymd_opt(year, month, day)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            set_object_created_at(&context, object, created_at);
        }

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?{}&sort=id.asc",
                objects_in_class_endpoint(class.id),
                query
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let response_objects: Vec<HubuumObject> = actix_test::read_body_json(resp).await;

        let object_names: Vec<&str> = response_objects
            .iter()
            .map(|object| object.name.as_str())
            .collect();
        assert_eq!(object_names, expected_names);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::equals("path__equals=<0>,<1>", vec![1])]
    #[case::contains("path__contains=<1>", vec![1, 2])]
    #[actix_web::test]
    async fn test_documented_array_operators_on_related_objects(
        #[case] query_template: &str,
        #[case] expected_indexes: Vec<usize>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let label = format!(
            "querying_arrays_{}",
            query_template
                .chars()
                .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
                .collect::<String>()
        );
        let (namespace, classes, objects) = create_related_objects_fixture(&context, &label).await;

        let query = objects
            .iter()
            .enumerate()
            .fold(query_template.to_string(), |acc, (index, object)| {
                acc.replace(&format!("<{index}>"), &object.id.to_string())
            });

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "{}?{}&sort=id.asc",
                related_objects_endpoint(classes[0].id, objects[0].id),
                query
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let related_objects: Vec<HubuumObjectWithPath> = actix_test::read_body_json(resp).await;

        let expected_ids: Vec<i32> = expected_indexes
            .iter()
            .map(|index| objects[*index].id)
            .collect();
        let fetched_ids: Vec<i32> = related_objects.iter().map(|object| object.id).collect();
        assert_eq!(fetched_ids, expected_ids);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[case::equals(
        "validate_schema__equals=true",
        vec!["querying_booleans-bool-true-a", "querying_booleans-bool-true-b"]
    )]
    #[actix_web::test]
    async fn test_documented_boolean_operators_on_classes(
        #[case] query: &str,
        #[case] expected_names: Vec<&str>,
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let namespace = create_boolean_class_fixture(&context, "querying_booleans").await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/classes?namespaces={}&{}&sort=name.asc",
                namespace.namespace.id, query
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let classes: Vec<crate::models::HubuumClassExpanded> =
            actix_test::read_body_json(resp).await;

        let class_names: Vec<&str> = classes.iter().map(|class| class.name.as_str()).collect();
        assert_eq!(class_names, expected_names);

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_sort_by_description_for_classes_namespaces_and_objects(
        #[future(awt)] test_context: TestContext,
    ) {
        let context = test_context;
        let admin_group = ensure_admin_group(&context.pool).await;

        let namespace_z = NewNamespaceWithAssignee {
            name: "querying_sort_description_ns_z".to_string(),
            description: "querying-sort-description-z".to_string(),
            group_id: admin_group.id,
        }
        .save(&context.pool)
        .await
        .unwrap();
        let namespace_a = NewNamespaceWithAssignee {
            name: "querying_sort_description_ns_a".to_string(),
            description: "querying-sort-description-a".to_string(),
            group_id: admin_group.id,
        }
        .save(&context.pool)
        .await
        .unwrap();
        let namespace_m = NewNamespaceWithAssignee {
            name: "querying_sort_description_ns_m".to_string(),
            description: "querying-sort-description-m".to_string(),
            group_id: admin_group.id,
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/namespaces?name__contains=querying_sort_description_ns_&sort=description.asc",
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let namespaces: Vec<Namespace> = actix_test::read_body_json(resp).await;

        let namespace_descriptions: Vec<&str> = namespaces
            .iter()
            .map(|namespace| namespace.description.as_str())
            .collect();
        assert_eq!(
            namespace_descriptions,
            vec![
                "querying-sort-description-a",
                "querying-sort-description-m",
                "querying-sort-description-z",
            ]
        );

        NewHubuumClass {
            namespace_id: namespace_a.id,
            name: "querying_sort_description_class_z".to_string(),
            description: "querying-sort-description-z".to_string(),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(&context.pool)
        .await
        .unwrap();
        NewHubuumClass {
            namespace_id: namespace_a.id,
            name: "querying_sort_description_class_a".to_string(),
            description: "querying-sort-description-a".to_string(),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(&context.pool)
        .await
        .unwrap();
        NewHubuumClass {
            namespace_id: namespace_a.id,
            name: "querying_sort_description_class_m".to_string(),
            description: "querying-sort-description-m".to_string(),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/classes?namespaces={}&name__contains=querying_sort_description_class_&sort=description.asc",
                namespace_a.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let classes: Vec<HubuumClassExpanded> = actix_test::read_body_json(resp).await;

        let class_descriptions: Vec<&str> = classes
            .iter()
            .map(|class| class.description.as_str())
            .collect();
        assert_eq!(
            class_descriptions,
            vec![
                "querying-sort-description-a",
                "querying-sort-description-m",
                "querying-sort-description-z",
            ]
        );

        let object_class = NewHubuumClass {
            namespace_id: namespace_a.id,
            name: "querying_sort_description_object_class".to_string(),
            description: "querying-sort-description-object-class".to_string(),
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(&context.pool)
        .await
        .unwrap();

        NewHubuumObject {
            namespace_id: namespace_a.id,
            hubuum_class_id: object_class.id,
            data: serde_json::json!({ "i": 1 }),
            name: "querying_sort_description_object_z".to_string(),
            description: "querying-sort-description-z".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();
        NewHubuumObject {
            namespace_id: namespace_a.id,
            hubuum_class_id: object_class.id,
            data: serde_json::json!({ "i": 2 }),
            name: "querying_sort_description_object_a".to_string(),
            description: "querying-sort-description-a".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();
        NewHubuumObject {
            namespace_id: namespace_a.id,
            hubuum_class_id: object_class.id,
            data: serde_json::json!({ "i": 3 }),
            name: "querying_sort_description_object_m".to_string(),
            description: "querying-sort-description-m".to_string(),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!(
                "/api/v1/classes/{}/?name__contains=querying_sort_description_object_&sort=description.asc",
                object_class.id
            ),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let objects: Vec<HubuumObject> = actix_test::read_body_json(resp).await;

        let object_descriptions: Vec<&str> = objects
            .iter()
            .map(|object| object.description.as_str())
            .collect();
        assert_eq!(
            object_descriptions,
            vec![
                "querying-sort-description-a",
                "querying-sort-description-m",
                "querying-sort-description-z",
            ]
        );

        namespace_a.delete(&context.pool).await.unwrap();
        namespace_m.delete(&context.pool).await.unwrap();
        namespace_z.delete(&context.pool).await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_sort_by_description_descending(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let admin_group = ensure_admin_group(&context.pool).await;

        let ns_z = NewNamespaceWithAssignee {
            name: "descending_sort_z".to_string(),
            description: "z-description".to_string(),
            group_id: admin_group.id,
        }
        .save(&context.pool)
        .await
        .unwrap();
        let ns_a = NewNamespaceWithAssignee {
            name: "descending_sort_a".to_string(),
            description: "a-description".to_string(),
            group_id: admin_group.id,
        }
        .save(&context.pool)
        .await
        .unwrap();
        let ns_m = NewNamespaceWithAssignee {
            name: "descending_sort_m".to_string(),
            description: "m-description".to_string(),
            group_id: admin_group.id,
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            "/api/v1/namespaces?name__contains=descending_sort_&sort=description.desc",
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let namespaces: Vec<Namespace> = actix_test::read_body_json(resp).await;

        let namespace_descriptions: Vec<&str> = namespaces
            .iter()
            .map(|namespace| namespace.description.as_str())
            .collect();
        assert_eq!(
            namespace_descriptions,
            vec!["z-description", "m-description", "a-description",]
        );

        ns_a.delete(&context.pool).await.unwrap();
        ns_m.delete(&context.pool).await.unwrap();
        ns_z.delete(&context.pool).await.unwrap();
    }
}
