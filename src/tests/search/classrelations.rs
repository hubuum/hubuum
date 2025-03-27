#[cfg(test)]
mod test {
    use regex::Regex;
    use yare::parameterized;

    use crate::db::DbPool;
    use crate::models::class::NewHubuumClass;
    use crate::models::group::GroupID;
    use crate::models::search::{FilterField, ParsedQueryParam, SearchOperator};
    use crate::models::{
        HubuumClass, HubuumClassRelation, Namespace, NewHubuumClassRelation, NewNamespace,
    };
    use crate::tests::{ensure_admin_group, ensure_admin_user, setup_pool_and_tokens};
    use crate::traits::{CanDelete, CanSave, Search};

    async fn create_data(
        pool: &DbPool,
        prefix: &str,
    ) -> (Namespace, Vec<HubuumClass>, Vec<HubuumClassRelation>) {
        let admin_group = ensure_admin_group(pool).await;
        let crname = format!("{}-classrelation", prefix);
        let namespace = NewNamespace {
            name: crname.clone(),
            description: crname.clone(),
        };
        let namespace = namespace
            .save_and_grant_all_to(pool, GroupID(admin_group.id))
            .await
            .unwrap();

        let mut classes = Vec::new();
        for i in 0..4 {
            let label = format!("{}-class-{}", prefix, i);
            let class = NewHubuumClass {
                name: label.clone(),
                description: label,
                namespace_id: namespace.id,
                json_schema: None,
                validate_schema: None,
            };
            let class = class.save(pool).await.unwrap();
            classes.push(class);
        }

        let mut relations = Vec::new();
        let rel1 = NewHubuumClassRelation {
            from_hubuum_class_id: classes[0].id,
            to_hubuum_class_id: classes[1].id,
        }
        .save(pool)
        .await
        .unwrap();

        let rel2 = NewHubuumClassRelation {
            from_hubuum_class_id: classes[1].id,
            to_hubuum_class_id: classes[2].id,
        }
        .save(pool)
        .await
        .unwrap();

        relations.push(rel1);
        relations.push(rel2);

        (namespace, classes, relations)
    }

    fn relations_constraint_query(relations: &[HubuumClassRelation]) -> ParsedQueryParam {
        ParsedQueryParam {
            field: FilterField::Id,
            operator: SearchOperator::Equals { is_negated: false },
            value: relations
                .iter()
                .map(|r| r.id.to_string())
                .collect::<Vec<String>>()
                .join(","),
        }
    }

    // field, operator, value, list_of_expected_ids
    // Note: The relations are class-0 -> class-1 and class-1 -> class-2.
    #[parameterized(
        search_by_id = { FilterField::ClassFrom,  SearchOperator::Equals { is_negated: false }, "<0>", vec![0] },
        search_by_class_from_name_contains = { FilterField::ClassFromName,  SearchOperator::Contains { is_negated: false }, "class_field", vec![0,1] },
        search_by_class_from_name_endswith = { FilterField::ClassFromName,  SearchOperator::EndsWith { is_negated: false }, "class-0", vec![0] },
        search_by_class_to_name_contains = { FilterField::ClassToName,  SearchOperator::Contains { is_negated: false }, "class_field", vec![0,1] },
        search_by_class_to_name_endswith = { FilterField::ClassToName,  SearchOperator::EndsWith { is_negated: false }, "class-1", vec![0] },

    )]
    #[test_macro(actix_rt::test)]
    async fn test_filter_by_class_field(
        field: FilterField,
        operator: SearchOperator,
        value: &str,
        list_of_expected_ids: Vec<usize>,
    ) {
        let prefix = format!(
            "test_filter_by_class_field_{}_{}_{}",
            field, operator, value
        );
        let (pool, _, _) = setup_pool_and_tokens().await;
        let (namespace, _classes, relations) = create_data(&pool, &prefix).await;

        let re = Regex::new(r"<(\d+)>").unwrap();
        let value = re.replace_all(value, |caps: &regex::Captures| {
            let index = caps[1].parse::<usize>().unwrap();

            match field {
                FilterField::ClassFrom => relations[index].from_hubuum_class_id.to_string(),
                FilterField::ClassTo => relations[index].to_hubuum_class_id.to_string(),
                FilterField::ClassFromName => value.to_string(),
                FilterField::ClassToName => value.to_string(),
                _ => panic!("unexpected field: {:?}", field),
            }
        });

        let query = vec![
            ParsedQueryParam {
                field: field.clone(),
                operator,
                value: value.to_string(),
            },
            relations_constraint_query(&relations),
        ];

        let admin_user = ensure_admin_user(&pool).await;
        let result = admin_user
            .search_class_relations(&pool, query)
            .await
            .unwrap();

        assert_eq!(result.len(), list_of_expected_ids.len());

        match field {
            FilterField::ClassFrom => {
                for (index, relation) in result.iter().enumerate() {
                    assert_eq!(
                        relation.from_hubuum_class_id,
                        relations[list_of_expected_ids[index]].from_hubuum_class_id
                    );
                }
            }
            FilterField::ClassTo => {
                for (index, relation) in result.iter().enumerate() {
                    assert_eq!(
                        relation.to_hubuum_class_id,
                        relations[list_of_expected_ids[index]].to_hubuum_class_id
                    );
                }
            }
            _ => {}
        }

        namespace.delete(&pool).await.unwrap();
    }
}
