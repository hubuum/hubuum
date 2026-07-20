macro_rules! apply_object_aggregate_source_filters {
    ($query:ident, $query_options:expr, $computed_filter_snapshot:expr) => {{
        let query_params = $query_options.filters.clone();
        for param in query_params.json_datas(FilterField::JsonData)? {
            $query = $query.filter(param.as_json_predicate()?);
        }
        for param in query_params {
            if param.field.computed_query().is_some() {
                let snapshot = $computed_filter_snapshot.ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Computed object aggregate filter is missing its resolved query snapshot"
                            .to_string(),
                    )
                })?;
                $query = $query.filter(
                    crate::db::traits::computed_field::computed_filter_predicate(&param, snapshot)?,
                );
                continue;
            }
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => crate::numeric_search!($query, param, operator, object_id),
                FilterField::Collections | FilterField::CollectionId => {
                    crate::numeric_search!($query, param, operator, object_collection_id)
                }
                FilterField::CreatedAt => {
                    crate::date_search!($query, param, operator, object_created_at)
                }
                FilterField::UpdatedAt => {
                    crate::date_search!($query, param, operator, object_updated_at)
                }
                FilterField::Name => {
                    crate::string_search!($query, param, operator, object_name)
                }
                FilterField::Description => {
                    crate::string_search!($query, param, operator, object_description)
                }
                FilterField::Classes | FilterField::ClassId => {
                    crate::numeric_search!($query, param, operator, hubuum_class_id)
                }
                FilterField::JsonData | FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }
    }};
}

pub(super) use apply_object_aggregate_source_filters;
