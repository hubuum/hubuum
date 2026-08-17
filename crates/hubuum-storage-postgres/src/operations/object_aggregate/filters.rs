macro_rules! apply_object_aggregate_source_filters {
    ($query:ident, $query_options:expr, $computed_filter_snapshot:expr) => {{
        let query_params = $query_options.filters().clone();
        for param in query_params {
            if param.field.computed_query().is_some() {
                let snapshot = $computed_filter_snapshot.ok_or_else(|| {
                    $crate::PostgresStorageError::internal(
                        "Computed object aggregate filter is missing its resolved query snapshot",
                    )
                })?;
                $query = $query.filter(
                    $crate::operations::computed_objects::query::computed_filter_predicate(
                        &param, snapshot,
                    )?,
                );
                continue;
            }
            match param.field {
                hubuum_query::FilterField::Id => {
                    $crate::postgres_integer_filter!($query, param, object_id)
                }
                hubuum_query::FilterField::Collections
                | hubuum_query::FilterField::CollectionId => {
                    $crate::postgres_integer_filter!($query, param, object_collection_id)
                }
                hubuum_query::FilterField::CreatedAt => {
                    $crate::postgres_datetime_filter!($query, param, object_created_at)
                }
                hubuum_query::FilterField::UpdatedAt => {
                    $crate::postgres_datetime_filter!($query, param, object_updated_at)
                }
                hubuum_query::FilterField::Name => {
                    $crate::postgres_string_filter!($query, param, object_name)
                }
                hubuum_query::FilterField::Description => {
                    $crate::postgres_string_filter!($query, param, object_description)
                }
                hubuum_query::FilterField::Classes | hubuum_query::FilterField::ClassId => {
                    $crate::postgres_integer_filter!($query, param, hubuum_class_id)
                }
                hubuum_query::FilterField::JsonData => {
                    $query = $query.filter($crate::operations::json_filter::json_predicate(
                        &param,
                        "hubuumobject.data",
                    )?);
                }
                hubuum_query::FilterField::Permissions => {}
                _ => {
                    return Err($crate::PostgresStorageError::bad_request(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }
    }};
}

pub(super) use apply_object_aggregate_source_filters;
