use super::*;
pub trait UserSearchBackend:
    SelfAccessors<User> + GroupAccessors + GroupMemberships + UserNamespaceAccessors
{
    async fn search_namespaces_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<Namespace>, ApiError> {
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};
        use crate::schema::permissions::dsl::{
            group_id, namespace_id as permissions_nid, permissions,
        };
        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching namespaces",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadCollection]);

        let mut base_query = if self.is_admin(pool).await? {
            namespaces.into_boxed()
        } else {
            let group_id_subquery = self.group_ids_subquery_from_backend();

            namespaces
                .filter(
                    namespace_id.eq_any(
                        permissions
                            .filter(group_id.eq_any(group_id_subquery))
                            .select(permissions_nid),
                    ),
                )
                .into_boxed()
        };

        for param in query_params {
            use crate::{date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::updated_at
                ),
                FilterField::Name => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::name
                ),
                FilterField::Description => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::description
                ),
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for namespaces",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, Namespace);

        with_connection(pool, |conn| {
            base_query
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })
    }

    async fn search_classes_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as hubuum_class_id, namespace_id as hubuum_classes_nid,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching classes",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadCollection]);

        let namespaces = self.namespaces(pool, &permissions_list).await?;
        let namespace_ids: Vec<i32> = namespaces.iter().map(|n| n.id).collect();

        debug!(
            message = "Searching classes",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumclass
            .filter(hubuum_classes_nid.eq_any(namespace_ids))
            .into_boxed();

        let json_schema_queries = query_params.json_schemas()?;
        if !json_schema_queries.is_empty() {
            debug!(
                message = "Searching classes",
                stage = "JSON Schema",
                user_id = self.id(),
                query_params = ?json_schema_queries
            );

            let json_schema_integers = self.json_schema_subquery(pool, json_schema_queries)?;

            if json_schema_integers.is_empty() {
                debug!(
                    message = "Searching classes",
                    stage = "JSON Schema",
                    user_id = self.id(),
                    result = "No class IDs found, returning empty result"
                );
                return Ok(vec![]);
            }

            debug!(
                message = "Searching classes",
                stage = "JSON Schema",
                user_id = self.id(),
                result = "Found class IDs",
                class_ids = ?json_schema_integers
            );

            base_query = base_query.filter(hubuum_class_id.eq_any(json_schema_integers));
        }

        for param in query_params {
            use crate::{boolean_search, date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::id
                ),
                FilterField::Namespaces => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::namespace_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::updated_at
                ),
                FilterField::Name => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::name
                ),
                FilterField::Description => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::description
                ),
                FilterField::ValidateSchema => boolean_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::validate_schema
                ),
                FilterField::JsonSchema => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for classes",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumClassExpanded);

        trace_query!(base_query, "Searching classes");

        let result = with_connection(pool, |conn| {
            base_query
                .select(hubuumclass::all_columns())
                .distinct()
                .load::<HubuumClass>(conn)
        })?;

        let namespace_map: std::collections::HashMap<i32, Namespace> =
            namespaces.into_iter().map(|n| (n.id, n)).collect();

        Ok(result.expand_namespace_from_map(&namespace_map))
    }

    async fn search_objects_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as hubuum_object_id, namespace_id as hubuum_object_nid,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching objects",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permission_list = query_params.permissions()?;
        permission_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadCollection]);

        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permission_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching objects",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumobject
            .filter(hubuum_object_nid.eq_any(namespace_ids))
            .into_boxed();

        let json_data_queries = query_params.json_datas(FilterField::JsonData)?;
        if !json_data_queries.is_empty() {
            debug!(
                message = "Searching objects",
                stage = "JSON Data",
                user_id = self.id(),
                query_params = ?json_data_queries
            );

            let json_data_integers = self.json_data_subquery(pool, json_data_queries)?;
            if json_data_integers.is_empty() {
                debug!(
                    message = "Searching objects",
                    stage = "JSON Data",
                    user_id = self.id(),
                    result = "No object IDs found, returning empty result"
                );
                return Ok(vec![]);
            }

            debug!(
                message = "Searching objects",
                stage = "JSON Data",
                user_id = self.id(),
                result = "Found object IDs",
                class_ids = ?json_data_integers
            );

            base_query = base_query.filter(hubuum_object_id.eq_any(json_data_integers));
        }

        for param in query_params {
            use crate::{date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::id
                ),
                FilterField::Namespaces => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::namespace_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::updated_at
                ),
                FilterField::Name => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::name
                ),
                FilterField::Description => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::description
                ),
                FilterField::Classes => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::hubuum_class_id
                ),
                FilterField::ClassId => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::hubuum_class_id
                ),
                FilterField::JsonData => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumObject);

        trace_query!(base_query, "Searching objects");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumobject::all_columns())
                .distinct()
                .load::<HubuumObject>(conn)
        })
    }

    async fn search_class_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{
            from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching class relations",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut query_params = query_params;
        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClassRelation]);

        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permissions_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching class relations",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumclass_relation.into_boxed();

        for param in &[FilterField::ClassFromName, FilterField::ClassToName] {
            if let Some(class_param) = query_params.iter().find(|p| &p.field == param) {
                let qparam = ParsedQueryParam {
                    field: FilterField::Name,
                    operator: class_param.operator.clone(),
                    value: class_param.value.clone(),
                };
                let query_options = QueryOptions {
                    filters: vec![qparam],
                    sort: vec![],
                    limit: None,
                    cursor: None,
                };
                let classes = self
                    .search_classes_from_backend(pool, query_options)
                    .await?;
                let class_ids: Vec<i32> = classes.iter().map(|c| c.id).collect();

                if class_ids.is_empty() {
                    debug!(
                        message = "Searching class relations with class names",
                        stage = "Class IDs",
                        user_id = self.id(),
                        result = "No class IDs found, returning empty result"
                    );
                    return Ok(vec![]);
                }

                debug!(
                    message = "Searching class relations with class names",
                    stage = "Class IDs",
                    user_id = self.id(),
                    result = "Found class IDs",
                    class_ids = ?class_ids
                );

                let field = match param {
                    FilterField::ClassFromName => FilterField::ClassFrom,
                    FilterField::ClassToName => FilterField::ClassTo,
                    _ => unreachable!(),
                };

                query_params.push(ParsedQueryParam {
                    field,
                    operator: SearchOperator::Equals { is_negated: false },
                    value: class_ids
                        .iter()
                        .map(|item| item.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                });
            }
        }

        base_query = base_query
            .filter(
                from_hubuum_class_id.eq_any(
                    crate::schema::hubuumclass::dsl::hubuumclass
                        .select(crate::schema::hubuumclass::id)
                        .filter(crate::schema::hubuumclass::namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .filter(
                to_hubuum_class_id.eq_any(
                    crate::schema::hubuumclass::dsl::hubuumclass
                        .select(crate::schema::hubuumclass::id)
                        .filter(crate::schema::hubuumclass::namespace_id.eq_any(&namespace_ids)),
                ),
            );

        for param in query_params {
            use crate::{date_search, numeric_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::id
                ),
                FilterField::ClassFrom => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::from_hubuum_class_id
                ),
                FilterField::ClassTo => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::to_hubuum_class_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::updated_at
                ),
                FilterField::ClassFromName => {}
                FilterField::ClassToName => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for class relations",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(base_query, "Searching class relations");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
        })
    }

    async fn search_object_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{
            from_hubuum_object_id, hubuumobject_relation, to_hubuum_object_id,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching object relations",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadObjectRelation]);

        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permissions_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching object relations",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumobject_relation.into_boxed();

        base_query = base_query
            .filter(
                from_hubuum_object_id.eq_any(
                    crate::schema::hubuumobject::dsl::hubuumobject
                        .select(crate::schema::hubuumobject::id)
                        .filter(crate::schema::hubuumobject::namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .filter(
                to_hubuum_object_id.eq_any(
                    crate::schema::hubuumobject::dsl::hubuumobject
                        .select(crate::schema::hubuumobject::id)
                        .filter(crate::schema::hubuumobject::namespace_id.eq_any(&namespace_ids)),
                ),
            );

        for param in query_params {
            use crate::{date_search, numeric_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::id
                ),
                FilterField::ClassRelation => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::class_relation_id
                ),
                FilterField::ObjectFrom => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::from_hubuum_object_id
                ),
                FilterField::ObjectTo => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::to_hubuum_object_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::updated_at
                ),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for object relations",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumObjectRelation);

        trace_query!(base_query, "Searching object relations");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumobject_relation::all_columns())
                .distinct()
                .load::<HubuumObjectRelation>(conn)
        })
    }

    async fn search_objects_related_to_from_backend<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
    ) -> Result<Vec<ObjectClosureRow>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        use crate::schema::hubuumobject;
        use crate::schema::hubuumobject_closure::dsl as obj_closure;
        use diesel::alias;

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching objects related to object",
            stage = "Starting",
            user_id = self.id(),
            object_id = object.id(),
            query_params = ?query_params
        );

        let object_param = ParsedQueryParam::new(
            &FilterField::ObjectFrom.to_string(),
            Some(SearchOperator::Equals { is_negated: false }),
            &object.id().to_string(),
        )?;

        let mut query_params = query_params;
        query_params.push(object_param);

        debug!(
            message = "Searching object relations related to object",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list
            .ensure_contains(&[Permissions::ReadObject, Permissions::ReadObjectRelation]);

        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permissions_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        if namespace_ids.is_empty() {
            debug!(
                message = "Searching object relations related to object",
                stage = "Namespace IDs",
                user_id = self.id(),
                result = "No namespace IDs found, returning empty result"
            );
            return Ok(vec![]);
        }

        debug!(
            message = "Searching object relations related to object",
            stage = "Namespace IDs",
            user_id = self.id(),
            result = "Found namespace IDs",
            namespace_ids = ?namespace_ids
        );

        let (ancestor_object, descendant_object) = alias!(
            crate::schema::hubuumobject as ancestor_object,
            crate::schema::hubuumobject as descendant_object,
        );

        let mut base_query =
            obj_closure::hubuumobject_closure
                .inner_join(ancestor_object.on(
                    obj_closure::ancestor_object_id.eq(ancestor_object.field(hubuumobject::id)),
                ))
                .inner_join(descendant_object.on(
                    obj_closure::descendant_object_id.eq(descendant_object.field(hubuumobject::id)),
                ))
                .into_boxed();
        base_query = base_query
            .filter(
                ancestor_object
                    .field(hubuumobject::namespace_id)
                    .eq_any(&namespace_ids),
            )
            .filter(
                descendant_object
                    .field(hubuumobject::namespace_id)
                    .eq_any(&namespace_ids),
            );

        for param in &query_params {
            use crate::{array_search, date_search, json_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match &param.field {
                FilterField::ObjectFrom => {
                    numeric_search!(base_query, param, operator, obj_closure::ancestor_object_id)
                }
                FilterField::Id | FilterField::ObjectTo => {
                    numeric_search!(
                        base_query,
                        param,
                        operator,
                        obj_closure::descendant_object_id
                    )
                }
                FilterField::ClassFrom => {
                    numeric_search!(
                        base_query,
                        param,
                        operator,
                        ancestor_object.field(hubuumobject::hubuum_class_id)
                    )
                }
                FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => {
                    numeric_search!(
                        base_query,
                        param,
                        operator,
                        descendant_object.field(hubuumobject::hubuum_class_id)
                    )
                }
                FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
                    numeric_search!(
                        base_query,
                        param,
                        operator,
                        descendant_object.field(hubuumobject::namespace_id)
                    )
                }
                FilterField::NamespacesFrom => {
                    numeric_search!(
                        base_query,
                        param,
                        operator,
                        ancestor_object.field(hubuumobject::namespace_id)
                    )
                }
                FilterField::Name | FilterField::NameTo => {
                    string_search!(
                        base_query,
                        param,
                        operator,
                        descendant_object.field(hubuumobject::name)
                    )
                }
                FilterField::NameFrom => {
                    string_search!(
                        base_query,
                        param,
                        operator,
                        ancestor_object.field(hubuumobject::name)
                    )
                }
                FilterField::Description | FilterField::DescriptionTo => {
                    string_search!(
                        base_query,
                        param,
                        operator,
                        descendant_object.field(hubuumobject::description)
                    )
                }
                FilterField::DescriptionFrom => {
                    string_search!(
                        base_query,
                        param,
                        operator,
                        ancestor_object.field(hubuumobject::description)
                    )
                }
                FilterField::CreatedAt | FilterField::CreatedAtTo => {
                    date_search!(
                        base_query,
                        param,
                        operator,
                        descendant_object.field(hubuumobject::created_at)
                    )
                }
                FilterField::CreatedAtFrom => {
                    date_search!(
                        base_query,
                        param,
                        operator,
                        ancestor_object.field(hubuumobject::created_at)
                    )
                }
                FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
                    date_search!(
                        base_query,
                        param,
                        operator,
                        descendant_object.field(hubuumobject::updated_at)
                    )
                }
                FilterField::UpdatedAtFrom => {
                    date_search!(
                        base_query,
                        param,
                        operator,
                        ancestor_object.field(hubuumobject::updated_at)
                    )
                }
                FilterField::JsonDataFrom => {
                    json_search!(
                        base_query,
                        query_params,
                        FilterField::JsonDataFrom,
                        obj_closure::ancestor_object_id,
                        self,
                        pool
                    )
                }
                FilterField::JsonDataTo => {
                    json_search!(
                        base_query,
                        query_params,
                        FilterField::JsonDataTo,
                        obj_closure::descendant_object_id,
                        self,
                        pool
                    )
                }
                FilterField::Depth => {
                    numeric_search!(base_query, param, operator, obj_closure::depth)
                }
                FilterField::Path => array_search!(base_query, param, operator, obj_closure::path),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for object relations",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, ObjectClosureRow);

        trace_query!(base_query, "Searching object relations");

        with_connection(pool, |conn| {
            base_query
                .select((
                    obj_closure::ancestor_object_id,
                    obj_closure::descendant_object_id,
                    obj_closure::depth,
                    diesel::dsl::sql::<diesel::sql_types::Array<diesel::sql_types::Integer>>(
                        "hubuumobject_closure.path",
                    ),
                    ancestor_object.field(hubuumobject::name),
                    descendant_object.field(hubuumobject::name),
                    ancestor_object.field(hubuumobject::namespace_id),
                    descendant_object.field(hubuumobject::namespace_id),
                    ancestor_object.field(hubuumobject::hubuum_class_id),
                    descendant_object.field(hubuumobject::hubuum_class_id),
                    ancestor_object.field(hubuumobject::description),
                    descendant_object.field(hubuumobject::description),
                    ancestor_object.field(hubuumobject::data),
                    descendant_object.field(hubuumobject::data),
                    ancestor_object.field(hubuumobject::created_at),
                    descendant_object.field(hubuumobject::created_at),
                    ancestor_object.field(hubuumobject::updated_at),
                    descendant_object.field(hubuumobject::updated_at),
                ))
                .distinct()
                .load::<ObjectClosureRow>(conn)
        })
    }
}

impl<T: ?Sized> UserSearchBackend for T where
    T: SelfAccessors<User> + GroupAccessors + GroupMemberships + UserNamespaceAccessors
{
}

impl User {
    pub async fn search_users(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<User>, ApiError> {
        use crate::schema::users::dsl::{created_at, email, id, updated_at, username, users};

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching users",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut base_query = users.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, username),
                FilterField::Username => string_search!(base_query, param, operator, username),
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, User);

        trace_query!(base_query, "Searching users");

        let result = with_connection(pool, |conn| {
            base_query
                .select(users::all_columns())
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<User>(conn)
        })?;

        Ok(result)
    }

    pub async fn search_groups(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::{
            created_at, description, groupname, groups, id, updated_at,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching groups",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut base_query = groups.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, groupname),
                FilterField::Groupname => string_search!(base_query, param, operator, groupname),
                FilterField::Description => {
                    string_search!(base_query, param, operator, description)
                }
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for groups",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, Group);

        trace_query!(base_query, "Searching groups");

        let result = with_connection(pool, |conn| {
            base_query
                .select(groups::all_columns())
                .distinct()
                .load::<Group>(conn)
        })?;

        Ok(result)
    }
}
