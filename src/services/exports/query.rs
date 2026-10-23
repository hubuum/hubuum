use super::*;

impl<'a, C, S> PermissionAwareExport<'a, C, S>
where
    C: AuthorizationContext,
    S: crate::traits::Search + ?Sized,
{
    pub(super) async fn new(
        backend: &'a C,
        subject: &'a S,
        scopes: Option<&'a TokenScope>,
    ) -> Result<Self, ApiError> {
        let authorization = match backend.authorization_mode() {
            AuthorizationMode::Delegated(permission_backend)
                if !permission_backend.supports_storage_visibility_filtering() =>
            {
                ExportAuthorization::External {
                    backend: permission_backend,
                    principal: PrincipalRef::load(backend, subject).await?,
                }
            }
            AuthorizationMode::LocalStorage | AuthorizationMode::Delegated(_) => {
                ExportAuthorization::LocalSql {
                    is_admin: subject.is_admin(backend).await?,
                }
            }
        };
        Ok(Self {
            backend,
            subject,
            scopes,
            authorization,
        })
    }

    pub(super) fn pool(&self) -> &C {
        self.backend
    }

    pub(super) fn external_backend(
        &self,
    ) -> Option<(&dyn crate::permissions::PermissionBackend, &PrincipalRef)> {
        self.authorization.external()
    }

    pub(super) async fn authorize_candidates<T>(
        &self,
        candidates: Vec<T>,
        resources: Vec<ResourceRef>,
        permissions: PermissionsList,
    ) -> Result<Vec<T>, ApiError> {
        let candidates = AuthorizationCandidates::new(candidates, resources)?;
        if !scope_allows(self.scopes, permissions.as_slice()) {
            return Ok(Vec::new());
        }
        let Some((backend, principal)) = self.external_backend() else {
            return Ok(candidates.into_values());
        };
        let scoped = candidates.retain_scoped(self.scopes);
        let requests = scoped.permission_requests(&permissions);
        let decisions = backend.authorize_many(principal, requests).await?;
        scoped.retain_decisions(decisions)
    }

    pub(super) async fn target_resources<T: AuthzTarget>(
        &self,
        candidates: &[T],
    ) -> Result<Vec<crate::permissions::ResourceRef>, ApiError> {
        let mut resources = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            resources.push(candidate.to_resource_ref(self.pool()).await?);
        }
        Ok(resources)
    }

    pub(super) async fn authorized_object_graph(
        &self,
        paths: &[Vec<i32>],
        object_permissions: PermissionsList,
        relation_permissions: PermissionsList,
    ) -> Result<AuthorizedObjectGraph, ApiError> {
        let mut object_ids = paths
            .iter()
            .flat_map(|path| path.iter().copied())
            .collect::<Vec<_>>();
        object_ids.sort_unstable();
        object_ids.dedup();

        let allowed_edges = self
            .object_relations_between_ids_with_permissions(&object_ids, relation_permissions)
            .await?
            .into_iter()
            .map(|relation| {
                ObjectGraphEdge::new(relation.from_hubuum_object_id, relation.to_hubuum_object_id)
            })
            .collect::<Result<HashSet<_>, _>>()?;

        let object_resources = object_authorization_resources(self.pool(), &object_ids).await?;
        let allowed_object_ids = self
            .authorize_candidates(object_ids, object_resources, object_permissions)
            .await?
            .into_iter()
            .collect::<HashSet<_>>();

        Ok(AuthorizedObjectGraph {
            object_ids: allowed_object_ids,
            edges: allowed_edges,
        })
    }

    pub(super) async fn collections(
        &self,
        mut query: QueryOptions,
    ) -> Result<Vec<Collection>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            query.set_include_total(false);
            return catalog_service::list_collections(
                self.pool(),
                self.subject.principal_id(),
                is_admin,
                self.scopes,
                query,
            )
            .await
            .map(|(rows, _)| rows);
        }
        let permissions = query_permissions(&query, &[Permissions::ReadCollection])?;
        authorized_storage_page(
            &query,
            async |options| {
                catalog_service::list_collections(
                    self.pool(),
                    self.subject.principal_id(),
                    true,
                    self.scopes,
                    options,
                )
                .await
                .map(|(rows, _)| rows)
            },
            async |candidates: Vec<Collection>| {
                let resources = self.target_resources(&candidates).await?;
                self.authorize_candidates(candidates, resources, permissions.clone())
                    .await
            },
        )
        .await
    }

    pub(super) async fn classes(
        &self,
        mut query: QueryOptions,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            query.set_include_total(false);
            return catalog_service::list_classes(
                self.pool(),
                self.subject.principal_id(),
                is_admin,
                self.scopes,
                query,
            )
            .await
            .map(|(rows, _)| rows);
        }
        let permissions = query_permissions(&query, &[Permissions::ReadClass])?;
        authorized_storage_page(
            &query,
            async |options| {
                catalog_service::list_classes(
                    self.pool(),
                    self.subject.principal_id(),
                    true,
                    self.scopes,
                    options,
                )
                .await
                .map(|(rows, _)| rows)
            },
            async |candidates: Vec<HubuumClassExpanded>| {
                let resources = candidates
                    .iter()
                    .map(|class| {
                        ResourceRef::class(class.id, class.collection.id, Some(class.name.clone()))
                    })
                    .collect();
                self.authorize_candidates(candidates, resources, permissions.clone())
                    .await
            },
        )
        .await
    }

    pub(super) async fn objects(
        &self,
        mut query: QueryOptions,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        let observed_query = query.clone();
        storage_handle(self.backend)
            .observe_query(&observed_query, None, async {
                if let Some(is_admin) = self.authorization.local_is_admin() {
                    query.set_include_total(false);
                    return catalog_service::list_objects(
                        self.pool(),
                        self.subject.principal_id(),
                        is_admin,
                        self.scopes,
                        query,
                    )
                    .await
                    .map(|(rows, _)| rows);
                }
                let permissions = query_permissions(&query, &[Permissions::ReadObject])?;
                authorized_storage_page(
                    &query,
                    async |options| {
                        catalog_service::list_objects(
                            self.pool(),
                            self.subject.principal_id(),
                            true,
                            self.scopes,
                            options,
                        )
                        .await
                        .map(|(rows, _)| rows)
                    },
                    async |candidates: Vec<HubuumObject>| {
                        let resources = self.target_resources(&candidates).await?;
                        self.authorize_candidates(candidates, resources, permissions.clone())
                            .await
                    },
                )
                .await
            })
            .await
    }

    pub(super) async fn class_relations(
        &self,
        query: QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            return relation_queries::list_class_relations(
                self.pool(),
                relation_queries::RelationAccess::new(
                    self.subject.principal_id(),
                    is_admin,
                    self.scopes,
                ),
                query,
            )
            .await
            .map(|(rows, _)| rows);
        }
        let permissions = query_permissions(&query, &[Permissions::ReadClassRelation])?;
        authorized_storage_page(
            &query,
            async |options| {
                relation_queries::list_class_relations(
                    self.pool(),
                    relation_queries::RelationAccess::new(
                        self.subject.principal_id(),
                        true,
                        self.scopes,
                    ),
                    options,
                )
                .await
                .map(|(rows, _)| rows)
            },
            async |candidates: Vec<HubuumClassRelation>| {
                let resources =
                    class_relation_authorization_resources(self.pool(), &candidates).await?;
                self.authorize_candidates(candidates, resources, permissions.clone())
                    .await
            },
        )
        .await
    }

    pub(super) async fn class_relations_touching_class_ids(
        &self,
        class_ids: &[i32],
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            return relation_queries::list_class_relations_touching_ids(
                self.pool(),
                relation_queries::RelationAccess::new(
                    self.subject.principal_id(),
                    is_admin,
                    self.scopes,
                ),
                class_ids,
            )
            .await;
        }
        let candidates = relation_queries::list_class_relations_touching_ids(
            self.pool(),
            relation_queries::RelationAccess::new(self.subject.principal_id(), true, None),
            class_ids,
        )
        .await?;
        let resources = class_relation_authorization_resources(self.pool(), &candidates).await?;
        self.authorize_candidates(
            candidates,
            resources,
            PermissionsList::new([Permissions::ReadClassRelation]),
        )
        .await
    }

    pub(super) async fn object_relations(
        &self,
        query: QueryOptions,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            return relation_queries::list_object_relations(
                self.pool(),
                relation_queries::RelationAccess::new(
                    self.subject.principal_id(),
                    is_admin,
                    self.scopes,
                ),
                query,
            )
            .await
            .map(|(rows, _)| rows);
        }
        let permissions = query_permissions(&query, &[Permissions::ReadObjectRelation])?;
        authorized_storage_page(
            &query,
            async |options| {
                relation_queries::list_object_relations(
                    self.pool(),
                    relation_queries::RelationAccess::new(
                        self.subject.principal_id(),
                        true,
                        self.scopes,
                    ),
                    options,
                )
                .await
                .map(|(rows, _)| rows)
            },
            async |candidates: Vec<HubuumObjectRelation>| {
                let resources =
                    object_relation_authorization_resources(self.pool(), &candidates).await?;
                self.authorize_candidates(candidates, resources, permissions.clone())
                    .await
            },
        )
        .await
    }

    pub(super) async fn related_objects(
        &self,
        object: HubuumObjectID,
        query: QueryOptions,
    ) -> Result<Vec<RelatedObjectGraphRow>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            return relation_queries::list_related_objects(
                self.pool(),
                relation_queries::RelationAccess::new(
                    self.subject.principal_id(),
                    is_admin,
                    self.scopes,
                ),
                object.id(),
                query,
            )
            .await
            .map(|(rows, _)| rows);
        }
        let permissions = query_permissions(&query, &[Permissions::ReadObject])?;
        authorized_storage_page(
            &query,
            async |options| {
                relation_queries::list_related_objects(
                    self.pool(),
                    relation_queries::RelationAccess::new(
                        self.subject.principal_id(),
                        true,
                        self.scopes,
                    ),
                    object.id(),
                    options,
                )
                .await
                .map(|(rows, _)| rows)
            },
            async |candidates: Vec<RelatedObjectGraphRow>| {
                let authorized_graph = self
                    .authorized_object_graph(
                        &candidates
                            .iter()
                            .map(|row| row.path.clone())
                            .collect::<Vec<_>>(),
                        permissions.clone(),
                        PermissionsList::new([Permissions::ReadObjectRelation]),
                    )
                    .await?;
                authorized_graph.retain_allowed(candidates, |candidate| &candidate.path)
            },
        )
        .await
    }

    pub(super) async fn related_objects_for_roots(
        &self,
        root_ids: &[i32],
        include: ExportIncludeRelatedQuery,
    ) -> Result<Vec<RelatedObjectIncludeRow>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            return relation_queries::list_related_objects_for_roots(
                self.pool(),
                relation_queries::RelationAccess::new(
                    self.subject.principal_id(),
                    is_admin,
                    self.scopes,
                ),
                root_ids,
                include,
                false,
            )
            .await;
        }
        let candidates = relation_queries::list_related_objects_for_roots(
            self.pool(),
            relation_queries::RelationAccess::new(self.subject.principal_id(), true, None),
            root_ids,
            ExportIncludeRelatedQuery {
                limit: i32::MAX,
                ..include
            },
            true,
        )
        .await?;
        let authorized_graph = self
            .authorized_object_graph(
                &candidates
                    .iter()
                    .map(|row| row.path.clone())
                    .collect::<Vec<_>>(),
                PermissionsList::new([Permissions::ReadObject]),
                PermissionsList::new([Permissions::ReadObjectRelation]),
            )
            .await?;
        Ok(canonicalize_related_include_paths(
            authorized_graph.retain_allowed(candidates, |candidate| &candidate.path)?,
            include,
        ))
    }

    pub(super) async fn bidirectionally_related_objects_for_roots(
        &self,
        root_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            return relation_queries::list_bidirectionally_related_objects_for_roots(
                self.pool(),
                relation_queries::RelationAccess::new(
                    self.subject.principal_id(),
                    is_admin,
                    self.scopes,
                ),
                root_ids,
                max_depth,
                per_root_cap,
                false,
            )
            .await;
        }
        let candidates = relation_queries::list_bidirectionally_related_objects_for_roots(
            self.pool(),
            relation_queries::RelationAccess::new(self.subject.principal_id(), true, None),
            root_ids,
            max_depth,
            i32::MAX,
            true,
        )
        .await?;
        let authorized_graph = self
            .authorized_object_graph(
                &candidates
                    .iter()
                    .map(|row| row.path.clone())
                    .collect::<Vec<_>>(),
                PermissionsList::new([Permissions::ReadObject]),
                PermissionsList::new([Permissions::ReadObjectRelation]),
            )
            .await?;
        Ok(canonicalize_related_object_paths(
            authorized_graph.retain_allowed(candidates, |candidate| &candidate.path)?,
            per_root_cap,
        ))
    }

    pub(super) async fn list_object_relations_between_ids(
        &self,
        object_ids: &[i32],
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        self.object_relations_between_ids_with_permissions(
            object_ids,
            PermissionsList::new([Permissions::ReadObjectRelation]),
        )
        .await
    }

    pub(super) async fn object_relations_between_ids_with_permissions(
        &self,
        object_ids: &[i32],
        permissions: PermissionsList,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        if let Some(is_admin) = self.authorization.local_is_admin() {
            return relation_queries::list_object_relations_between_ids(
                self.pool(),
                relation_queries::RelationAccess::new(
                    self.subject.principal_id(),
                    is_admin,
                    self.scopes,
                ),
                object_ids,
                None,
            )
            .await;
        }
        let candidates = relation_queries::list_object_relations_between_ids(
            self.pool(),
            relation_queries::RelationAccess::new(self.subject.principal_id(), true, None),
            object_ids,
            None,
        )
        .await?;
        let resources = object_relation_authorization_resources(self.pool(), &candidates).await?;
        self.authorize_candidates(candidates, resources, permissions)
            .await
    }
}

pub(super) async fn execute_scope<C, S>(
    exporter: &PermissionAwareExport<'_, C, S>,
    scope: ValidatedExportScope,
    mut query_options: QueryOptions,
) -> Result<(Vec<serde_json::Value>, Vec<ExportWarning>, bool), ApiError>
where
    C: AuthorizationContext,
    S: crate::traits::Search + ?Sized,
{
    let pool = exporter.pool();
    let item_limit = query_options.limit().unwrap_or(1).saturating_sub(1).max(1);

    let data = match scope {
        ValidatedExportScope::Collections => {
            to_json_items(exporter.collections(query_options).await?)?
        }
        ValidatedExportScope::Classes => to_json_items(exporter.classes(query_options).await?)?,
        ValidatedExportScope::ObjectsInClass(class_id) => {
            push_exact_filter(&mut query_options, FilterField::ClassId, class_id.id())?;
            to_json_items(exporter.objects(query_options).await?)?
        }
        ValidatedExportScope::ClassRelations => {
            to_json_items(exporter.class_relations(query_options).await?)?
        }
        ValidatedExportScope::ObjectRelations => {
            to_json_items(exporter.object_relations(query_options).await?)?
        }
        ValidatedExportScope::RelatedObjects {
            class_id,
            object_id,
        } => {
            let object = object_id.instance(pool).await?;
            crate::can!(
                exporter.backend,
                exporter.subject,
                exporter.scopes,
                [Permissions::ReadObject],
                &object
            );
            check_if_object_in_class(pool, &class_id, &object_id).await?;
            let related = exporter.related_objects(object_id, query_options).await?;
            to_json_items(
                related
                    .into_iter()
                    .map(|relation| relation.to_descendant_object_with_path())
                    .collect::<Vec<_>>(),
            )?
        }
    };

    let (items, truncated) = truncate_items(data, item_limit);
    Ok((items, Vec::new(), truncated))
}

pub(super) async fn apply_export_includes<C, S>(
    exporter: &PermissionAwareExport<'_, C, S>,
    export: &ExportRequest,
    items: &mut [serde_json::Value],
) -> Result<(), ApiError>
where
    C: AuthorizationContext,
    S: crate::traits::Search + ?Sized,
{
    let Some(related_objects) = export
        .include
        .as_ref()
        .and_then(|include| include.related_objects.as_ref())
    else {
        return Ok(());
    };

    if related_objects.is_empty() || items.is_empty() {
        return Ok(());
    }

    let root_object_ids = export_item_ids(items)?;
    for alias in related_objects.keys() {
        initialize_related_alias(items, alias)?;
    }

    let item_indexes = root_object_ids
        .iter()
        .enumerate()
        .map(|(index, object_id)| (*object_id, index))
        .collect::<HashMap<i32, usize>>();
    for (alias, include) in related_objects {
        let max_depth = include
            .max_depth
            .unwrap_or(RELATED_INCLUDE_DEFAULT_MAX_DEPTH);
        let limit = include.limit.unwrap_or(RELATED_INCLUDE_DEFAULT_LIMIT);
        let direction = include
            .direction
            .unwrap_or(ExportIncludeRelatedDirection::Any);
        let sort = include.sort.unwrap_or(ExportIncludeRelatedSort::Path);
        let include_query = ExportIncludeRelatedQuery {
            class_id: include.class_id,
            class_relation_id: include.class_relation_id,
            direction,
            sort,
            max_depth,
            limit,
        };
        let related = exporter
            .related_objects_for_roots(&root_object_ids, include_query)
            .await?;

        for row in related {
            let Some(item_index) = item_indexes.get(&row.root_object_id) else {
                continue;
            };
            let related_object = row.to_descendant_object_with_path();
            let related_value = serde_json::to_value(related_object).map_err(ApiError::from)?;
            push_related_alias_value(&mut items[*item_index], alias, related_value)?;
        }
    }

    Ok(())
}

pub(super) fn export_item_ids(items: &[serde_json::Value]) -> Result<Vec<i32>, ApiError> {
    items
        .iter()
        .map(|item| {
            let id = item
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Export object item did not include integer id".to_string(),
                    )
                })?;
            i32::try_from(id).map_err(|_| {
                ApiError::InternalServerError(format!(
                    "Export object item id '{id}' is outside i32 range"
                ))
            })
        })
        .collect()
}

pub(super) fn initialize_related_alias(
    items: &mut [serde_json::Value],
    alias: &str,
) -> Result<(), ApiError> {
    for item in items {
        let related = related_object_mut(item)?;
        related.insert(alias.to_string(), serde_json::Value::Array(Vec::new()));
    }
    Ok(())
}

pub(super) fn push_related_alias_value(
    item: &mut serde_json::Value,
    alias: &str,
    value: serde_json::Value,
) -> Result<(), ApiError> {
    let related = related_object_mut(item)?;
    let Some(alias_value) = related.get_mut(alias) else {
        return Err(ApiError::InternalServerError(format!(
            "Related include alias '{alias}' was not initialized"
        )));
    };
    let Some(alias_values) = alias_value.as_array_mut() else {
        return Err(ApiError::InternalServerError(format!(
            "Related include alias '{alias}' is not an array"
        )));
    };
    alias_values.push(value);
    Ok(())
}

pub(super) fn related_object_mut(
    item: &mut serde_json::Value,
) -> Result<&mut serde_json::Map<String, serde_json::Value>, ApiError> {
    let Some(item_object) = item.as_object_mut() else {
        return Err(ApiError::InternalServerError(
            "Export object item was not a JSON object".to_string(),
        ));
    };

    let related = item_object
        .entry("related")
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
    related.as_object_mut().ok_or_else(|| {
        ApiError::InternalServerError(
            "Export object item related field was not an object".to_string(),
        )
    })
}

pub(super) fn push_exact_filter(
    query_options: &mut QueryOptions,
    field: FilterField,
    value: i32,
) -> Result<(), ApiError> {
    if query_options.filters().iter().any(|param| {
        *param.field() == field
            && matches!(
                *param.operator(),
                crate::models::search::SearchOperator::Equals { is_negated: false }
            )
            && param.value() == value.to_string()
    }) {
        return Ok(());
    }

    query_options.filters_mut().try_push(ParsedQueryParam::new(
        &field.to_string(),
        None,
        &value.to_string(),
    )?)?;
    Ok(())
}

pub(super) fn to_json_items<T: Serialize>(
    items: Vec<T>,
) -> Result<Vec<serde_json::Value>, ApiError> {
    items
        .into_iter()
        .map(|item| serde_json::to_value(item).map_err(ApiError::from))
        .collect()
}

pub(super) fn truncate_items(
    mut items: Vec<serde_json::Value>,
    limit: usize,
) -> (Vec<serde_json::Value>, bool) {
    if items.len() > limit {
        items.truncate(limit);
        (items, true)
    } else {
        (items, false)
    }
}
