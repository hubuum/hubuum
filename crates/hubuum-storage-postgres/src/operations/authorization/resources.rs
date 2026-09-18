use crate::schema::{
    collections, export_templates, hubuumclass, hubuumclass_relation, hubuumobject,
    hubuumobject_relation, remote_targets,
};
use crate::{PostgresRuntime, PostgresStorageError};
use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::{
    ClassId, ClassRelationId, CollectionId, ExportTemplateId, ObjectId, ObjectRelationId,
    RemoteTargetId,
};
use hubuum_storage_core::{
    StorageAuthorizationClassResource as Class, StorageAuthorizationObjectResource as Object,
    StorageAuthorizationResource as R, StorageAuthorizationResourceKey as K,
    StorageAuthorizationResourcesQuery,
};

/// At most one narrow query per referenced resource kind, independent of page size.
pub async fn load_authorization_resources(
    runtime: &PostgresRuntime,
    query: StorageAuthorizationResourcesQuery,
) -> Result<Vec<R>, PostgresStorageError> {
    if query.keys().is_empty() {
        return Ok(Vec::new());
    }
    runtime
        .with_read_connection(async |connection| {
            let mut resources = Vec::new();
            macro_rules! ids {
                ($kind:ident) => {
                    query
                        .keys()
                        .iter()
                        .filter_map(|key| {
                            if let K::$kind(id) = key {
                                Some(id.id())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                };
            }
            let ids = ids!(Class);
            if !ids.is_empty() {
                for (id, collection_id, name) in hubuumclass::table
                    .filter(hubuumclass::id.eq_any(ids))
                    .select((
                        hubuumclass::id,
                        hubuumclass::collection_id,
                        hubuumclass::name,
                    ))
                    .load::<(i32, i32, String)>(connection)
                    .await?
                {
                    resources.push(R::Class {
                        resource: Class::new(ClassId::new(id)?, CollectionId::new(collection_id)?),
                        name,
                    });
                }
            }
            let ids = ids!(Object);
            if !ids.is_empty() {
                for (id, collection_id, class_id, name) in hubuumobject::table
                    .filter(hubuumobject::id.eq_any(ids))
                    .select((
                        hubuumobject::id,
                        hubuumobject::collection_id,
                        hubuumobject::hubuum_class_id,
                        hubuumobject::name,
                    ))
                    .load::<(i32, i32, i32, String)>(connection)
                    .await?
                {
                    resources.push(R::Object(Object::new(
                        ObjectId::new(id)?,
                        CollectionId::new(collection_id)?,
                        ClassId::new(class_id)?,
                        name,
                    )));
                }
            }
            let ids = ids!(Collection);
            if !ids.is_empty() {
                for (id, name) in collections::table
                    .filter(collections::id.eq_any(ids))
                    .select((collections::id, collections::name))
                    .load::<(i32, String)>(connection)
                    .await?
                {
                    resources.push(R::Collection {
                        id: CollectionId::new(id)?,
                        name,
                    });
                }
            }
            let ids = ids!(ExportTemplate);
            if !ids.is_empty() {
                for (id, collection_id, name) in export_templates::table
                    .filter(export_templates::id.eq_any(ids))
                    .select((
                        export_templates::id,
                        export_templates::collection_id,
                        export_templates::name,
                    ))
                    .load::<(i32, i32, String)>(connection)
                    .await?
                {
                    resources.push(R::ExportTemplate {
                        id: ExportTemplateId::new(id)?,
                        collection_id: CollectionId::new(collection_id)?,
                        name,
                    });
                }
            }
            let ids = ids!(RemoteTarget);
            if !ids.is_empty() {
                for (id, collection_id, name) in remote_targets::table
                    .filter(remote_targets::id.eq_any(ids))
                    .select((
                        remote_targets::id,
                        remote_targets::collection_id,
                        remote_targets::name,
                    ))
                    .load::<(i32, i32, String)>(connection)
                    .await?
                {
                    resources.push(R::RemoteTarget {
                        id: RemoteTargetId::new(id)?,
                        collection_id: CollectionId::new(collection_id)?,
                        name,
                    });
                }
            }
            let ids = ids!(ClassRelation);
            if !ids.is_empty() {
                let (from, to) = diesel::alias!(hubuumclass as from_class, hubuumclass as to_class);
                let rows = hubuumclass_relation::table
                    .inner_join(
                        from.on(from
                            .field(hubuumclass::id)
                            .eq(hubuumclass_relation::from_hubuum_class_id)),
                    )
                    .inner_join(
                        to.on(to
                            .field(hubuumclass::id)
                            .eq(hubuumclass_relation::to_hubuum_class_id)),
                    )
                    .filter(hubuumclass_relation::id.eq_any(ids))
                    .select((
                        hubuumclass_relation::id,
                        from.fields((hubuumclass::id, hubuumclass::collection_id)),
                        to.fields((hubuumclass::id, hubuumclass::collection_id)),
                    ))
                    .load::<(i32, (i32, i32), (i32, i32))>(connection)
                    .await?;
                for (id, (from_id, from_collection), (to_id, to_collection)) in rows {
                    resources.push(R::ClassRelation {
                        id: ClassRelationId::new(id)?,
                        from: Class::new(
                            ClassId::new(from_id)?,
                            CollectionId::new(from_collection)?,
                        ),
                        to: Class::new(ClassId::new(to_id)?, CollectionId::new(to_collection)?),
                    });
                }
            }
            let ids = ids!(ObjectRelation);
            if !ids.is_empty() {
                let (from, to) =
                    diesel::alias!(hubuumobject as from_object, hubuumobject as to_object);
                let rows = hubuumobject_relation::table
                    .inner_join(
                        from.on(from
                            .field(hubuumobject::id)
                            .eq(hubuumobject_relation::from_hubuum_object_id)),
                    )
                    .inner_join(
                        to.on(to
                            .field(hubuumobject::id)
                            .eq(hubuumobject_relation::to_hubuum_object_id)),
                    )
                    .filter(hubuumobject_relation::id.eq_any(ids))
                    .select((
                        hubuumobject_relation::id,
                        hubuumobject_relation::class_relation_id,
                        from.fields((
                            hubuumobject::id,
                            hubuumobject::collection_id,
                            hubuumobject::hubuum_class_id,
                            hubuumobject::name,
                        )),
                        to.fields((
                            hubuumobject::id,
                            hubuumobject::collection_id,
                            hubuumobject::hubuum_class_id,
                            hubuumobject::name,
                        )),
                    ))
                    .load::<(i32, i32, (i32, i32, i32, String), (i32, i32, i32, String))>(
                        connection,
                    )
                    .await?;
                for (
                    id,
                    class_relation_id,
                    (from_id, from_collection, from_class, from_name),
                    (to_id, to_collection, to_class, to_name),
                ) in rows
                {
                    resources.push(R::ObjectRelation {
                        id: ObjectRelationId::new(id)?,
                        class_relation_id: ClassRelationId::new(class_relation_id)?,
                        from: Object::new(
                            ObjectId::new(from_id)?,
                            CollectionId::new(from_collection)?,
                            ClassId::new(from_class)?,
                            from_name,
                        ),
                        to: Object::new(
                            ObjectId::new(to_id)?,
                            CollectionId::new(to_collection)?,
                            ClassId::new(to_class)?,
                            to_name,
                        ),
                    });
                }
            }
            Ok::<_, PostgresStorageError>(resources)
        })
        .await
}

/// Fetch only relevant direct grants once, preserving the single-check rule that
/// one group must grant every requested permission (never union partial grants).
pub async fn authorize_local_collection_batch(
    runtime: &PostgresRuntime,
    queries: Vec<hubuum_storage_core::StorageAuthorizationCollectionAccessQuery>,
) -> Result<Vec<bool>, PostgresStorageError> {
    use super::rows::PermissionRow;
    use crate::schema::{group_memberships, permissions};
    use diesel::SelectableHelper;
    use std::collections::{BTreeSet, HashMap};
    if queries.is_empty() {
        return Ok(Vec::new());
    }
    let principals = queries
        .iter()
        .map(|q| q.principal_id().id())
        .collect::<BTreeSet<_>>();
    let collections = queries
        .iter()
        .map(|q| q.collection_id().id())
        .collect::<BTreeSet<_>>();
    runtime
        .with_read_connection(async |connection| {
            let rows = permissions::table
                .inner_join(
                    group_memberships::table
                        .on(permissions::group_id.eq(group_memberships::group_id)),
                )
                .filter(group_memberships::principal_id.eq_any(principals))
                .filter(permissions::collection_id.eq_any(collections))
                .select((group_memberships::principal_id, PermissionRow::as_select()))
                .load::<(i32, PermissionRow)>(connection)
                .await?;
            let mut grants = HashMap::<_, Vec<_>>::new();
            for (principal, row) in rows {
                let collection = row.collection_id;
                grants
                    .entry((principal, collection))
                    .or_default()
                    .push(row.permissions());
            }
            Ok::<_, PostgresStorageError>(
                queries
                    .iter()
                    .map(|query| {
                        grants
                            .get(&(query.principal_id().id(), query.collection_id().id()))
                            .is_some_and(|rows| {
                                rows.iter().any(|grant| {
                                    query
                                        .permissions()
                                        .iter()
                                        .all(|permission| grant.contains(permission))
                                })
                            })
                    })
                    .collect(),
            )
        })
        .await
}
