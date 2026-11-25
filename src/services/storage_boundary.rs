use crate::errors::ApiError;
use crate::models::{
    Collection, HubuumClassExpanded, HubuumObject, PrincipalSettings, PrincipalSettingsPatch,
    PrincipalSettingsResponse, TokenResourceScope, TokenScope,
};
#[cfg(test)]
use crate::models::{HubuumClass, HubuumClassRelation, HubuumObjectRelation};
use crate::permissions::permission_to_storage;
#[cfg(test)]
use crate::storage::StorageObjectRelationCreate;
#[cfg(test)]
use crate::storage::{
    StorageClass, StorageClassRelation, StorageClassRelationCreate, StorageClassSchemaPolicy,
    StorageClassSelector, StorageCollection, StorageObjectRelation, StorageObjectSelector,
};
use crate::storage::{
    StorageClassWithCollection, StorageObject, StoragePrincipalSettings,
    StoragePrincipalSettingsMutation, StorageRecordMetadata, StorageResourceScope,
    StorageVisibility,
};
use hubuum_domain::{ClassId, CollectionId, PrincipalId, ResourceId};
#[cfg(test)]
use hubuum_domain::{ClassRelationId, ObjectId};

pub(super) fn visibility(
    principal_id: i32,
    is_admin: bool,
    scope: Option<&TokenScope>,
) -> Result<StorageVisibility, ApiError> {
    let permissions = scope.and_then(TokenScope::permissions).map(|permissions| {
        permissions
            .iter()
            .copied()
            .map(permission_to_storage)
            .collect::<Vec<_>>()
    });
    let resources = scope.map(resource_scope).transpose()?.flatten();
    Ok(StorageVisibility::new(
        PrincipalId::new(principal_id)?,
        is_admin,
        permissions,
        resources,
    ))
}

fn resource_scope(scope: &TokenScope) -> Result<Option<StorageResourceScope>, ApiError> {
    let Some(resources) = scope.resources()? else {
        return Ok(None);
    };
    let mut collection_ids = Vec::new();
    let mut class_ids = Vec::new();
    let mut object_ids = Vec::new();
    for resource in resources {
        match resource {
            TokenResourceScope::Collection(id) => collection_ids.push(id),
            TokenResourceScope::Class(id) => class_ids.push(id),
            TokenResourceScope::Object(id) => object_ids.push(id),
        }
    }
    Ok(Some(StorageResourceScope::new(
        collection_ids,
        class_ids,
        object_ids,
    )))
}

pub(crate) fn principal_settings_from_storage(
    row: StoragePrincipalSettings,
) -> Result<PrincipalSettingsResponse, ApiError> {
    let (principal_id, revision, document) = row.into_parts();
    Ok(PrincipalSettingsResponse::new(
        principal_id.id(),
        revision,
        PrincipalSettings::new(document)?,
    ))
}

pub(crate) fn principal_settings_mutation_to_storage(
    mutation: PrincipalSettingsPatch,
) -> Result<StoragePrincipalSettingsMutation, ApiError> {
    match mutation {
        PrincipalSettingsPatch::MergePatch(settings) => Ok(
            StoragePrincipalSettingsMutation::MergePatch(settings.as_value().clone()),
        ),
        PrincipalSettingsPatch::JsonPatch(document) => Ok(
            StoragePrincipalSettingsMutation::JsonPatch(serde_json::to_value(document)?),
        ),
    }
}

#[cfg(test)]
pub(crate) fn collection_to_storage(collection: Collection) -> StorageCollection {
    StorageCollection::try_new(
        StorageRecordMetadata::try_new(
            ResourceId::new(collection.id).expect("stored collection id must be positive"),
            collection.created_at.and_utc(),
            collection.updated_at.and_utc(),
            collection.revision,
        )
        .expect("stored collection timestamps must be ordered"),
        collection.name,
        collection.description,
        collection
            .parent_collection_id
            .map(|id| CollectionId::new(id).expect("stored parent collection id must be positive")),
    )
    .expect("stored collection hierarchy must satisfy the storage contract")
}

pub(super) fn class_from_storage(
    row: StorageClassWithCollection,
) -> Result<HubuumClassExpanded, ApiError> {
    let (
        id,
        name,
        collection,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        revision,
    ) = row.into_parts();
    Ok(HubuumClassExpanded {
        id: id.id(),
        name,
        collection: Collection::from(collection),
        json_schema,
        validate_schema,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        revision,
    })
}

#[cfg(test)]
pub(crate) fn class_record_to_storage(class: HubuumClass) -> Result<StorageClass, ApiError> {
    let schema_policy =
        StorageClassSchemaPolicy::try_from_parts(class.json_schema, class.validate_schema)
            .map_err(|error| ApiError::from(error.into_request_error()))?;
    Ok(StorageClass::builder(
        StorageRecordMetadata::try_new(
            ResourceId::new(class.id)?,
            class.created_at.and_utc(),
            class.updated_at.and_utc(),
            class.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        class.name,
        CollectionId::new(class.collection_id)?,
        class.description,
    )
    .schema_policy(schema_policy)
    .build())
}

pub(crate) fn object_to_storage(object: HubuumObject) -> Result<StorageObject, ApiError> {
    Ok(StorageObject::new(
        StorageRecordMetadata::try_new(
            ResourceId::new(object.id)?,
            object.created_at.and_utc(),
            object.updated_at.and_utc(),
            object.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        object.name,
        CollectionId::new(object.collection_id)?,
        ClassId::new(object.hubuum_class_id)?,
        object.data,
        object.description,
    ))
}

#[cfg(test)]
pub(crate) fn class_relation_to_storage(
    relation: HubuumClassRelation,
) -> Result<StorageClassRelation, ApiError> {
    Ok(StorageClassRelation::try_new(
        StorageRecordMetadata::try_new(
            ResourceId::new(relation.id.id())?,
            relation.created_at.and_utc(),
            relation.updated_at.and_utc(),
            relation.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        relation.from_hubuum_class_id,
        relation.to_hubuum_class_id,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?
    .with_labels(relation.forward_label, relation.reverse_label)
    .with_relation_details(
        relation.forward_template_alias,
        relation.reverse_template_alias,
        relation.from_max_relations,
        relation.to_max_relations,
    ))
}

#[cfg(test)]
pub(crate) fn object_relation_to_storage(
    relation: HubuumObjectRelation,
) -> Result<StorageObjectRelation, ApiError> {
    StorageObjectRelation::try_new(
        StorageRecordMetadata::try_new(
            ResourceId::new(relation.id)?,
            relation.created_at.and_utc(),
            relation.updated_at.and_utc(),
            relation.revision,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?,
        ObjectId::new(relation.from_hubuum_object_id)?,
        ObjectId::new(relation.to_hubuum_object_id)?,
        ClassRelationId::new(relation.class_relation_id)?,
    )
    .map_err(|error| ApiError::InternalServerError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use hubuum_domain::{ResourceId, ResourceRevision};
    use rstest::rstest;

    use super::*;
    use crate::models::{CollectionID, HubuumClassID, Permissions};
    use crate::models::{
        NewHubuumObject, PreparedClassRelation, PreparedObjectRelation,
        ResolvedClassRelationTarget, ResolvedClassTarget, ResolvedObjectRelationTarget,
        ResolvedObjectTarget, UpdateHubuumObject,
    };
    use crate::storage::{
        StorageObjectCreate, StorageObjectUpdate, StoragePreparedClassRelation,
        StoragePreparedObjectRelation, StorageResolvedClass, StorageResolvedClassRelation,
        StorageResolvedObject, StorageResolvedObjectRelation,
    };

    #[rstest]
    #[case(0, 1)]
    #[case(-1, 1)]
    #[case(1, 0)]
    #[case(1, -1)]
    fn object_creation_rejects_raw_invalid_ids(#[case] collection_id: i32, #[case] class_id: i32) {
        let result = StorageObjectCreate::try_from(NewHubuumObject {
            name: "invalid_ids".into(),
            collection_id,
            hubuum_class_id: class_id,
            data: serde_json::json!({}),
            description: String::new(),
        });
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
    }

    #[rstest]
    #[case(Some(0), None)]
    #[case(Some(-1), None)]
    #[case(None, Some(0))]
    #[case(None, Some(-1))]
    fn object_updates_reject_raw_invalid_ids(
        #[case] collection_id: Option<i32>,
        #[case] class_id: Option<i32>,
    ) {
        let result = StorageObjectUpdate::try_from(UpdateHubuumObject {
            name: None,
            collection_id,
            hubuum_class_id: class_id,
            data: None,
            description: None,
        });
        assert!(matches!(result, Err(ApiError::BadRequest(_))));
    }

    fn metadata(id: i32) -> StorageRecordMetadata {
        let timestamp = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        StorageRecordMetadata::try_new(
            ResourceId::new(id).unwrap(),
            timestamp,
            timestamp,
            ResourceRevision::INITIAL,
        )
        .unwrap()
    }

    fn stored_class(id: i32) -> StorageClass {
        StorageClass::builder(
            metadata(id),
            format!("class_{id}"),
            CollectionID::new(1).unwrap(),
            String::new(),
        )
        .build()
    }

    fn stored_object(id: i32) -> StorageObject {
        StorageObject::new(
            metadata(id),
            format!("object_{id}"),
            CollectionID::new(1).unwrap(),
            ClassId::new(id).unwrap(),
            serde_json::json!({"payload": "retained object data"}),
            String::new(),
        )
    }

    #[derive(Clone, Copy)]
    enum Aggregate {
        Class,
        Object,
        PreparedClassRelation,
        ClassRelation,
        PreparedObjectRelation,
        ObjectRelation,
    }

    #[rstest]
    #[case(Aggregate::Class)]
    #[case(Aggregate::Object)]
    #[case(Aggregate::PreparedClassRelation)]
    #[case(Aggregate::ClassRelation)]
    #[case(Aggregate::PreparedObjectRelation)]
    #[case(Aggregate::ObjectRelation)]
    fn application_wrappers_retain_the_original_aggregate(#[case] aggregate: Aggregate) {
        let from_class = stored_class(1);
        let to_class = stored_class(2);
        let original_name = from_class.name().as_ptr();
        let relation = || {
            StorageClassRelation::try_new(
                metadata(3),
                ClassId::new(1).unwrap(),
                ClassId::new(2).unwrap(),
            )
            .unwrap()
        };

        match aggregate {
            Aggregate::Class => {
                let stored = StorageResolvedClass::try_new(
                    StorageClassSelector::Name("class_1".into()),
                    from_class,
                )
                .unwrap();
                let target = ResolvedClassTarget::from(stored);
                assert_eq!(target.as_storage().class().name().as_ptr(), original_name);
            }
            Aggregate::Object => {
                let stored = StorageResolvedObject::try_new(
                    StorageObjectSelector::Names {
                        class_name: "class_1".into(),
                        object_name: "object_1".into(),
                    },
                    from_class,
                    stored_object(1),
                )
                .unwrap();
                let target = ResolvedObjectTarget::from(stored);
                assert_eq!(target.as_storage().class().name().as_ptr(), original_name);
            }
            Aggregate::PreparedClassRelation => {
                let stored = StoragePreparedClassRelation::try_new(
                    StorageClassRelationCreate::builder(
                        ClassId::new(1).unwrap(),
                        ClassId::new(2).unwrap(),
                    )
                    .build(),
                    from_class,
                    to_class,
                )
                .unwrap();
                let target = PreparedClassRelation::from(stored);
                assert_eq!(
                    target.as_storage().from_class().name().as_ptr(),
                    original_name
                );
            }
            Aggregate::ClassRelation => {
                let stored =
                    StorageResolvedClassRelation::try_new(relation(), from_class, to_class)
                        .unwrap();
                let target = ResolvedClassRelationTarget::from(stored);
                assert_eq!(
                    target.as_storage().from_class().name().as_ptr(),
                    original_name
                );
            }
            Aggregate::PreparedObjectRelation => {
                let class_relation =
                    StorageResolvedClassRelation::try_new(relation(), from_class, to_class)
                        .unwrap();
                let stored = StoragePreparedObjectRelation::try_new(
                    StorageObjectRelationCreate::new(
                        ObjectId::new(1).unwrap(),
                        ObjectId::new(2).unwrap(),
                        ClassRelationId::new(3).unwrap(),
                    ),
                    stored_object(1),
                    stored_object(2),
                    class_relation,
                )
                .unwrap();
                let target = PreparedObjectRelation::from(stored);
                assert_eq!(
                    target
                        .as_storage()
                        .class_relation()
                        .from_class()
                        .name()
                        .as_ptr(),
                    original_name
                );
            }
            Aggregate::ObjectRelation => {
                let class_relation =
                    StorageResolvedClassRelation::try_new(relation(), from_class, to_class)
                        .unwrap();
                let stored = StorageResolvedObjectRelation::try_new(
                    StorageObjectRelation::try_new(
                        metadata(4),
                        ObjectId::new(1).unwrap(),
                        ObjectId::new(2).unwrap(),
                        ClassRelationId::new(3).unwrap(),
                    )
                    .unwrap(),
                    stored_object(1),
                    stored_object(2),
                    class_relation,
                )
                .unwrap();
                let target = ResolvedObjectRelationTarget::from(stored);
                assert_eq!(
                    target
                        .as_storage()
                        .class_relation()
                        .from_class()
                        .name()
                        .as_ptr(),
                    original_name
                );
            }
        }
    }

    #[test]
    fn visibility_preserves_independent_token_dimensions() {
        let scope = TokenScope::from_stored_parts(
            Some(vec![Permissions::ReadCollection, Permissions::ReadClass]),
            Some(vec![
                TokenResourceScope::Collection(CollectionID::new(7).unwrap()),
                TokenResourceScope::Class(HubuumClassID::new(9).unwrap()),
            ]),
        )
        .unwrap();

        let visibility = visibility(42, false, Some(&scope)).unwrap();

        assert!(visibility.allows_permissions(&[
            crate::storage::StorageAuthorizationPermission::ReadCollection,
            crate::storage::StorageAuthorizationPermission::ReadClass,
        ]));
        let resources = visibility.resources().unwrap();
        assert_eq!(resources.collection_ids()[0].id(), 7);
        assert_eq!(resources.class_ids()[0].id(), 9);
        assert!(resources.object_ids().is_empty());
    }
}
