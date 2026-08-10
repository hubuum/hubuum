use crate::errors::ApiError;
use crate::models::{
    CollectionID, HubuumClassID, HubuumObjectID, TokenResourceScope, TokenScope, UserID,
};
use crate::storage::StorageVisibility;
use crate::storage::postgres::operations::authorization::permission_from_storage;

pub(super) fn principal(visibility: &StorageVisibility) -> Result<UserID, ApiError> {
    UserID::new(visibility.principal_id())
}

pub(super) fn token_scope(visibility: &StorageVisibility) -> Result<Option<TokenScope>, ApiError> {
    let permissions = visibility.permissions().map(|permissions| {
        permissions
            .iter()
            .copied()
            .map(permission_from_storage)
            .collect::<Vec<_>>()
    });
    let resources = visibility
        .resources()
        .map(|scope| {
            scope
                .collection_ids()
                .iter()
                .copied()
                .map(|id| CollectionID::new(id).map(TokenResourceScope::Collection))
                .chain(
                    scope
                        .class_ids()
                        .iter()
                        .copied()
                        .map(|id| HubuumClassID::new(id).map(TokenResourceScope::Class)),
                )
                .chain(
                    scope
                        .object_ids()
                        .iter()
                        .copied()
                        .map(|id| HubuumObjectID::new(id).map(TokenResourceScope::Object)),
                )
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;

    if permissions.is_none() && resources.is_none() {
        Ok(None)
    } else {
        TokenScope::from_stored_parts(permissions, resources).map(Some)
    }
}
