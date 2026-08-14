use hubuum_storage_core::AuthenticationTokenScope;

use crate::errors::ApiError;
use crate::models::{
    CollectionID, HubuumClassID, HubuumObjectID, Permissions, TokenResourceScope, TokenScope,
};

pub(crate) fn token_scope_from_storage(
    scope: AuthenticationTokenScope,
) -> Result<TokenScope, ApiError> {
    let (permissions, resources) = scope.into_parts();
    let permissions = permissions
        .map(|permissions| {
            permissions
                .into_iter()
                .map(|permission| Permissions::from_string(&permission))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    let resources = resources
        .map(|resources| {
            let (collections, classes, objects) = resources.into_parts();
            collections
                .into_iter()
                .map(|id| CollectionID::new(id).map(TokenResourceScope::Collection))
                .chain(
                    classes
                        .into_iter()
                        .map(|id| HubuumClassID::new(id).map(TokenResourceScope::Class)),
                )
                .chain(
                    objects
                        .into_iter()
                        .map(|id| HubuumObjectID::new(id).map(TokenResourceScope::Object)),
                )
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?;
    TokenScope::from_stored_parts(permissions, resources)
}
