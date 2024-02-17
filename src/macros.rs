#[macro_export]
/// Check permissions for a user on a namespace, class, or object.
///
/// ## Arguments
///
/// * `request_obj` - The request object (namespace, class, or object).
/// * `pool` - The database pool.
/// * `user` - The user to check permissions for (will be cloned).
/// * `permission` - The permission to check for.
///
/// ## Returns
///
/// This macro causes a return with a `ApiError::Forbidden` if the user does
/// not have the specified permission.
///
/// ## Example
///
/// ```
///  check_permissions!(namespace, pool, requestor.user, Permissions::ReadCollection);
/// ```
macro_rules! check_permissions {
    ($request_obj:expr, $pool:expr, $user:expr, $permission:expr) => {{
        if !$request_obj
            .user_can(&$pool, $user.clone(), $permission)
            .await?
        {
            use crate::errors::ApiError;
            use crate::traits::NamespaceAccessors;
            use tracing::warn;
            let namespace_id = $request_obj.namespace_id(&$pool).await?;
            let user_id = $user.id();
            warn!(
                message = "Permission denied",
                user_id = user_id,
                namespace_id = namespace_id,
                permission = ?$permission,
            );
            return Err(ApiError::Forbidden(format!(
                "User {} does not have permission {} on namespace {}",
                user_id, $permission, namespace_id
            )));
        }
    }};
}
