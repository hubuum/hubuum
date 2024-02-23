#[macro_export]
/// Check permissions for a user on a namespace, class, or object.
///
/// ## Arguments
///
/// * `request_obj` - The request object (namespace, class, or object).
/// * `pool` - The database pool.
/// * `user` - The user to check permissions for (will be cloned).
/// * `permission+` - The permissions to check for, one or more.
///
/// ## Returns
///
/// This macro causes a return with a `ApiError::Forbidden` if the user does
/// not have the specified permission.
///
/// ## Example
///
/// ```
/// check_permissions!(namespace, pool, requestor.user, Permissions::ReadCollection);
/// check_permissions!(namespace, pool, requestor.user, Permissions::ReadCollection, Permissions::UpdateCollection);
///
/// ```
macro_rules! check_permissions {
    // Captures any number of permissions passed after the user argument and converts them into a vector
    ($request_obj:expr, $pool:expr, $user:expr, $($permissions:expr),+ $(,)?) => {{
        use $crate::errors::ApiError;
        use $crate::traits::NamespaceAccessors;
        use tracing::warn;

        let permissions_vec = vec![$($permissions),+];

        if !$request_obj
            .user_can_all(&$pool, $user.clone(), permissions_vec.clone())
            .await?
        {
            let namespace_id = $request_obj.namespace_id(&$pool).await?;
            let user_id = $user.id();
            warn!(
                message = "Permission denied",
                user_id = user_id,
                namespace_id = namespace_id,
                permissions = ?permissions_vec,
            );
            return Err(ApiError::Forbidden(format!(
                "User {} does not have permissions {:?} on namespace {}",
                user_id, permissions_vec, namespace_id
            )));
        }
    }};
}
