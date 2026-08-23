/// Check whether a subject has permissions for a set of resources.
///
/// The scope argument is mandatory. Request handlers pass
/// `requestor.scopes()`; intentionally unscoped internal callers pass `None`.
#[macro_export]
macro_rules! can {
    ($context:expr, $subject:expr, $scopes:expr, [$($permission:expr),+], $($resource:expr),+) => {{
        #[allow(unused_imports)]
        use $crate::permissions::AuthzTarget as _;
        #[allow(unused_imports)]
        use $crate::traits::CollectionAccessors as _;

        let context = &$context;
        let resources = vec![
            $($resource.to_resource_ref(context).await?),+
        ];
        if !$crate::traits::scope_allows_resources($scopes, &resources) {
            return Err($crate::errors::ApiError::Forbidden("Permission denied".to_string()));
        }

        match $crate::permissions::AuthorizationContext::authorization_mode(context) {
            $crate::permissions::AuthorizationMode::Delegated(permission_backend) => {
                $crate::permissions::authorize_resources(
                    permission_backend,
                    context,
                    $subject,
                    $scopes,
                    vec![$($permission),+],
                    resources,
                )
                .await?
            }
            $crate::permissions::AuthorizationMode::LocalStorage => {
                $subject
                    .can(
                        context,
                        vec![$($permission),+],
                        vec![
                            $($resource.collection_id(context).await?),+
                        ],
                        $scopes,
                    )
                    .await?
            }
        }
    }};
}
