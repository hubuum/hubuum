//! Transitional application fixture API for user creation.
//!
//! Production user persistence lives in `hubuum-storage-postgres`. This trait
//! remains temporarily for integration fixtures that create users without
//! lifecycle events.

use hubuum_storage_core::{StorageError, StorageUser, StorageUserCreate};

use crate::errors::ApiError;
use crate::models::{NewUser, User};
use crate::storage::postgres::PostgresPool;

pub trait CreateUserRecord {
    async fn create_user_record_without_events(
        &self,
        pool: &PostgresPool,
    ) -> Result<User, ApiError>;
}

impl CreateUserRecord for NewUser {
    async fn create_user_record_without_events(
        &self,
        pool: &PostgresPool,
    ) -> Result<User, ApiError> {
        let runtime = hubuum_storage_postgres::PostgresRuntime::new(pool.clone());
        hubuum_storage_postgres::operations::user::create_user(
            &runtime,
            StorageUserCreate::new(
                self.identity_scope.clone(),
                self.name.clone(),
                self.password.clone(),
                self.proper_name.clone(),
                self.email.clone(),
                None,
            ),
        )
        .await
        .map_err(StorageError::from)
        .map_err(ApiError::from)
        .map(user_from_storage)
    }
}

fn user_from_storage(user: StorageUser) -> User {
    let (id, password, proper_name, email, created_at, updated_at, anonymized_at) =
        user.into_parts();
    User {
        id,
        kind: "human".to_string(),
        password,
        proper_name,
        email,
        created_at,
        updated_at,
        anonymized_at,
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::ApiError;
    use crate::events::EventContext;
    use crate::models::user::UserID;
    use crate::models::{Token, UpdateUser, User};
    use crate::services::authentication::authenticate_bearer_token;
    use crate::tests::{TestScope, create_user_with_params};

    async fn user_with_tokens(scope: &TestScope, label: &str) -> (User, Vec<Token>) {
        let user =
            create_user_with_params(&scope.pool, &scope.scoped_name(label), "initial-password")
                .await;
        let first = user.create_token(&scope.pool).await.unwrap();
        let second = user.create_token(&scope.pool).await.unwrap();
        (user, vec![first, second])
    }

    async fn assert_tokens_active(pool: &crate::storage::postgres::PostgresPool, tokens: &[Token]) {
        for token in tokens {
            authenticate_bearer_token(pool, token)
                .await
                .expect("token should be active");
        }
    }

    async fn assert_tokens_revoked(
        pool: &crate::storage::postgres::PostgresPool,
        tokens: &[Token],
    ) {
        for token in tokens {
            assert!(matches!(
                authenticate_bearer_token(pool, token).await,
                Err(ApiError::Unauthorized(_))
            ));
        }
    }

    #[actix_web::test]
    async fn setting_password_revokes_all_active_tokens() {
        let scope = TestScope::new();
        let (user, tokens) = user_with_tokens(&scope, "set_password_revokes").await;
        assert_tokens_active(&scope.pool, &tokens).await;

        user.set_password(&scope.pool, "replacement-password")
            .await
            .unwrap();

        assert_tokens_revoked(&scope.pool, &tokens).await;
        user.delete_without_events(&scope.pool).await.unwrap();
    }

    #[actix_web::test]
    async fn password_update_revokes_all_active_tokens() {
        let scope = TestScope::new();
        let (user, tokens) = user_with_tokens(&scope, "password_update_revokes").await;
        assert_tokens_active(&scope.pool, &tokens).await;
        let context = EventContext::user(
            crate::events::PrincipalId::new(user.id).expect("stored user id must be positive"),
            None,
            None,
        );

        UpdateUser {
            password: Some("replacement-password".to_string()),
            proper_name: None,
            email: None,
        }
        .save(UserID::new(user.id).unwrap(), &scope.pool, Some(&context))
        .await
        .unwrap();

        assert_tokens_revoked(&scope.pool, &tokens).await;
        user.delete_without_events(&scope.pool).await.unwrap();
    }

    #[actix_web::test]
    async fn profile_update_preserves_active_tokens() {
        let scope = TestScope::new();
        let (user, tokens) = user_with_tokens(&scope, "profile_update_preserves").await;
        let context = EventContext::user(
            crate::events::PrincipalId::new(user.id).expect("stored user id must be positive"),
            None,
            None,
        );

        UpdateUser {
            password: None,
            proper_name: Some("Updated Name".to_string()),
            email: None,
        }
        .save(UserID::new(user.id).unwrap(), &scope.pool, Some(&context))
        .await
        .unwrap();

        assert_tokens_active(&scope.pool, &tokens).await;
        user.delete_without_events(&scope.pool).await.unwrap();
    }
}
