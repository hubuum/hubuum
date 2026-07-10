use crate::db::traits::user::{
    AnonymizeUserRecord, CreateUserRecord, DeleteUserRecord, OwnedUserTokenRecord,
    StoreUserTokenRecord, UpdateUserRecord,
};
use crate::events::EventContext;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::load_principal_by_id;
use crate::models::token::{PrincipalToken, Token};
use crate::schema::users;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::traits::{
    BackendContext, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

use tracing::{error, warn};

/// A human user. The id is the principal id; the login/display name lives on
/// `principals.name`, not here.
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Insertable, PartialEq, Debug, Clone, ToSchema,
)]
#[diesel(table_name = users)]
pub struct User {
    pub id: i32,
    #[serde(skip_serializing)]
    pub kind: String,
    #[serde(skip_serializing)]
    pub password: Option<String>,
    pub proper_name: Option<String>,
    pub email: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub anonymized_at: Option<chrono::NaiveDateTime>,
}

/// Public representation of a user, including the name resolved from the
/// principal (the name authority).
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct UserResponse {
    pub id: i32,
    pub identity_scope: String,
    pub provider_kind: String,
    pub provider_managed: bool,
    pub name: String,
    pub proper_name: Option<String>,
    pub email: Option<String>,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl UserResponse {
    /// Build a response from a user plus its resolved principal name.
    pub fn from_parts(
        user: &User,
        identity_scope: String,
        provider_kind: String,
        name: String,
        provider_managed: bool,
        last_sync_attempted_at: Option<chrono::NaiveDateTime>,
        last_sync_success_at: Option<chrono::NaiveDateTime>,
    ) -> Self {
        Self {
            id: user.id,
            identity_scope,
            provider_kind,
            provider_managed,
            name,
            proper_name: user.proper_name.clone(),
            email: user.email.clone(),
            last_sync_attempted_at,
            last_sync_success_at,
            created_at: user.created_at,
            updated_at: user.updated_at,
        }
    }
}

/// Explicit list/search projection: the `users` row plus the principal name (the
/// name lives on `principals`). This keeps `User` a faithful `users`-table model
/// while giving cursor pagination an honest name value — `User` itself never
/// smuggles a non-table field into Diesel mappings.
#[derive(Debug, Clone)]
pub struct UserWithName {
    pub user: User,
    pub identity_scope: String,
    pub provider_kind: String,
    pub name: String,
    pub provider_managed: bool,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
}

impl UserWithName {
    /// Build from a joined user/principal/identity-scope tuple.
    pub fn from_tuple(
        t: (
            User,
            String,
            String,
            String,
            bool,
            Option<chrono::NaiveDateTime>,
            Option<chrono::NaiveDateTime>,
        ),
    ) -> Self {
        Self {
            user: t.0,
            identity_scope: t.1,
            provider_kind: t.2,
            name: t.3,
            provider_managed: t.4,
            last_sync_attempted_at: t.5,
            last_sync_success_at: t.6,
        }
    }
}

impl From<UserWithName> for UserResponse {
    fn from(value: UserWithName) -> Self {
        UserResponse::from_parts(
            &value.user,
            value.identity_scope,
            value.provider_kind,
            value.name,
            value.provider_managed,
            value.last_sync_attempted_at,
            value.last_sync_success_at,
        )
    }
}

impl CursorPaginated for UserWithName {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::IdentityScope
                | FilterField::Username
                | FilterField::ProperName
                | FilterField::Email
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.user.id as i64),
            FilterField::IdentityScope => CursorValue::String(self.identity_scope.clone()),
            FilterField::Name | FilterField::Username => CursorValue::String(self.name.clone()),
            FilterField::ProperName => match &self.user.proper_name {
                Some(value) => CursorValue::String(value.clone()),
                None => CursorValue::Null,
            },
            FilterField::Email => match &self.user.email {
                Some(email) => CursorValue::String(email.clone()),
                None => CursorValue::Null,
            },
            FilterField::CreatedAt => CursorValue::DateTime(self.user.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.user.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for users",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for UserWithName {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "users.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Username => CursorSqlField {
                column: "principals.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::IdentityScope => CursorSqlField {
                column: "identity_scopes.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::ProperName => CursorSqlField {
                column: "users.proper_name",
                sql_type: CursorSqlType::String,
                nullable: true,
            },
            FilterField::Email => CursorSqlField {
                column: "users.email",
                sql_type: CursorSqlType::String,
                nullable: true,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "users.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "users.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for users",
                    field
                )));
            }
        })
    }
}

impl User {
    /// Resolve this user's identity scope, provider metadata, and name from the
    /// principal/identity scope tables.
    pub async fn identity_scope_and_name<C>(
        &self,
        backend: &C,
    ) -> Result<
        (
            String,
            String,
            String,
            bool,
            Option<chrono::NaiveDateTime>,
            Option<chrono::NaiveDateTime>,
        ),
        ApiError,
    >
    where
        C: BackendContext + ?Sized,
    {
        let metadata =
            crate::db::traits::principal::principal_identity_metadata(backend.db_pool(), self.id)
                .await?;
        Ok((
            metadata.identity_scope,
            metadata.provider_kind,
            metadata.name,
            metadata.provider_managed,
            metadata.last_sync_attempted_at,
            metadata.last_sync_success_at,
        ))
    }

    /// Resolve this user's name from the principals table.
    pub async fn name<C>(&self, backend: &C) -> Result<String, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        Ok(load_principal_by_id(backend.db_pool(), self.id).await?.name)
    }

    /// Build a [`UserResponse`], resolving the name from the principal.
    pub async fn to_response<C>(&self, backend: &C) -> Result<UserResponse, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let (
            identity_scope,
            provider_kind,
            name,
            provider_managed,
            last_sync_attempted_at,
            last_sync_success_at,
        ) = self.identity_scope_and_name(backend).await?;
        Ok(UserResponse::from_parts(
            self,
            identity_scope,
            provider_kind,
            name,
            provider_managed,
            last_sync_attempted_at,
            last_sync_success_at,
        ))
    }

    pub async fn create_token<C>(&self, backend: &C) -> Result<Token, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let generated_token = crate::utilities::auth::generate_token();

        self.store_user_token_record(backend.db_pool(), &generated_token)
            .await?;

        Ok(generated_token)
    }

    pub async fn token_is_mine<C>(
        &self,
        token_param: Token,
        backend: &C,
    ) -> Result<PrincipalToken, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.load_owned_user_token_record(&token_param, backend.db_pool())
            .await
    }

    pub async fn delete_token<C>(&self, token_param: Token, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_owned_user_token_record(&token_param, backend.db_pool())
            .await
    }

    pub async fn delete_all_tokens<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_all_user_tokens_record(backend.db_pool()).await
    }

    /// Delete this user without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture cleanup, and event-system tests. Normal application code should
    /// use [`User::delete`] so event subscribers observe the change.
    pub async fn delete_without_events<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_user_record_without_events(backend.db_pool())
            .await
    }

    pub async fn delete<C>(
        &self,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_user_record(backend.db_pool(), context).await
    }

    pub async fn anonymize<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.anonymize_user_record(backend.db_pool()).await
    }
}

/// Struct to update a user.
///
/// The password, if present, is expected to be plaintext. The name lives on the
/// principal; renaming is handled via the principal, not here.
#[derive(AsChangeset, Deserialize, Serialize, Clone, ToSchema)]
#[schema(example = update_user_example)]
#[diesel(table_name = users)]
pub struct UpdateUser {
    pub password: Option<String>,
    pub proper_name: Option<String>,
    pub email: Option<String>,
}

impl UpdateUser {
    pub fn hash_password(self) -> Result<Self, ApiError> {
        if let Some(ref pass) = self.password {
            match crate::utilities::auth::hash_password(pass) {
                Ok(hashed_password) => {
                    return Ok(UpdateUser {
                        password: Some(hashed_password),
                        ..self
                    });
                }
                Err(e) => return Err(ApiError::HashError(format!("Failed to hash password: {e}"))),
            }
        }
        Ok(self)
    }

    /// Persist changes without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture construction, cleanup, and event-system tests. Normal application
    /// code should use [`UpdateUser::save`] so event subscribers observe the
    /// change.
    pub async fn save_without_events<C>(self, user_id: i32, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password()?;
        hashed
            .update_user_record_without_events(user_id, backend.db_pool())
            .await
    }

    pub async fn save<C>(
        self,
        user_id: i32,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password()?;
        hashed
            .update_user_record(user_id, backend.db_pool(), context)
            .await
    }
}

/// Struct to create a new user.
///
/// The password is expected to be plaintext. `name` is the principal name.
#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[schema(example = new_user_example)]
pub struct NewUser {
    pub identity_scope: Option<String>,
    pub name: String,
    pub password: String,
    pub proper_name: Option<String>,
    pub email: Option<String>,
}

impl NewUser {
    /// Persist without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture construction, cleanup, and event-system tests. Normal application
    /// code should use [`NewUser::save`] so event subscribers observe the change.
    pub async fn save_without_events<C>(self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password()?;
        hashed
            .create_user_record_without_events(backend.db_pool())
            .await
    }

    pub async fn save<C>(
        self,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password()?;
        hashed.create_user_record(backend.db_pool(), context).await
    }

    pub fn hash_password(mut self) -> Result<Self, ApiError> {
        match crate::utilities::auth::hash_password(&self.password) {
            Ok(hashed_password) => {
                self.password = hashed_password;
            }
            Err(e) => return Err(ApiError::HashError(format!("Failed to hash password: {e}"))),
        }
        Ok(self)
    }
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`User`].
    pub struct UserID;
    noun = "user id";
}

impl UserID {
    pub async fn user<C>(&self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        use crate::db::traits::user::LoadUserRecord;
        self.load_user_record(backend.db_pool()).await
    }

    pub async fn delete<C>(
        &self,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<usize, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.delete_user_record(backend.db_pool(), context).await
    }

    pub async fn anonymize<C>(&self, backend: &C) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.anonymize_user_record(backend.db_pool()).await
    }
}

/// Struct to log in a user.
///
/// The password is expected to be plaintext. `name` is the principal name.
#[derive(Deserialize, Serialize, ToSchema)]
#[schema(example = login_user_example)]
pub struct LoginUser {
    pub identity_scope: Option<String>,
    pub name: String,
    pub password: String,
}

impl LoginUser {
    /// Check if the user exists and the plaintext password in the struct
    /// matches the hashed password in the database.
    pub async fn login<C>(self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        // We deliberately map "not found" to a generic auth failure (401) rather
        // than 404 so we do not leak which names exist. Service-account
        // principals have no users row, so they naturally cannot log in here.
        let identity_scope = self
            .identity_scope
            .as_deref()
            .unwrap_or(LOCAL_IDENTITY_SCOPE);
        let user =
            match User::get_by_name_in_scope(backend.db_pool(), identity_scope, &self.name).await {
                Ok(user) => user,
                Err(_) => {
                    // Keep unknown-user and wrong-password paths comparable: both execute
                    // one Argon2 verification before returning the same public error.
                    let password = self.password.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        crate::utilities::auth::verify_dummy_password(&password)
                    })
                    .await;
                    warn!(message = "Login failed (user not found)", user = self.name);
                    return Err(auth_failure());
                }
            };

        let plaintext_password = &self.password;
        let Some(hashed_password) = &user.password else {
            warn!(
                message = "Login failed (local password missing)",
                user = self.name
            );
            return Err(auth_failure());
        };

        let plaintext_password = plaintext_password.clone();
        let hashed_password = hashed_password.clone();
        let verification = tokio::task::spawn_blocking(move || {
            crate::utilities::auth::verify_password(&plaintext_password, &hashed_password)
        })
        .await;

        match verification {
            Ok(Ok(true)) => Ok(user),
            Ok(Ok(false)) => {
                warn!(
                    message = "Login failed (password mismatch)",
                    user = self.name
                );
                Err(auth_failure())
            }
            Ok(Err(e)) => {
                error!(
                    message = "Login failed (hashing error)",
                    user = self.name,
                    error = e.to_string()
                );
                Err(auth_failure())
            }
            Err(e) => {
                error!(
                    message = "Login failed (password worker error)",
                    user = self.name,
                    error = e.to_string()
                );
                Err(auth_failure())
            }
        }
    }
}

pub fn auth_failure() -> ApiError {
    ApiError::Unauthorized("Authentication failure".to_string())
}

#[allow(dead_code)]
fn update_user_example() -> UpdateUser {
    UpdateUser {
        password: Some("new-password".to_string()),
        proper_name: Some("Alice Doe".to_string()),
        email: Some("alice@example.com".to_string()),
    }
}

#[allow(dead_code)]
fn new_user_example() -> NewUser {
    NewUser {
        identity_scope: None,
        name: "alice".to_string(),
        password: "correct-horse-battery-staple".to_string(),
        proper_name: Some("Alice Doe".to_string()),
        email: Some("alice@example.com".to_string()),
    }
}

#[allow(dead_code)]
fn login_user_example() -> LoginUser {
    LoginUser {
        identity_scope: None,
        name: "alice".to_string(),
        password: "correct-horse-battery-staple".to_string(),
    }
}
