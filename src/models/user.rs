use std::fmt;

use crate::db::prelude::*;
use crate::db::traits::user::{
    AnonymizeUserRecord, CreateUserRecord, DeleteUserRecord, OwnedUserTokenRecord,
    SetUserPasswordRecord, UpdateUserRecord,
};
use crate::events::EventContext;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::load_principal_by_id;
use crate::models::token::{IssuedToken, PrincipalToken, PrincipalTokenCreateRequest, Token};
use crate::models::{PrincipalID, REDACTED_DEBUG_VALUE, ResourceRevision, redacted_debug_option};
use crate::schema::users;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::traits::{
    BackendContext, CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

use tracing::{debug, error, warn};

pub const MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS: usize = 255;
pub const MAX_LOGIN_NAME_CHARACTERS: usize = 255;
pub const MAX_LOGIN_PASSWORD_CHARACTERS: usize = 4096;

/// A human user. The id is the principal id; the login/display name lives on
/// `principals.name`, not here.
#[derive(Serialize, Deserialize, Queryable, Selectable, Insertable, PartialEq, Clone, ToSchema)]
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

impl fmt::Debug for User {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("User")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("password", &redacted_debug_option(&self.password))
            .field("proper_name", &self.proper_name)
            .field("email", &self.email)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("anonymized_at", &self.anonymized_at)
            .finish()
    }
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
    pub revision: ResourceRevision,
}

/// Strongly tagged point representation of a user.
///
/// Directory-provider metadata is intentionally absent: its lifecycle is not
/// owned by the principal revision used for this representation's ETag.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct UserPointResponse {
    pub id: i32,
    pub identity_scope_id: i32,
    pub provider_managed: bool,
    pub name: String,
    pub proper_name: Option<String>,
    pub email: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

impl UserPointResponse {
    pub fn from_parts(
        user: User,
        identity_scope_id: i32,
        name: String,
        provider_managed: bool,
        revision: ResourceRevision,
    ) -> Self {
        Self {
            id: user.id,
            identity_scope_id,
            provider_managed,
            name,
            proper_name: user.proper_name,
            email: user.email,
            created_at: user.created_at,
            updated_at: user.updated_at,
            revision,
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
    pub revision: ResourceRevision,
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
            ResourceRevision,
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
            revision: t.7,
        }
    }
}

impl From<UserWithName> for UserResponse {
    fn from(value: UserWithName) -> Self {
        Self {
            id: value.user.id,
            identity_scope: value.identity_scope,
            provider_kind: value.provider_kind,
            provider_managed: value.provider_managed,
            name: value.name,
            proper_name: value.user.proper_name,
            email: value.user.email,
            last_sync_attempted_at: value.last_sync_attempted_at,
            last_sync_success_at: value.last_sync_success_at,
            created_at: value.user.created_at,
            updated_at: value.user.updated_at,
            revision: value.revision,
        }
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
                | FilterField::Revision
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
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
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
            FilterField::Revision => CursorSqlField {
                column: "principals.revision",
                sql_type: CursorSqlType::BigInt,
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
        crate::db::traits::principal::load_user_response(backend.db_pool(), self.id).await
    }

    /// Build the strongly tagged point representation in one database snapshot.
    pub async fn to_point_response<C>(&self, backend: &C) -> Result<UserPointResponse, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        crate::db::traits::principal::load_user_point_response(backend.db_pool(), self.id).await
    }

    /// Set a new local password and revoke every active bearer token for this
    /// user in the same database transaction.
    pub async fn set_password<C>(&self, backend: &C, new_password: &str) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
    {
        debug!(message = "Setting new password", user_id = self.id);
        let password_hash = crate::utilities::auth::hash_password_async(new_password.to_string())
            .await
            .map_err(|error| ApiError::HashError(format!("Failed to hash password: {error}")))?;
        let revoked_tokens = self
            .set_password_record(backend.db_pool(), &password_hash)
            .await?;
        debug!(
            message = "Password changed and active tokens revoked",
            user_id = self.id,
            revoked_tokens
        );
        Ok(())
    }

    pub async fn create_token<C>(&self, backend: &C) -> Result<Token, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        Ok(self.create_issued_token(backend).await?.into_token())
    }

    pub async fn create_issued_token<C>(&self, backend: &C) -> Result<IssuedToken, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        PrincipalTokenCreateRequest::new(PrincipalID::new(self.id)?)
            .create_issued(backend, None)
            .await
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
    pub(crate) fn has_changes(&self, current: &User) -> bool {
        self.password.is_some()
            || self
                .proper_name
                .as_ref()
                .is_some_and(|value| Some(value) != current.proper_name.as_ref())
            || self
                .email
                .as_ref()
                .is_some_and(|value| Some(value) != current.email.as_ref())
    }

    pub async fn hash_password(mut self) -> Result<Self, ApiError> {
        if let Some(password) = self.password.take() {
            self.password = Some(
                crate::utilities::auth::hash_password_async(password)
                    .await
                    .map_err(|error| {
                        ApiError::HashError(format!("Failed to hash password: {error}"))
                    })?,
            );
        }
        Ok(self)
    }

    /// Persist changes without emitting domain events.
    ///
    /// Intended only for internal infrastructure paths such as bootstrap/setup,
    /// fixture construction, cleanup, and event-system tests. Normal application
    /// code should use [`UpdateUser::save`] so event subscribers observe the
    /// change.
    pub async fn save_without_events<C>(
        self,
        user_id: UserID,
        backend: &C,
    ) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password().await?;
        hashed
            .update_user_record_without_events(user_id.id(), backend.db_pool())
            .await
    }

    pub async fn save<C>(
        self,
        user_id: UserID,
        backend: &C,
        context: Option<&EventContext>,
    ) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let hashed = self.hash_password().await?;
        hashed
            .update_user_record(user_id.id(), backend.db_pool(), context)
            .await
    }
}

/// Struct to create a new user.
///
/// The password is expected to be plaintext. `name` is the principal name.
#[derive(Serialize, Deserialize, ToSchema)]
#[schema(example = new_user_example)]
pub struct NewUser {
    pub identity_scope: Option<String>,
    pub name: String,
    pub password: String,
    pub proper_name: Option<String>,
    pub email: Option<String>,
}

impl fmt::Debug for NewUser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewUser")
            .field("identity_scope", &self.identity_scope)
            .field("name", &self.name)
            .field("password", &REDACTED_DEBUG_VALUE)
            .field("proper_name", &self.proper_name)
            .field("email", &self.email)
            .finish()
    }
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
        let hashed = self.hash_password().await?;
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
        let hashed = self.hash_password().await?;
        hashed.create_user_record(backend.db_pool(), context).await
    }

    pub async fn hash_password(mut self) -> Result<Self, ApiError> {
        self.password = crate::utilities::auth::hash_password_async(self.password)
            .await
            .map_err(|error| ApiError::HashError(format!("Failed to hash password: {error}")))?;
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
    #[schema(max_length = 255)]
    pub identity_scope: Option<String>,
    #[schema(max_length = 255)]
    pub name: String,
    #[schema(max_length = 4096)]
    pub password: String,
}

impl LoginUser {
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_login_field_length(
            "identity_scope",
            self.identity_scope.as_deref().unwrap_or_default(),
            MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS,
        )?;
        validate_login_field_length("name", &self.name, MAX_LOGIN_NAME_CHARACTERS)?;
        validate_login_field_length("password", &self.password, MAX_LOGIN_PASSWORD_CHARACTERS)
    }

    /// Check if the user exists and the plaintext password in the struct
    /// matches the hashed password in the database.
    pub async fn login<C>(self, backend: &C) -> Result<User, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.validate()?;
        // We deliberately map "not found" to a generic auth failure (401) rather
        // than 404 so we do not leak which names exist. Service-account
        // principals have no users row, so they naturally cannot log in here.
        let identity_scope = self
            .identity_scope
            .as_deref()
            .unwrap_or(LOCAL_IDENTITY_SCOPE);
        let user = match User::get_by_name_in_scope(backend.db_pool(), identity_scope, &self.name)
            .await
        {
            Ok(user) => user,
            Err(_) => {
                // Keep unknown-user and wrong-password paths comparable: both execute
                // one Argon2 verification before returning the same public error.
                let _ = crate::utilities::auth::verify_dummy_password_async(self.password.clone())
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
        let verification =
            crate::utilities::auth::verify_password_async(plaintext_password, hashed_password)
                .await;

        match verification {
            Ok(true) => Ok(user),
            Ok(false) => {
                warn!(
                    message = "Login failed (password mismatch)",
                    user = self.name
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

fn validate_login_field_length(
    field: &str,
    value: &str,
    maximum_characters: usize,
) -> Result<(), ApiError> {
    if value.chars().nth(maximum_characters).is_some() {
        return Err(ApiError::BadRequest(format!(
            "{field} must be at most {maximum_characters} characters"
        )));
    }
    Ok(())
}

pub fn auth_failure() -> ApiError {
    ApiError::Unauthorized("Authentication failure".to_string())
}

fn update_user_example() -> UpdateUser {
    UpdateUser {
        password: Some("new-password".to_string()),
        proper_name: Some("Alice Doe".to_string()),
        email: Some("alice@example.com".to_string()),
    }
}

fn new_user_example() -> NewUser {
    NewUser {
        identity_scope: None,
        name: "alice".to_string(),
        password: "correct-horse-battery-staple".to_string(),
        proper_name: Some("Alice Doe".to_string()),
        email: Some("alice@example.com".to_string()),
    }
}

fn login_user_example() -> LoginUser {
    LoginUser {
        identity_scope: None,
        name: "alice".to_string(),
        password: "correct-horse-battery-staple".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_timestamp() -> chrono::NaiveDateTime {
        chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .unwrap()
            .naive_utc()
    }

    #[test]
    fn user_debug_redacts_stored_password_hash() {
        let password_hash = "$argon2id$stored-password-hash";
        let user = User {
            id: 1,
            kind: "human".to_string(),
            password: Some(password_hash.to_string()),
            proper_name: Some("Alice".to_string()),
            email: Some("alice@example.com".to_string()),
            created_at: test_timestamp(),
            updated_at: test_timestamp(),
            anonymized_at: None,
        };

        let output = format!("{user:?}");

        assert!(output.contains(REDACTED_DEBUG_VALUE));
        assert!(!output.contains(password_hash));
    }

    #[test]
    fn new_user_debug_redacts_plaintext_password() {
        let password = "correct-horse-battery-staple";
        let user = NewUser {
            identity_scope: None,
            name: "alice".to_string(),
            password: password.to_string(),
            proper_name: Some("Alice".to_string()),
            email: Some("alice@example.com".to_string()),
        };

        let output = format!("{user:?}");

        assert!(output.contains(REDACTED_DEBUG_VALUE));
        assert!(!output.contains(password));
    }

    #[rstest::rstest]
    #[case::identity_scope(
        Some("s".repeat(MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS + 1)),
        "alice".to_string(),
        "password".to_string(),
        "identity_scope"
    )]
    #[case::name(
        None,
        "a".repeat(MAX_LOGIN_NAME_CHARACTERS + 1),
        "password".to_string(),
        "name"
    )]
    #[case::password(
        None,
        "alice".to_string(),
        "p".repeat(MAX_LOGIN_PASSWORD_CHARACTERS + 1),
        "password"
    )]
    fn oversized_login_fields_are_rejected(
        #[case] identity_scope: Option<String>,
        #[case] name: String,
        #[case] password: String,
        #[case] field: &str,
    ) {
        let error = LoginUser {
            identity_scope,
            name,
            password,
        }
        .validate()
        .unwrap_err();

        assert!(matches!(error, ApiError::BadRequest(message) if message.contains(field)));
    }

    #[test]
    fn login_fields_accept_their_documented_character_limits() {
        let login = LoginUser {
            identity_scope: Some("s".repeat(MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS)),
            name: "a".repeat(MAX_LOGIN_NAME_CHARACTERS),
            password: "p".repeat(MAX_LOGIN_PASSWORD_CHARACTERS),
        };

        login.validate().unwrap();
    }
}
