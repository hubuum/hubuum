use hubuum_domain::PlaintextPassword;
use std::fmt;

use crate::events::EventContext;
use crate::models::identity::LOCAL_IDENTITY_SCOPE;
use crate::models::principal::load_principal_by_id;
use crate::models::token::{IssuedToken, PrincipalTokenCreateRequest, Token};
use crate::models::{
    PrincipalID, PrincipalKind, REDACTED_DEBUG_VALUE, ResourceRevision, redacted_debug_option,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::storage::StorageContext;
use crate::traits::{CursorPaginated, CursorValue};

use crate::storage::{StorageUserCreate, StorageUserUpdate};
use crate::utilities::auth::{HashedPassword, hash_password_async};

pub const MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS: usize = 255;
pub const MAX_LOGIN_NAME_CHARACTERS: usize = 255;
pub const MAX_LOGIN_PASSWORD_CHARACTERS: usize = PlaintextPassword::MAX_CHARACTERS;

/// A human user. The id is the principal id; the login/display name lives on
/// `principals.name`, not here.
#[derive(Serialize, Deserialize, PartialEq, Clone, ToSchema)]
pub struct User {
    pub id: i32,
    #[serde(skip_serializing)]
    pub kind: PrincipalKind,
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

impl User {
    /// Resolve this user's name from the principals table.
    pub async fn name<C>(&self, backend: &C) -> Result<String, ApiError>
    where
        C: StorageContext,
    {
        Ok(load_principal_by_id(backend, self.id).await?.name)
    }

    /// Build the strongly tagged point representation in one database snapshot.
    pub async fn to_point_response<C>(&self, backend: &C) -> Result<UserPointResponse, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::get_user_point(backend, self.id).await
    }

    pub async fn create_token<C>(&self, backend: &C) -> Result<Token, ApiError>
    where
        C: StorageContext,
    {
        Ok(self.create_issued_token(backend).await?.into_token())
    }

    pub async fn create_issued_token<C>(&self, backend: &C) -> Result<IssuedToken, ApiError>
    where
        C: StorageContext,
    {
        PrincipalTokenCreateRequest::new(PrincipalID::new(self.id)?)
            .create_issued(backend, &EventContext::system())
            .await
    }

    pub async fn delete_all_tokens<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::revoke_all_principal_tokens(
            backend,
            self.id,
            &EventContext::system(),
        )
        .await
    }

    /// Delete this user with system audit attribution.
    ///
    /// This compatibility name is retained for internal callers; the mutation
    /// still emits an audit event attributed to the system actor.
    pub async fn delete_without_events<C>(&self, backend: &C) -> Result<usize, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::delete_user(backend, self.id, &EventContext::system()).await
    }

    pub async fn delete<C>(&self, backend: &C, context: &EventContext) -> Result<usize, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::delete_user(backend, self.id, context).await
    }

    pub async fn anonymize<C>(&self, backend: &C, context: &EventContext) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::anonymize_user(backend, self.id, context).await
    }
}

/// Struct to update a user.
///
/// The password, if present, is expected to be plaintext. The name lives on the
/// principal; renaming is handled via the principal, not here.
#[derive(Deserialize, Serialize, Clone, ToSchema)]
#[schema(example = update_user_example)]
pub struct UpdateUser {
    #[schema(min_length = 1, max_length = 4096)]
    pub password: Option<String>,
    pub proper_name: Option<String>,
    pub email: Option<String>,
}

impl UpdateUser {
    pub async fn hash_password(self) -> Result<HashedUserUpdate, ApiError> {
        let password = match self.password {
            Some(password) => Some(hash_plaintext(password).await?),
            None => None,
        };
        Ok(HashedUserUpdate {
            password,
            proper_name: self.proper_name,
            email: self.email,
        })
    }

    /// Persist changes with system audit attribution.
    ///
    /// This compatibility name is retained for internal callers; the mutation
    /// still emits an audit event attributed to the system actor.
    pub async fn save_without_events<C>(
        self,
        user_id: UserID,
        backend: &C,
    ) -> Result<User, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::update_user(backend, user_id.id(), self, &EventContext::system())
            .await
    }

    pub async fn save<C>(
        self,
        user_id: UserID,
        backend: &C,
        context: &EventContext,
    ) -> Result<User, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::update_user(backend, user_id.id(), self, context).await
    }
}

/// Struct to create a new user.
///
/// The password is expected to be plaintext. `name` is the principal name.
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = new_user_example)]
pub struct NewUser {
    pub identity_scope: Option<String>,
    pub name: String,
    #[schema(min_length = 1, max_length = 4096)]
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
    /// Persist with system audit attribution.
    ///
    /// This compatibility name is retained for internal callers; the mutation
    /// still emits an audit event attributed to the system actor.
    pub async fn save_without_events<C>(self, backend: &C) -> Result<User, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::create_user(backend, self, &EventContext::system()).await
    }

    pub async fn save<C>(self, backend: &C, context: &EventContext) -> Result<User, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::create_user(backend, self, context).await
    }

    pub async fn hash_password(self) -> Result<HashedNewUser, ApiError> {
        let password = hash_plaintext(self.password).await?;
        Ok(HashedNewUser {
            identity_scope: self.identity_scope,
            name: self.name,
            password,
            proper_name: self.proper_name,
            email: self.email,
        })
    }

    #[cfg(any(test, feature = "integration-test-support"))]
    pub(crate) fn with_password_hash(self, password: HashedPassword) -> HashedNewUser {
        HashedNewUser {
            identity_scope: self.identity_scope,
            name: self.name,
            password,
            proper_name: self.proper_name,
            email: self.email,
        }
    }
}

async fn hash_plaintext(password: String) -> Result<HashedPassword, ApiError> {
    hash_password_async(
        PlaintextPassword::new(password)
            .map_err(|error| ApiError::BadRequest(error.to_string()))?,
    )
    .await
    .map_err(|error| ApiError::HashError(format!("Failed to hash password: {error}")))
}

pub struct HashedNewUser {
    identity_scope: Option<String>,
    name: String,
    password: HashedPassword,
    proper_name: Option<String>,
    email: Option<String>,
}

impl HashedNewUser {
    pub(crate) fn into_storage(self, event: EventContext) -> StorageUserCreate {
        StorageUserCreate::new(
            self.identity_scope,
            self.name,
            self.password.into_encoded(),
            self.proper_name,
            self.email,
            event,
        )
    }
}

pub struct HashedUserUpdate {
    password: Option<HashedPassword>,
    proper_name: Option<String>,
    email: Option<String>,
}

impl HashedUserUpdate {
    pub(crate) fn into_storage(self, id: UserID, event: EventContext) -> StorageUserUpdate {
        StorageUserUpdate::new(
            id,
            self.password.map(HashedPassword::into_encoded),
            self.proper_name,
            self.email,
            event,
        )
    }
}

pub use hubuum_domain::UserId as UserID;

/// Application behavior for a backend-neutral user identifier.
pub trait UserIdApplicationExt {
    async fn user<C>(&self, backend: &C) -> Result<User, ApiError>
    where
        C: StorageContext;

    async fn delete<C>(&self, backend: &C, context: &EventContext) -> Result<usize, ApiError>
    where
        C: StorageContext;

    async fn anonymize<C>(&self, backend: &C, context: &EventContext) -> Result<(), ApiError>
    where
        C: StorageContext;
}

impl UserIdApplicationExt for UserID {
    async fn user<C>(&self, backend: &C) -> Result<User, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::get_user(backend, self.id()).await
    }

    async fn delete<C>(&self, backend: &C, context: &EventContext) -> Result<usize, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::delete_user(backend, self.id(), context).await
    }

    async fn anonymize<C>(&self, backend: &C, context: &EventContext) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::anonymize_user(backend, self.id(), context).await
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
    #[schema(min_length = 1, max_length = 4096)]
    pub password: String,
}

/// Login input whose length and plaintext-password invariants have been checked.
#[derive(Debug)]
pub struct ValidatedLogin {
    identity_scope: String,
    name: String,
    password: PlaintextPassword,
}

impl ValidatedLogin {
    pub fn identity_scope(&self) -> &str {
        &self.identity_scope
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub(crate) fn password(&self) -> &PlaintextPassword {
        &self.password
    }
    pub(crate) fn into_password(self) -> PlaintextPassword {
        self.password
    }
}

impl LoginUser {
    pub fn validate(self) -> Result<ValidatedLogin, ApiError> {
        validate_login_field_length(
            "identity_scope",
            self.identity_scope.as_deref().unwrap_or_default(),
            MAX_LOGIN_IDENTITY_SCOPE_CHARACTERS,
        )?;
        validate_login_field_length("name", &self.name, MAX_LOGIN_NAME_CHARACTERS)?;
        Ok(ValidatedLogin {
            identity_scope: self
                .identity_scope
                .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.into()),
            name: self.name,
            password: PlaintextPassword::new(self.password)
                .map_err(|error| ApiError::BadRequest(error.to_string()))?,
        })
    }

    /// Authenticate a validated local credential without issuing a token.
    pub async fn login<C>(self, backend: &C) -> Result<User, ApiError>
    where
        C: StorageContext,
    {
        crate::services::authentication::authenticate_local(backend, self.validate()?)
            .await
            .map(crate::services::identity::user_from_storage)
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
    fn validated_login_debug_redacts_plaintext_password() {
        let login = LoginUser {
            identity_scope: None,
            name: "alice".into(),
            password: "credential-must-not-appear".into(),
        }
        .validate()
        .unwrap();
        assert!(!format!("{login:?}").contains("credential-must-not-appear"));
    }

    #[test]
    fn user_debug_redacts_stored_password_hash() {
        let password_hash = "$argon2id$stored-password-hash";
        let user = User {
            id: 1,
            kind: PrincipalKind::Human,
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
    #[case::empty_password(None, "alice".into(), String::new(), "password")]
    fn invalid_login_fields_are_rejected(
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
