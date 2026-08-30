//! PostgreSQL implementation of the human-user storage contract.

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{AsChangeset, JoinOnDsl, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{IdentityScopeId, UserId};
use hubuum_events_core::{Action, AuditDocument, EntityType, EventContext, NewEvent};
use hubuum_query::FilterField;
use hubuum_storage_core::{
    StorageMutationOutcome, StoragePage, StorageUser, StorageUserAnonymize, StorageUserCreate,
    StorageUserDelete, StorageUserDetails, StorageUserListItem, StorageUserListQuery,
    StorageUserPasswordUpdate, StorageUserUpdate,
};
use serde_json::{Value, json};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::event_record::append_event;
use crate::revision::RevisionOwner;
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

const ANONYMIZED_PASSWORD: &str = "!anonymized-no-login";
const HUMAN_PRINCIPAL_KIND: &str = "human";
const LOCAL_IDENTITY_SCOPE: &str = "local";

macro_rules! apply_user_filters {
    ($query:ident, $options:expr) => {
        for parameter in $options.filters() {
            match parameter.field {
                FilterField::Id => {
                    crate::postgres_integer_filter!($query, parameter, crate::schema::users::id)
                }
                FilterField::Name | FilterField::Username => crate::postgres_string_filter!(
                    $query,
                    parameter,
                    crate::schema::principals::name
                ),
                FilterField::IdentityScope => crate::postgres_string_filter!(
                    $query,
                    parameter,
                    crate::schema::identity_scopes::name
                ),
                FilterField::ProperName => crate::postgres_string_filter!(
                    $query,
                    parameter,
                    crate::schema::users::proper_name
                ),
                FilterField::Email => {
                    crate::postgres_string_filter!($query, parameter, crate::schema::users::email)
                }
                FilterField::CreatedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::users::created_at
                ),
                FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                    $query,
                    parameter,
                    crate::schema::users::updated_at
                ),
                FilterField::Revision => crate::postgres_revision_filter!(
                    $query,
                    parameter,
                    crate::schema::principals::revision
                ),
                _ => {
                    return Err(PostgresStorageError::invalid_input(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        parameter.field
                    )));
                }
            }
        }
    };
}

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::users)]
pub(crate) struct UserRow {
    pub(crate) id: i32,
    #[diesel(column_name = kind)]
    pub(crate) _kind: String,
    pub(crate) password: Option<String>,
    pub(crate) proper_name: Option<String>,
    pub(crate) email: Option<String>,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
    pub(crate) anonymized_at: Option<NaiveDateTime>,
}

impl UserRow {
    fn into_storage(self) -> Result<StorageUser, PostgresStorageError> {
        crate::validate_persisted(
            "user",
            StorageUser::try_new(
                hubuum_domain::UserId::new(self.id)?,
                self.password,
                self.proper_name,
                self.email,
                self.created_at.and_utc(),
                self.updated_at.and_utc(),
                self.anonymized_at.map(|timestamp| timestamp.and_utc()),
            ),
        )
    }

    fn snapshot(&self, name: &str, revision: PostgresRevision) -> Value {
        json!({
            "id": self.id,
            "name": name,
            "proper_name": self.proper_name,
            "email": self.email,
            "revision": revision,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::users)]
struct UpdateUserRow<'value> {
    password: Option<&'value String>,
    proper_name: Option<&'value String>,
    email: Option<&'value String>,
}

pub async fn get_user(
    runtime: &PostgresRuntime,
    user_id: i32,
) -> Result<StorageUser, PostgresStorageError> {
    validate_positive_id(user_id, "user id")?;
    runtime
        .with_connection(async |connection| {
            load_user_row(connection, user_id).await?.into_storage()
        })
        .await
}

pub async fn get_user_by_name(
    runtime: &PostgresRuntime,
    identity_scope: String,
    name: String,
) -> Result<StorageUser, PostgresStorageError> {
    runtime
        .with_connection(async move |connection| {
            crate::schema::users::table
                .inner_join(
                    crate::schema::principals::table
                        .on(crate::schema::users::id.eq(crate::schema::principals::id)),
                )
                .inner_join(
                    crate::schema::identity_scopes::table
                        .on(crate::schema::principals::identity_scope_id
                            .eq(crate::schema::identity_scopes::id)),
                )
                .filter(crate::schema::principals::name.eq(name))
                .filter(crate::schema::identity_scopes::name.eq(identity_scope))
                .select(UserRow::as_select())
                .first::<UserRow>(connection)
                .await
                .map_err(PostgresStorageError::from)?
                .into_storage()
        })
        .await
}

pub async fn get_user_details(
    runtime: &PostgresRuntime,
    user_id: i32,
) -> Result<StorageUserDetails, PostgresStorageError> {
    validate_positive_id(user_id, "user id")?;
    runtime
        .with_connection(async |connection| {
            let (user, identity_scope_id, provider_managed, name, revision) =
                crate::schema::users::table
                    .inner_join(
                        crate::schema::principals::table
                            .on(crate::schema::principals::id.eq(crate::schema::users::id)),
                    )
                    .filter(crate::schema::users::id.eq(user_id))
                    .select((
                        UserRow::as_select(),
                        crate::schema::principals::identity_scope_id,
                        crate::schema::principals::provider_managed,
                        crate::schema::principals::name,
                        crate::schema::principals::revision,
                    ))
                    .first::<(UserRow, i32, bool, String, PostgresRevision)>(connection)
                    .await?;
            crate::validate_persisted(
                "user details",
                StorageUserDetails::builder(
                    UserId::new(user.id)?,
                    user.created_at.and_utc(),
                    user.updated_at.and_utc(),
                    IdentityScopeId::new(identity_scope_id)?,
                    name,
                    revision.into_domain(),
                )
                .proper_name(user.proper_name)
                .email(user.email)
                .provider_managed(provider_managed)
                .try_build(),
            )
        })
        .await
}

pub async fn list_users(
    runtime: &PostgresRuntime,
    query: StorageUserListQuery,
) -> Result<StoragePage<StorageUserListItem>, PostgresStorageError> {
    let options = query.into_options();
    runtime
        .with_read_only_snapshot(async move |connection| {
            let structured_predicate = match options.structured_filter() {
                Some(expression) => Some(
                    crate::operations::structured_search::structured_filter_predicate(
                        connection,
                        expression,
                        crate::operations::structured_search::StructuredResourceKind::User,
                        None,
                    )
                    .await?,
                ),
                None => None,
            };
            let build_query = || -> Result<_, PostgresStorageError> {
                let mut records = crate::schema::users::table
                    .inner_join(
                        crate::schema::principals::table
                            .on(crate::schema::users::id.eq(crate::schema::principals::id)),
                    )
                    .inner_join(
                        crate::schema::identity_scopes::table
                            .on(crate::schema::principals::identity_scope_id
                                .eq(crate::schema::identity_scopes::id)),
                    )
                    .into_boxed();
                if let Some(predicate) = structured_predicate.clone() {
                    records = records.filter(predicate);
                }
                apply_user_filters!(records, options);
                Ok(records)
            };
            let total = if options.include_total() {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = options
                .sort()
                .iter()
                .map(|sort| user_cursor_field(&sort.field))
                .collect::<Result<Vec<_>, _>>()?;
            crate::apply_query_options_with_fields!(
                records,
                options,
                fields,
                crate::cursor::CursorTieBreaker::new(
                    FilterField::Id,
                    false,
                    user_cursor_field(&FilterField::Id)?,
                )
            );
            let rows = records
                .select((
                    UserRow::as_select(),
                    crate::schema::identity_scopes::name,
                    crate::schema::identity_scopes::provider_kind,
                    crate::schema::principals::name,
                    crate::schema::principals::provider_managed,
                    crate::schema::principals::last_sync_attempted_at,
                    crate::schema::principals::last_sync_success_at,
                    crate::schema::principals::revision,
                ))
                .distinct()
                .load::<(
                    UserRow,
                    String,
                    String,
                    String,
                    bool,
                    Option<NaiveDateTime>,
                    Option<NaiveDateTime>,
                    PostgresRevision,
                )>(connection)
                .await?;
            let items = rows
                .into_iter()
                .map(
                    |(user, scope, provider, name, managed, attempted, succeeded, revision)| {
                        crate::validate_persisted(
                            "user list item",
                            StorageUserListItem::builder(
                                user.into_storage()?,
                                scope,
                                provider,
                                name,
                                revision.into_domain(),
                            )
                            .provider_managed(managed)
                            .last_sync_attempted_at(attempted.map(|timestamp| timestamp.and_utc()))
                            .last_sync_success_at(succeeded.map(|timestamp| timestamp.and_utc()))
                            .try_build(),
                        )
                    },
                )
                .collect::<Result<Vec<_>, PostgresStorageError>>()?;
            crate::persisted_page(items, total)
        })
        .await
}

pub async fn create_user(
    runtime: &PostgresRuntime,
    request: StorageUserCreate,
) -> Result<StorageMutationOutcome<StorageUser>, PostgresStorageError> {
    let (identity_scope, name, password, proper_name, email, context) = request.into_parts();
    let identity_scope = identity_scope.unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
    if identity_scope != LOCAL_IDENTITY_SCOPE {
        return Err(PostgresStorageError::invalid_input(
            "users in non-local identity scopes are managed by their identity provider",
        ));
    }
    runtime
        .with_transaction(async move |connection| {
            let scope_id = local_identity_scope_id(connection).await?;
            let principal_id = diesel::insert_into(crate::schema::principals::table)
                .values((
                    crate::schema::principals::identity_scope_id.eq(scope_id),
                    crate::schema::principals::kind.eq(HUMAN_PRINCIPAL_KIND),
                    crate::schema::principals::name.eq(&name),
                ))
                .returning(crate::schema::principals::id)
                .get_result::<i32>(connection)
                .await?;
            let user = diesel::insert_into(crate::schema::users::table)
                .values((
                    crate::schema::users::id.eq(principal_id),
                    crate::schema::users::password.eq(Some(password)),
                    crate::schema::users::proper_name.eq(proper_name),
                    crate::schema::users::email.eq(email),
                ))
                .get_result::<UserRow>(connection)
                .await?;
            let revision = principal_revision(connection, principal_id).await?;
            let document = AuditDocument::try_new(
                format!("User '{name}' created"),
                None,
                Some(user.snapshot(&name, revision)),
                json!({}),
            )?;
            let event = user_event(&user, &name, Action::Created, &context, document)?;
            let audit = append_event(connection, &event).await?.into_audit_receipt();
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(
                user.into_storage()?,
                audit,
            ))
        })
        .await
}

pub async fn update_user(
    runtime: &PostgresRuntime,
    request: StorageUserUpdate,
) -> Result<StorageMutationOutcome<StorageUser>, PostgresStorageError> {
    let (user_id, password, proper_name, email, context) = request.into_parts();
    let user_id = user_id.id();
    validate_positive_id(user_id, "user id")?;
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_principal_revision(connection, user_id).await?;
            ensure_user_allows_local_write(connection, user_id).await?;
            let before = load_user_row(connection, user_id).await?;
            let name = principal_name(connection, user_id).await?;
            if password
                .as_ref()
                .is_none_or(|value| Some(value) == before.password.as_ref())
                && proper_name
                    .as_ref()
                    .is_none_or(|value| Some(value) == before.proper_name.as_ref())
                && email
                    .as_ref()
                    .is_none_or(|value| Some(value) == before.email.as_ref())
            {
                return Ok(StorageMutationOutcome::unchanged(before.into_storage()?));
            }
            let password_changed = password.is_some();
            let changes = UpdateUserRow {
                password: password.as_ref(),
                proper_name: proper_name.as_ref(),
                email: email.as_ref(),
            };
            let after = diesel::update(
                crate::schema::users::table.filter(crate::schema::users::id.eq(user_id)),
            )
            .set(changes)
            .get_result::<UserRow>(connection)
            .await?;
            if password_changed {
                revoke_all_tokens(connection, user_id).await?;
            }
            let after_revision = principal_revision(connection, user_id).await?;
            let document = AuditDocument::try_new(
                format!("User '{name}' updated"),
                Some(before.snapshot(&name, before_revision)),
                Some(after.snapshot(&name, after_revision)),
                json!({ "password_changed": password_changed }),
            )?;
            let event = user_event(&after, &name, Action::Updated, &context, document)?;
            let audit = append_event(connection, &event).await?.into_audit_receipt();
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(
                after.into_storage()?,
                audit,
            ))
        })
        .await
}

pub async fn set_user_password(
    runtime: &PostgresRuntime,
    request: StorageUserPasswordUpdate,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    let (user_id, password_hash, context) = request.into_parts();
    let user_id = user_id.id();
    validate_positive_id(user_id, "user id")?;
    runtime
        .with_transaction(async move |connection| {
            set_user_password_on_connection(connection, user_id, password_hash, &context, false)
                .await
        })
        .await
}

pub(crate) async fn set_user_password_on_connection(
    connection: &mut PostgresConnection,
    user_id: i32,
    password_hash: String,
    context: &EventContext,
    credential_reset: bool,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    let before_revision = lock_principal_revision(connection, user_id).await?;
    ensure_user_allows_local_write(connection, user_id).await?;
    let (before, name) = load_user_with_name(connection, user_id).await?;
    diesel::update(crate::schema::users::table.filter(crate::schema::users::id.eq(user_id)))
        .set(crate::schema::users::password.eq(Some(password_hash)))
        .execute(connection)
        .await?;
    let revoked = revoke_all_tokens(connection, user_id).await?;
    let after = load_user_row(connection, user_id).await?;
    let after_revision = principal_revision(connection, user_id).await?;
    let document = AuditDocument::try_new(
        format!("User '{name}' password changed"),
        Some(before.snapshot(&name, before_revision)),
        Some(after.snapshot(&name, after_revision)),
        json!({
            "password_changed": true,
            "revoked_token_count": revoked,
            "credential_reset": credential_reset,
        }),
    )?;
    let event = user_event(&after, &name, Action::Updated, context, document)?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(revoked, audit))
}

pub async fn delete_user(
    runtime: &PostgresRuntime,
    request: StorageUserDelete,
) -> Result<StorageMutationOutcome<usize>, PostgresStorageError> {
    let (user_id, context) = request.into_parts();
    let user_id = user_id.id();
    validate_positive_id(user_id, "user id")?;
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_principal_revision(connection, user_id).await?;
            ensure_user_allows_local_write(connection, user_id).await?;
            let (user, name) = load_user_with_name(connection, user_id).await?;
            let deleted = diesel::delete(
                crate::schema::principals::table.filter(crate::schema::principals::id.eq(user_id)),
            )
            .execute(connection)
            .await?;
            let document = AuditDocument::try_new(
                format!("User '{name}' deleted"),
                Some(user.snapshot(&name, before_revision)),
                None,
                json!({}),
            )?;
            let event = user_event(&user, &name, Action::Deleted, &context, document)?;
            let audit = append_event(connection, &event).await?.into_audit_receipt();
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed(deleted, audit))
        })
        .await
}

pub async fn anonymize_user(
    runtime: &PostgresRuntime,
    request: StorageUserAnonymize,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    let (user_id, context) = request.into_parts();
    let user_id = user_id.id();
    validate_positive_id(user_id, "user id")?;
    runtime
        .with_transaction(async move |connection| {
            let before_revision = lock_principal_revision(connection, user_id).await?;
            ensure_user_allows_local_write(connection, user_id).await?;
            let (before, name) = load_user_with_name(connection, user_id).await?;
            if before.anonymized_at.is_some() {
                return Ok(StorageMutationOutcome::unchanged(()));
            }
            diesel::delete(
                crate::schema::computed_field_definitions::table
                    .filter(
                        crate::schema::computed_field_definitions::owner_user_id.eq(Some(user_id)),
                    )
                    .filter(crate::schema::computed_field_definitions::visibility.eq("personal")),
            )
            .execute(connection)
            .await?;
            let updated = diesel::update(
                crate::schema::users::table.filter(crate::schema::users::id.eq(user_id)),
            )
            .set((
                crate::schema::users::proper_name.eq::<Option<String>>(None),
                crate::schema::users::email.eq::<Option<String>>(None),
                crate::schema::users::password.eq(Some(ANONYMIZED_PASSWORD)),
                crate::schema::users::anonymized_at.eq(diesel::dsl::now),
            ))
            .execute(connection)
            .await?;
            if updated == 0 {
                return Err(PostgresStorageError::not_found(format!(
                    "User {user_id} not found"
                )));
            }
            diesel::update(
                crate::schema::principals::table.filter(crate::schema::principals::id.eq(user_id)),
            )
            .set(crate::schema::principals::name.eq(format!("anonymized-{user_id}")))
            .execute(connection)
            .await?;
            revoke_all_tokens(connection, user_id).await?;
            let after = load_user_row(connection, user_id).await?;
            let anonymized_name = principal_name(connection, user_id).await?;
            let after_revision = principal_revision(connection, user_id).await?;
            let document = AuditDocument::try_new(
                format!("User '{name}' anonymized"),
                Some(before.snapshot(&name, before_revision)),
                Some(after.snapshot(&anonymized_name, after_revision)),
                json!({ "anonymized": true }),
            )?;
            let event = user_event(&after, &name, Action::Updated, &context, document)?;
            let audit = append_event(connection, &event).await?.into_audit_receipt();
            Ok::<_, PostgresStorageError>(StorageMutationOutcome::committed((), audit))
        })
        .await
}

async fn load_user_row(
    connection: &mut PostgresConnection,
    user_id: i32,
) -> Result<UserRow, diesel::result::Error> {
    crate::schema::users::table
        .filter(crate::schema::users::id.eq(user_id))
        .select(UserRow::as_select())
        .first(connection)
        .await
}

async fn load_user_with_name(
    connection: &mut PostgresConnection,
    user_id: i32,
) -> Result<(UserRow, String), diesel::result::Error> {
    crate::schema::users::table
        .inner_join(
            crate::schema::principals::table
                .on(crate::schema::users::id.eq(crate::schema::principals::id)),
        )
        .filter(crate::schema::users::id.eq(user_id))
        .select((UserRow::as_select(), crate::schema::principals::name))
        .first(connection)
        .await
}

async fn local_identity_scope_id(
    connection: &mut PostgresConnection,
) -> Result<i32, diesel::result::Error> {
    crate::schema::identity_scopes::table
        .filter(crate::schema::identity_scopes::name.eq(LOCAL_IDENTITY_SCOPE))
        .select(crate::schema::identity_scopes::id)
        .first(connection)
        .await
}

async fn principal_name(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<String, diesel::result::Error> {
    crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::name)
        .first(connection)
        .await
}

async fn principal_revision(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<PostgresRevision, diesel::result::Error> {
    crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::revision)
        .first(connection)
        .await
}

async fn lock_principal_revision(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<PostgresRevision, PostgresStorageError> {
    let revision = crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await?;
    assert_locked_revision_precondition(
        connection,
        &RevisionOwner::Principal.key(principal_id),
        revision,
    )
    .await?;
    Ok(revision)
}

async fn ensure_user_allows_local_write(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<(), PostgresStorageError> {
    let provider_managed = crate::schema::principals::table
        .filter(crate::schema::principals::id.eq(principal_id))
        .select(crate::schema::principals::provider_managed)
        .first::<bool>(connection)
        .await?;
    if provider_managed {
        Err(PostgresStorageError::permission_denied(
            "Provider-managed users are read-only in Hubuum",
        ))
    } else {
        Ok(())
    }
}

async fn revoke_all_tokens(
    connection: &mut PostgresConnection,
    principal_id: i32,
) -> Result<usize, PostgresStorageError> {
    diesel::update(
        crate::schema::tokens::table
            .filter(crate::schema::tokens::principal_id.eq(principal_id))
            .filter(crate::schema::tokens::revoked_at.is_null()),
    )
    .set(crate::schema::tokens::revoked_at.eq(diesel::dsl::now))
    .execute(connection)
    .await
    .map_err(PostgresStorageError::from)
}

fn user_cursor_field(field: &FilterField) -> Result<CursorSqlField, PostgresStorageError> {
    Ok(match field {
        FilterField::Id => cursor_field("users.id", CursorSqlType::Integer, false),
        FilterField::Name | FilterField::Username => {
            cursor_field("principals.name", CursorSqlType::String, false)
        }
        FilterField::IdentityScope => {
            cursor_field("identity_scopes.name", CursorSqlType::String, false)
        }
        FilterField::ProperName => cursor_field("users.proper_name", CursorSqlType::String, true),
        FilterField::Email => cursor_field("users.email", CursorSqlType::String, true),
        FilterField::CreatedAt => cursor_field("users.created_at", CursorSqlType::DateTime, false),
        FilterField::UpdatedAt => cursor_field("users.updated_at", CursorSqlType::DateTime, false),
        FilterField::Revision => cursor_field("principals.revision", CursorSqlType::BigInt, false),
        _ => {
            return Err(PostgresStorageError::invalid_input(format!(
                "Field '{field}' is not orderable for users"
            )));
        }
    })
}

const fn cursor_field(
    column: &'static str,
    sql_type: CursorSqlType,
    nullable: bool,
) -> CursorSqlField {
    CursorSqlField {
        column,
        sql_type,
        nullable,
    }
}

fn user_event(
    user: &UserRow,
    name: &str,
    action: Action,
    context: &EventContext,
    document: AuditDocument,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::from_document(EntityType::User, action, context.actor_kind(), document)
        .map_err(|error| PostgresStorageError::database(error.to_string()))
        .and_then(|event| {
            Ok(event
                .with_context(context)
                .with_entity_id(hubuum_events_core::EventEntityId::new(user.id)?)
                .with_entity_name(name.to_string()))
        })
}

fn validate_positive_id(id: i32, field: &str) -> Result<(), PostgresStorageError> {
    if id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "{field} must be greater than zero"
        )))
    }
}
