use diesel::{ExpressionMethods, QueryDsl, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{BoundedJsonPatch, IdentityScopeId, JsonPatchErrorKind, PrincipalKind};
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    MutationOutcome, StoragePrincipal, StoragePrincipalSettings, StoragePrincipalSettingsMutation,
};
use serde_json::{Map, Value, json};

use crate::operations::event_record::append_event;
use crate::revision::{RevisionOwner, record_metadata};
use crate::runtime::assert_locked_revision_precondition;
use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::principals)]
pub(crate) struct PrincipalRow {
    pub(crate) id: i32,
    pub(crate) kind: String,
    pub(crate) name: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) identity_scope_id: i32,
    pub(crate) provider_managed: bool,
    pub(crate) settings: Value,
    pub(crate) external_subject: Option<String>,
    pub(crate) last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub(crate) last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub(crate) revision: PostgresRevision,
}

impl PrincipalRow {
    pub(crate) fn into_storage(self) -> Result<StoragePrincipal, PostgresStorageError> {
        let kind = self
            .kind
            .parse::<PrincipalKind>()
            .map_err(|error| PostgresStorageError::database(error.to_string()))?;
        Ok(StoragePrincipal::builder(
            record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
            kind,
            self.name,
            IdentityScopeId::new(self.identity_scope_id)?,
        )
        .provider_managed(self.provider_managed)
        .settings(self.settings)
        .external_subject(self.external_subject)
        .last_sync_attempted_at(
            self.last_sync_attempted_at
                .map(|timestamp| timestamp.and_utc()),
        )
        .last_sync_success_at(
            self.last_sync_success_at
                .map(|timestamp| timestamp.and_utc()),
        )
        .build())
    }
}

/// Load one principal without exposing the PostgreSQL principal row.
pub async fn get_principal(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<StoragePrincipal, PostgresStorageError> {
    validate_principal_id(principal_id)?;
    runtime
        .with_connection(async |connection| {
            crate::schema::principals::table
                .filter(crate::schema::principals::id.eq(principal_id))
                .select(PrincipalRow::as_select())
                .first::<PrincipalRow>(connection)
                .await
        })
        .await?
        .into_storage()
}

/// Load one principal-settings document and its owning revision.
pub async fn get_principal_settings(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<StoragePrincipalSettings, PostgresStorageError> {
    validate_principal_id(principal_id)?;
    let (document, revision) = runtime
        .with_connection(async |connection| {
            crate::schema::principals::table
                .filter(crate::schema::principals::id.eq(principal_id))
                .select((
                    crate::schema::principals::settings,
                    crate::schema::principals::revision,
                ))
                .first::<(Value, PostgresRevision)>(connection)
                .await
        })
        .await?;
    validate_stored_settings(principal_id, &document)?;
    Ok(StoragePrincipalSettings::new(
        hubuum_domain::PrincipalId::new(principal_id)?,
        revision.into_domain(),
        document,
    ))
}

/// Atomically mutate principal settings and append their audit event.
pub async fn update_principal_settings(
    runtime: &PostgresRuntime,
    principal_id: i32,
    mutation: StoragePrincipalSettingsMutation,
    event_context: &EventContext,
) -> Result<MutationOutcome<StoragePrincipalSettings>, PostgresStorageError> {
    validate_principal_id(principal_id)?;
    runtime
        .with_transaction(
            async |connection| -> Result<MutationOutcome<StoragePrincipalSettings>, PostgresStorageError> {
                let (kind, name, before, before_revision) = crate::schema::principals::table
                    .filter(crate::schema::principals::id.eq(principal_id))
                    .select((
                        crate::schema::principals::kind,
                        crate::schema::principals::name,
                        crate::schema::principals::settings,
                        crate::schema::principals::revision,
                    ))
                    .for_update()
                    .first::<(String, String, Value, PostgresRevision)>(connection)
                    .await?;
                let kind = kind
                    .parse::<PrincipalKind>()
                    .map_err(|error| PostgresStorageError::database(error.to_string()))?;
                assert_locked_revision_precondition(
                    connection,
                    &RevisionOwner::Principal.key(principal_id),
                    before_revision,
                )
                .await?;
                validate_stored_settings(principal_id, &before)?;
                let after = apply_settings_mutation(before.clone(), mutation)?;

                if before == after {
                    return Ok(MutationOutcome::unchanged(StoragePrincipalSettings::new(
                        hubuum_domain::PrincipalId::new(principal_id)?,
                        before_revision.into_domain(),
                        after,
                    )));
                }

                let after_revision = diesel::update(
                    crate::schema::principals::table
                        .filter(crate::schema::principals::id.eq(principal_id)),
                )
                .set(crate::schema::principals::settings.eq(&after))
                .returning(crate::schema::principals::revision)
                .get_result::<PostgresRevision>(connection)
                .await?;

                let entity_type = principal_entity_type(kind);
                let event = NewEvent::new(
                    entity_type,
                    Action::Updated,
                    event_context.actor_kind(),
                    format!("Principal settings for '{name}' updated"),
                )
                .map_err(|error| PostgresStorageError::database(error.to_string()))?
                .with_context(event_context)
                .with_entity_id(hubuum_events_core::EventEntityId::new(principal_id)?)
                .with_entity_name(name)
                .with_before(json!({ "revision": before_revision, "settings": before }))
                .with_after(json!({ "revision": after_revision, "settings": after }));
                let audit = append_event(connection, &event).await?.into_audit_receipt()?;

                Ok(MutationOutcome::committed(StoragePrincipalSettings::new(
                    hubuum_domain::PrincipalId::new(principal_id)?,
                    after_revision.into_domain(),
                    after,
                ), audit))
            },
        )
        .await
}

fn apply_settings_mutation(
    before: Value,
    mutation: StoragePrincipalSettingsMutation,
) -> Result<Value, PostgresStorageError> {
    match mutation {
        StoragePrincipalSettingsMutation::Replace(document) => validate_input_settings(document),
        StoragePrincipalSettingsMutation::MergePatch(patch) => {
            let patch = validate_input_settings(patch)?;
            let mut after = before;
            merge_settings_objects(
                after
                    .as_object_mut()
                    .expect("stored settings were validated as an object"),
                patch
                    .as_object()
                    .expect("settings merge patch was validated as an object"),
            );
            Ok(after)
        }
        StoragePrincipalSettingsMutation::JsonPatch(document) => {
            let patch = serde_json::from_value::<BoundedJsonPatch>(document)
                .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
            let after = patch.apply(&before).map_err(|error| {
                let (kind, message) = error.into_parts();
                match kind {
                    JsonPatchErrorKind::BadRequest => PostgresStorageError::invalid_input(message),
                    JsonPatchErrorKind::Conflict => PostgresStorageError::conflict(message),
                    JsonPatchErrorKind::PayloadTooLarge => {
                        PostgresStorageError::input_too_large(message)
                    }
                }
            })?;
            validate_input_settings(after)
        }
        StoragePrincipalSettingsMutation::Reset => Ok(json!({})),
    }
}

fn merge_settings_objects(target: &mut Map<String, Value>, patch: &Map<String, Value>) {
    for (key, patch_value) in patch {
        match patch_value {
            Value::Null => {
                target.remove(key);
            }
            Value::Object(patch_object) => {
                let target_value = target.entry(key.clone()).or_insert_with(|| json!({}));
                if !target_value.is_object() {
                    *target_value = json!({});
                }
                merge_settings_objects(
                    target_value
                        .as_object_mut()
                        .expect("replacement settings value is an object"),
                    patch_object,
                );
            }
            _ => {
                target.insert(key.clone(), patch_value.clone());
            }
        }
    }
}

fn validate_input_settings(document: Value) -> Result<Value, PostgresStorageError> {
    if document.is_object() {
        Ok(document)
    } else {
        Err(PostgresStorageError::invalid_input(
            "principal settings must be a JSON object",
        ))
    }
}

fn validate_stored_settings(
    principal_id: i32,
    document: &Value,
) -> Result<(), PostgresStorageError> {
    if document.is_object() {
        Ok(())
    } else {
        Err(PostgresStorageError::internal(format!(
            "Principal '{principal_id}' has invalid settings in the database"
        )))
    }
}

const fn principal_entity_type(kind: PrincipalKind) -> EntityType {
    match kind {
        PrincipalKind::Human => EntityType::User,
        PrincipalKind::ServiceAccount => EntityType::ServiceAccount,
    }
}

fn validate_principal_id(principal_id: i32) -> Result<(), PostgresStorageError> {
    if principal_id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(
            "Invalid principal ID: expected a positive integer",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_patch_recurses_and_removes_null_members() {
        let before = json!({
            "appearance": { "theme": "light", "density": "compact" },
            "locale": "en",
        });
        let patch = json!({
            "appearance": { "theme": "dark", "density": null },
            "locale": null,
        });

        let after =
            apply_settings_mutation(before, StoragePrincipalSettingsMutation::MergePatch(patch))
                .expect("merge patch should be valid");

        assert_eq!(after, json!({ "appearance": { "theme": "dark" } }));
    }

    #[test]
    fn settings_mutations_cannot_replace_the_object_root() {
        let error = apply_settings_mutation(
            json!({}),
            StoragePrincipalSettingsMutation::Replace(json!(["invalid"])),
        )
        .expect_err("non-object settings must be rejected");

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }
}
