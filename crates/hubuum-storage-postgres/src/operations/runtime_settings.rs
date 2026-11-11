//! Revisioned, audited runtime settings shared by every replica.
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use hubuum_domain::{PrincipalId, ResourceRevision};
use hubuum_events_core::{ActorKind, CorrelationId};
use hubuum_storage_core::{
    StorageRuntimeSettingsActor, StorageRuntimeSettingsAssignment, StorageRuntimeSettingsChange,
    StorageRuntimeSettingsGroup, StorageRuntimeSettingsHistoryQuery, StorageRuntimeSettingsRecord,
    StorageRuntimeSettingsSnapshot, StorageRuntimeSettingsUpdate,
    StorageRuntimeSettingsUpdateOutcome,
};
use serde_json::Value;
use uuid::Uuid;

use crate::{PostgresRuntime, PostgresStorageError, validate_persisted};

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::runtime_settings)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct RuntimeSettingsRow {
    setting_group: String,
    revision: i64,
    value: Option<Value>,
    changed_at: DateTime<Utc>,
    changed_by: Option<i32>,
}

fn group(value: &str) -> Result<StorageRuntimeSettingsGroup, PostgresStorageError> {
    validate_persisted(
        "runtime settings group",
        StorageRuntimeSettingsGroup::parse(value),
    )
}

fn assignment(
    group: StorageRuntimeSettingsGroup,
    value: Option<&Value>,
) -> Result<StorageRuntimeSettingsAssignment, PostgresStorageError> {
    validate_persisted(
        "runtime settings value",
        StorageRuntimeSettingsAssignment::from_document(group, value),
    )
}

fn revision(value: i64) -> Result<ResourceRevision, PostgresStorageError> {
    validate_persisted("runtime settings revision", ResourceRevision::new(value))
}

impl RuntimeSettingsRow {
    fn record(self) -> Result<StorageRuntimeSettingsRecord, PostgresStorageError> {
        let group = group(&self.setting_group)?;
        Ok(StorageRuntimeSettingsRecord::new(
            assignment(group, self.value.as_ref())?,
            revision(self.revision)?,
            self.changed_at,
            self.changed_by.map(PrincipalId::new).transpose()?,
        ))
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::runtime_setting_changes)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct RuntimeSettingChangeRow {
    setting_group: String,
    revision: i64,
    previous_value: Option<Value>,
    value: Option<Value>,
    changed_at: DateTime<Utc>,
    actor_kind: String,
    actor_principal_id: Option<i32>,
    request_id: Option<Uuid>,
    correlation_id: Option<String>,
}

impl RuntimeSettingChangeRow {
    fn change(self) -> Result<StorageRuntimeSettingsChange, PostgresStorageError> {
        let group = group(&self.setting_group)?;
        let actor = StorageRuntimeSettingsActor::new(
            validate_persisted(
                "runtime settings actor kind",
                ActorKind::parse(&self.actor_kind),
            )?,
            self.actor_principal_id.map(PrincipalId::new).transpose()?,
            self.request_id,
            self.correlation_id
                .map(|value| {
                    validate_persisted("runtime settings correlation id", CorrelationId::new(value))
                })
                .transpose()?,
        );
        validate_persisted(
            "runtime settings change",
            StorageRuntimeSettingsChange::try_new(
                revision(self.revision)?,
                assignment(group, self.previous_value.as_ref())?,
                assignment(group, self.value.as_ref())?,
                self.changed_at,
                actor,
            ),
        )
    }
}

pub async fn load_runtime_settings(
    runtime: &PostgresRuntime,
) -> Result<StorageRuntimeSettingsSnapshot, PostgresStorageError> {
    use crate::schema::runtime_settings::dsl as settings;
    runtime
        .with_read_connection(async |connection| -> Result<_, PostgresStorageError> {
            let records = settings::runtime_settings
                .select(RuntimeSettingsRow::as_select())
                .load(connection)
                .await?
                .into_iter()
                .map(RuntimeSettingsRow::record)
                .collect::<Result<Vec<_>, _>>()?;
            validate_persisted(
                "runtime settings snapshot",
                StorageRuntimeSettingsSnapshot::try_new(records),
            )
        })
        .await
}

pub async fn update_runtime_settings(
    runtime: &PostgresRuntime,
    request: StorageRuntimeSettingsUpdate,
) -> Result<StorageRuntimeSettingsUpdateOutcome, PostgresStorageError> {
    use crate::schema::runtime_setting_changes::dsl as changes;
    use crate::schema::runtime_settings::dsl as settings;
    runtime
        .with_transaction(async |connection| -> Result<_, PostgresStorageError> {
            let group = request.group();
            let current = settings::runtime_settings
                .filter(settings::setting_group.eq(group.as_str()))
                .for_update()
                .select(RuntimeSettingsRow::as_select())
                .first(connection)
                .await?
                .record()?;
            if current.revision() != request.expected_revision() {
                return Err(PostgresStorageError::revision_conflict(
                    format!("The {} runtime settings revision changed", group.as_str()),
                    current.revision(),
                ));
            }
            if current.assignment() == request.assignment() {
                return Ok(StorageRuntimeSettingsUpdateOutcome::Unchanged(current));
            }
            let next = current
                .revision()
                .checked_advance()
                .map_err(|error| PostgresStorageError::internal(error.to_string()))?;
            let context = request.context();
            let record = diesel::update(settings::runtime_settings)
                .filter(settings::setting_group.eq(group.as_str()))
                .set((
                    settings::revision.eq(next.get()),
                    settings::value.eq(request.assignment().to_document()),
                    settings::changed_at.eq(diesel::dsl::now),
                    settings::changed_by.eq(context.actor_user_id().map(PrincipalId::id)),
                ))
                .returning(RuntimeSettingsRow::as_returning())
                .get_result(connection)
                .await?
                .record()?;
            diesel::insert_into(changes::runtime_setting_changes)
                .values((
                    changes::setting_group.eq(group.as_str()),
                    changes::revision.eq(next.get()),
                    changes::previous_value.eq(current.assignment().to_document()),
                    changes::value.eq(request.assignment().to_document()),
                    changes::changed_at.eq(record.changed_at()),
                    changes::actor_kind.eq(context.actor_kind().as_str()),
                    changes::actor_principal_id.eq(context.actor_user_id().map(PrincipalId::id)),
                    changes::request_id.eq(context.request_id()),
                    changes::correlation_id.eq(context.correlation_id().map(CorrelationId::as_str)),
                ))
                .execute(connection)
                .await?;
            crate::worker_notifications::notify_runtime_settings(connection, group).await?;
            Ok(StorageRuntimeSettingsUpdateOutcome::Committed(record))
        })
        .await
}

pub async fn list_runtime_settings_changes(
    runtime: &PostgresRuntime,
    query: StorageRuntimeSettingsHistoryQuery,
) -> Result<Vec<StorageRuntimeSettingsChange>, PostgresStorageError> {
    use crate::schema::runtime_setting_changes::dsl as changes;
    runtime
        .with_read_connection(async |connection| -> Result<_, PostgresStorageError> {
            let mut statement = changes::runtime_setting_changes
                .filter(changes::setting_group.eq(query.group().as_str()))
                .into_boxed();
            if let Some(before) = query.before() {
                statement = statement.filter(changes::revision.lt(before.get()));
            }
            statement
                .order(changes::revision.desc())
                .limit(i64::try_from(query.limit()).expect("history pages are bounded"))
                .select(RuntimeSettingChangeRow::as_select())
                .load(connection)
                .await?
                .into_iter()
                .map(RuntimeSettingChangeRow::change)
                .collect()
        })
        .await
}
