//! Adapter-owned integration-test probes.
//!
//! These helpers deliberately expose crate-owned values rather than Diesel
//! connections or rows. They are compiled only when the explicit
//! `integration-test-support` feature is enabled.

use chrono::NaiveDateTime;
use diesel::sql_types::{Bool, Integer, Timestamp};
use diesel::{
    ExpressionMethods, Insertable, NullableExpressionMethods, OptionalExtension, QueryDsl,
    Queryable, QueryableByName, Selectable, SelectableHelper,
};
use diesel_async::RunQueryDsl;
use hubuum_domain::{
    EventDeliveryId, EventDeliverySettings, EventDeliveryStatus, GroupId, PrincipalId,
    ResourceRevision, RestoreJobId, TaskId,
};
use hubuum_events_core::{Action, EntityType, EventEntityId, EventSequence, NewEvent};
use hubuum_storage_core::{
    StorageEventDelivery, StorageEventDeliveryWorkItem, StorageExportTaskArtifact,
    StorageIdentityGroup, StorageRecordedEvent, StorageTask, StorageTaskClaim,
    StorageTaskClaimToken, StorageTaskKind, StorageTaskLease, StorageTaskLeaseDuration,
    StorageTaskProgress, StorageTaskScopeSnapshot, StorageTaskStatus,
};
use tokio::time::{Duration, Instant, sleep};

use crate::{
    DatabaseRoleNames, PostgresConnection, PostgresPool, PostgresPoolSettings,
    PostgresStorageError, build_postgres_pool,
};

/// Return the migrated database URL selected by the repository test runner.
///
/// Environment translation stays in this adapter-owned support module instead
/// of being repeated by each integration-test binary.
#[must_use]
pub fn integration_test_database_url() -> String {
    std::env::var("HUBUUM_DATABASE_URL")
        .expect("HUBUUM_DATABASE_URL must identify the migrated integration-test database")
}

/// Build a pool for an adapter integration test against the migrated database
/// selected by the repository test runner.
///
/// Environment translation stays in this adapter-owned support module instead
/// of being repeated by each integration-test binary.
#[must_use]
pub fn integration_test_pool(max_size: u32) -> PostgresPool {
    let database_url = integration_test_database_url();
    let settings = PostgresPoolSettings::builder(database_url)
        .max_size(max_size)
        .statement_timeout_ms(0)
        .acquire_timeout_ms(5_000)
        .build()
        .expect("integration-test PostgreSQL settings must be valid");
    build_postgres_pool(&settings).expect("integration-test PostgreSQL pool must be constructed")
}

/// Whether the repository runner provisioned the dedicated database-role fixture.
#[must_use]
pub fn database_role_tests_enabled() -> bool {
    std::env::var("HUBUUM_DATABASE_ROLE_TESTS").as_deref() == Ok("true")
}

/// Role names provisioned by the repository integration-test runner.
#[must_use]
pub fn integration_test_database_roles() -> DatabaseRoleNames {
    DatabaseRoleNames::new(
        std::env::var("HUBUUM_DATABASE_OWNER_ROLE").expect("test owner role"),
        std::env::var("HUBUUM_DATABASE_MIGRATOR_ROLE").expect("test migrator role"),
        std::env::var("HUBUUM_DATABASE_RUNTIME_ROLE").expect("test runtime role"),
    )
    .expect("test role names")
}

/// Build a pool authenticated as the repository test migrator.
#[must_use]
pub fn integration_test_migration_pool(max_size: u32) -> PostgresPool {
    let database_url = std::env::var("HUBUUM_MIGRATION_DATABASE_URL")
        .expect("HUBUUM_MIGRATION_DATABASE_URL must identify the migrated test database");
    let settings = PostgresPoolSettings::builder(database_url)
        .max_size(max_size)
        .statement_timeout_ms(0)
        .acquire_timeout_ms(5_000)
        .build()
        .expect("integration-test migration settings must be valid");
    build_postgres_pool(&settings).expect("integration-test migration pool must be constructed")
}

/// Persisted credential evidence needed by request-level authentication tests.
///
/// The raw bearer value is never accepted here; `token_hash` is the one-way
/// persisted lookup value.
pub struct PersistedTestToken {
    id: i32,
    token_hash: String,
    principal_id: i32,
    name: Option<String>,
    description: Option<String>,
    issued: NaiveDateTime,
    expires_at: Option<NaiveDateTime>,
    last_used_at: Option<NaiveDateTime>,
    revoked_at: Option<NaiveDateTime>,
    permission_scoped: bool,
    resource_scoped: bool,
    revision: ResourceRevision,
}

/// Persisted remote-call artifact evidence for request-level tests.
pub struct PersistedRemoteCallResult {
    id: i32,
    task_id: i32,
    target_id: Option<i32>,
    subject_type: String,
    subject_id: i32,
    method: String,
    rendered_url: String,
    response_status: Option<i32>,
    response_headers: Option<serde_json::Value>,
    response_body_preview: Option<String>,
    duration_ms: i32,
    success: bool,
    error: Option<String>,
    created_at: NaiveDateTime,
}

/// Valid task state used by application and adapter integration fixtures.
pub struct TestTaskCreate {
    kind: StorageTaskKind,
    status: StorageTaskStatus,
    submitted_by: Option<hubuum_domain::PrincipalId>,
    request_payload: Option<serde_json::Value>,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
    summary: Option<String>,
    progress: StorageTaskProgress,
    scope_snapshot: StorageTaskScopeSnapshot,
    terminal_at: Option<NaiveDateTime>,
    initiator_principal_id: Option<hubuum_domain::PrincipalId>,
}

/// Computed-object materialization fixture input.
pub struct TestComputedObjectDataCreate {
    object_id: hubuum_domain::ObjectId,
    class_id: hubuum_domain::ClassId,
    evaluation_revision: i64,
    source_data_sha256: String,
    values: serde_json::Value,
    errors: serde_json::Value,
}

impl TestComputedObjectDataCreate {
    #[must_use]
    pub fn new(
        object_id: hubuum_domain::ObjectId,
        class_id: hubuum_domain::ClassId,
        evaluation_revision: i64,
        source_data_sha256: impl Into<String>,
        values: serde_json::Value,
    ) -> Self {
        Self {
            object_id,
            class_id,
            evaluation_revision,
            source_data_sha256: source_data_sha256.into(),
            values,
            errors: serde_json::json!({}),
        }
    }

    #[must_use]
    pub fn errors(mut self, value: serde_json::Value) -> Self {
        self.errors = value;
        self
    }
}

/// Typed computed-object materialization evidence.
pub struct TestComputedObjectData {
    object_id: hubuum_domain::ObjectId,
    class_id: hubuum_domain::ClassId,
    evaluation_revision: i64,
    source_data_sha256: String,
    values: serde_json::Value,
    errors: serde_json::Value,
    computed_at: NaiveDateTime,
}

impl TestComputedObjectData {
    pub const fn object_id(&self) -> hubuum_domain::ObjectId {
        self.object_id
    }
    pub const fn class_id(&self) -> hubuum_domain::ClassId {
        self.class_id
    }
    pub const fn evaluation_revision(&self) -> i64 {
        self.evaluation_revision
    }
    pub fn source_data_sha256(&self) -> &str {
        &self.source_data_sha256
    }
    pub const fn values(&self) -> &serde_json::Value {
        &self.values
    }
    pub const fn errors(&self) -> &serde_json::Value {
        &self.errors
    }
    pub const fn computed_at(&self) -> NaiveDateTime {
        self.computed_at
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::object_computed_data)]
struct TestComputedObjectDataCreateRow {
    object_id: i32,
    class_id: i32,
    evaluation_revision: i64,
    source_data_sha256: String,
    values: serde_json::Value,
    errors: serde_json::Value,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::object_computed_data)]
struct TestComputedObjectDataRow {
    object_id: i32,
    class_id: i32,
    evaluation_revision: i64,
    source_data_sha256: String,
    values: serde_json::Value,
    errors: serde_json::Value,
    computed_at: NaiveDateTime,
}

impl TestTaskCreate {
    #[must_use]
    pub fn new(
        kind: StorageTaskKind,
        status: StorageTaskStatus,
        submitted_by: hubuum_domain::PrincipalId,
    ) -> Self {
        Self {
            kind,
            status,
            submitted_by: Some(submitted_by),
            request_payload: Some(serde_json::json!({})),
            idempotency_key: None,
            request_hash: None,
            summary: None,
            progress: StorageTaskProgress::try_new(0, 0, 0, 0)
                .expect("zero task progress should be valid"),
            scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
            terminal_at: None,
            initiator_principal_id: None,
        }
    }

    #[must_use]
    pub fn internal_reindex(status: StorageTaskStatus) -> Self {
        Self {
            kind: StorageTaskKind::Reindex,
            status,
            submitted_by: None,
            request_payload: Some(serde_json::json!({})),
            idempotency_key: None,
            request_hash: None,
            summary: None,
            progress: StorageTaskProgress::try_new(0, 0, 0, 0)
                .expect("zero task progress should be valid"),
            scope_snapshot: StorageTaskScopeSnapshot::unscoped(),
            terminal_at: None,
            initiator_principal_id: None,
        }
    }

    /// Build a historical terminal task that predates principal attribution.
    pub fn internal_completed(
        kind: StorageTaskKind,
        status: StorageTaskStatus,
    ) -> Result<Self, PostgresStorageError> {
        if !status.is_terminal() {
            return Err(PostgresStorageError::invalid_input(
                "unattributed non-reindex test tasks must be terminal",
            ));
        }
        let mut request = Self::internal_reindex(status);
        request.kind = kind;
        Ok(request)
    }

    #[must_use]
    pub fn request_payload(mut self, value: Option<serde_json::Value>) -> Self {
        self.request_payload = value;
        self
    }

    #[must_use]
    pub fn idempotency_key(mut self, value: Option<String>) -> Self {
        self.idempotency_key = value;
        self
    }

    #[must_use]
    pub fn request_hash(mut self, value: Option<String>) -> Self {
        self.request_hash = value;
        self
    }

    #[must_use]
    pub fn summary(mut self, value: Option<String>) -> Self {
        self.summary = value;
        self
    }

    #[must_use]
    pub const fn progress(mut self, value: StorageTaskProgress) -> Self {
        self.progress = value;
        self
    }

    #[must_use]
    pub fn scope_snapshot(mut self, value: StorageTaskScopeSnapshot) -> Self {
        self.scope_snapshot = value;
        self
    }

    #[must_use]
    pub const fn terminal_at(mut self, value: NaiveDateTime) -> Self {
        self.terminal_at = Some(value);
        self
    }

    #[must_use]
    pub const fn initiator_principal_id(
        mut self,
        value: Option<hubuum_domain::PrincipalId>,
    ) -> Self {
        self.initiator_principal_id = value;
        self
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::tasks)]
struct TestTaskRow {
    kind: String,
    status: String,
    submitted_by: Option<i32>,
    submitted_token_id: Option<i32>,
    submitted_token_scoped: bool,
    submitted_token_scopes: serde_json::Value,
    idempotency_key: Option<String>,
    request_hash: Option<String>,
    request_payload: Option<serde_json::Value>,
    summary: Option<String>,
    total_items: i32,
    processed_items: i32,
    success_items: i32,
    failed_items: i32,
    request_redacted_at: Option<NaiveDateTime>,
    started_at: Option<NaiveDateTime>,
    finished_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    lease_token: Option<uuid::Uuid>,
    lease_expires_at: Option<NaiveDateTime>,
    initiator_user_id: Option<i32>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::export_task_outputs)]
struct TestExportOutputRow {
    task_id: i32,
    template_name: Option<String>,
    content_type: String,
    json_output: Option<serde_json::Value>,
    text_output: Option<String>,
    meta_json: serde_json::Value,
    warnings_json: serde_json::Value,
    warning_count: i32,
    truncated: bool,
    output_expires_at: NaiveDateTime,
    total_duration_ms: i32,
    query_duration_ms: i32,
    hydration_duration_ms: i32,
    render_duration_ms: i32,
}

impl PersistedRemoteCallResult {
    pub const fn id(&self) -> i32 {
        self.id
    }
    pub const fn task_id(&self) -> i32 {
        self.task_id
    }
    pub const fn target_id(&self) -> Option<i32> {
        self.target_id
    }
    pub fn subject_type(&self) -> &str {
        &self.subject_type
    }
    pub const fn subject_id(&self) -> i32 {
        self.subject_id
    }
    pub fn method(&self) -> &str {
        &self.method
    }
    pub fn rendered_url(&self) -> &str {
        &self.rendered_url
    }
    pub const fn response_status(&self) -> Option<i32> {
        self.response_status
    }
    pub const fn response_headers(&self) -> Option<&serde_json::Value> {
        self.response_headers.as_ref()
    }
    pub fn response_body_preview(&self) -> Option<&str> {
        self.response_body_preview.as_deref()
    }
    pub const fn duration_ms(&self) -> i32 {
        self.duration_ms
    }
    pub const fn success(&self) -> bool {
        self.success
    }
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }
}

impl PersistedTestToken {
    pub const fn id(&self) -> i32 {
        self.id
    }
    pub fn token_hash(&self) -> &str {
        &self.token_hash
    }
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
    pub const fn issued(&self) -> NaiveDateTime {
        self.issued
    }
    pub const fn expires_at(&self) -> Option<NaiveDateTime> {
        self.expires_at
    }
    pub const fn last_used_at(&self) -> Option<NaiveDateTime> {
        self.last_used_at
    }
    pub const fn revoked_at(&self) -> Option<NaiveDateTime> {
        self.revoked_at
    }
    pub const fn permission_scoped(&self) -> bool {
        self.permission_scoped
    }
    pub const fn resource_scoped(&self) -> bool {
        self.resource_scoped
    }
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::tokens)]
struct PersistedTestTokenRow {
    id: i32,
    token: String,
    principal_id: i32,
    name: Option<String>,
    description: Option<String>,
    issued: NaiveDateTime,
    expires_at: Option<NaiveDateTime>,
    last_used_at: Option<NaiveDateTime>,
    revoked_at: Option<NaiveDateTime>,
    permission_scoped: bool,
    resource_scoped: bool,
    revision: crate::PostgresRevision,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::remote_call_results)]
struct PersistedRemoteCallResultRow {
    id: i32,
    task_id: i32,
    target_id: Option<i32>,
    subject_type: String,
    subject_id: i32,
    method: String,
    rendered_url: String,
    response_status: Option<i32>,
    response_headers: Option<serde_json::Value>,
    response_body_preview: Option<String>,
    duration_ms: i32,
    success: bool,
    error: Option<String>,
    created_at: NaiveDateTime,
}

/// Insert one invariant-preserving task fixture and return its typed projection.
pub async fn create_task(
    pool: &PostgresPool,
    request: TestTaskCreate,
) -> Result<StorageTask, PostgresStorageError> {
    if !request.status.is_terminal() && request.request_payload.is_none() {
        return Err(PostgresStorageError::invalid_input(
            "active test tasks require a request payload",
        ));
    }
    if request.submitted_by.is_none()
        && request.kind != StorageTaskKind::Reindex
        && !request.status.is_terminal()
    {
        return Err(PostgresStorageError::invalid_input(
            "unattributed non-reindex test tasks must be terminal",
        ));
    }
    let mut connection = pool.get().await?;
    let now = diesel::select(diesel::dsl::sql::<Timestamp>(
        "clock_timestamp() AT TIME ZONE 'UTC'",
    ))
    .get_result::<NaiveDateTime>(&mut connection)
    .await?;
    let terminal_at = request.terminal_at.unwrap_or(now);
    let created_at = if request.status.is_terminal() {
        terminal_at.min(now)
    } else {
        now
    };
    let updated_at = if request.status.is_terminal() {
        terminal_at.max(now)
    } else {
        now
    };
    let started_at = match request.status {
        StorageTaskStatus::Queued => None,
        status if status.is_terminal() => Some(created_at),
        _ => Some(now),
    };
    let finished_at = request.status.is_terminal().then_some(terminal_at);
    let request_redacted_at = request.status.is_terminal().then_some(terminal_at);
    let lease_token = (request.status == StorageTaskStatus::Running).then(uuid::Uuid::new_v4);
    let lease_expires_at = lease_token.map(|_| now + chrono::Duration::hours(1));
    let scope = request.scope_snapshot;
    let progress = request.progress;
    let row = TestTaskRow {
        kind: request.kind.as_str().to_string(),
        status: request.status.as_str().to_string(),
        submitted_by: request.submitted_by.map(hubuum_domain::PrincipalId::id),
        submitted_token_id: scope.token_id().map(hubuum_domain::TokenId::id),
        submitted_token_scoped: scope.scoped(),
        submitted_token_scopes: scope.scopes().clone(),
        idempotency_key: request.idempotency_key,
        request_hash: request.request_hash,
        request_payload: request.request_payload,
        summary: request.summary,
        total_items: progress.total(),
        processed_items: progress.processed(),
        success_items: progress.succeeded(),
        failed_items: progress.failed(),
        request_redacted_at,
        started_at,
        finished_at,
        created_at,
        updated_at,
        lease_token,
        lease_expires_at,
        initiator_user_id: request
            .initiator_principal_id
            .map(hubuum_domain::PrincipalId::id),
    };
    diesel::insert_into(crate::schema::tasks::table)
        .values(row)
        .returning(crate::operations::task_rows::TaskRow::as_returning())
        .get_result::<crate::operations::task_rows::TaskRow>(&mut connection)
        .await?
        .into_storage()
}

/// Resolve a task idempotency key within one principal namespace.
pub async fn find_task_by_idempotency(
    pool: &PostgresPool,
    principal_id: hubuum_domain::PrincipalId,
    idempotency_key_value: impl Into<String>,
) -> Result<Option<StorageTask>, PostgresStorageError> {
    use crate::schema::tasks::dsl::{idempotency_key, submitted_by, tasks};

    let mut connection = pool.get().await?;
    tasks
        .filter(submitted_by.eq(principal_id.id()))
        .filter(idempotency_key.eq(idempotency_key_value.into()))
        .select(crate::operations::task_rows::TaskRow::as_select())
        .first::<crate::operations::task_rows::TaskRow>(&mut connection)
        .await
        .optional()?
        .map(crate::operations::task_rows::TaskRow::into_storage)
        .transpose()
}

/// Claim one exact queued task for deterministic application integration tests.
pub async fn claim_task_by_id(
    pool: &PostgresPool,
    task_id_value: TaskId,
) -> Result<StorageTaskClaim, PostgresStorageError> {
    claim_task_by_id_with_lease(
        pool,
        task_id_value,
        StorageTaskLeaseDuration::from_milliseconds(60_000)
            .expect("the fixed integration-test lease duration must be positive"),
    )
    .await
}

/// Claim one exact queued task with a caller-selected lease duration.
pub async fn claim_task_by_id_with_lease(
    pool: &PostgresPool,
    task_id_value: TaskId,
    lease_duration: StorageTaskLeaseDuration,
) -> Result<StorageTaskClaim, PostgresStorageError> {
    use crate::schema::tasks::dsl::{
        attempt_count, id, lease_expires_at, lease_token, started_at, status, tasks, updated_at,
    };

    let token = uuid::Uuid::new_v4();
    let mut connection = pool.get().await?;
    let now = diesel::select(diesel::dsl::sql::<Timestamp>(
        "clock_timestamp() AT TIME ZONE 'UTC'",
    ))
    .get_result::<NaiveDateTime>(&mut connection)
    .await?;
    let row = diesel::update(
        tasks
            .filter(id.eq(task_id_value.id()))
            .filter(status.eq(StorageTaskStatus::Queued.as_str())),
    )
    .set((
        status.eq(StorageTaskStatus::Validating.as_str()),
        started_at.eq(Some(now)),
        lease_token.eq(Some(token)),
        lease_expires_at.eq(Some(
            now + chrono::Duration::milliseconds(lease_duration.milliseconds()),
        )),
        attempt_count.eq(attempt_count + 1),
        updated_at.eq(now),
    ))
    .returning(crate::operations::task_rows::TaskRow::as_returning())
    .get_result::<crate::operations::task_rows::TaskRow>(&mut connection)
    .await?;
    crate::validate_persisted(
        "task claim fixture",
        StorageTaskClaim::try_new(
            row.into_storage()?,
            StorageTaskLease::new(task_id_value, StorageTaskClaimToken::new(token.to_string())),
        ),
    )
}

/// Persist one export artifact for a terminal task fixture.
pub async fn store_export_output(
    pool: &PostgresPool,
    task_id_value: TaskId,
    artifact: StorageExportTaskArtifact,
) -> Result<(), PostgresStorageError> {
    let (identity, content, report, output_expires_at, durations) = artifact.into_parts();
    let (template_name, content_type) = identity.into_parts();
    let (json_output, text_output) = content.into_parts();
    let (meta_json, warnings_json, warning_count, truncated) = report.into_parts();
    let row = TestExportOutputRow {
        task_id: task_id_value.id(),
        template_name,
        content_type,
        json_output,
        text_output,
        meta_json,
        warnings_json,
        warning_count,
        truncated,
        output_expires_at: output_expires_at.naive_utc(),
        total_duration_ms: durations.total_ms(),
        query_duration_ms: durations.query_ms(),
        hydration_duration_ms: durations.hydration_ms(),
        render_duration_ms: durations.render_ms(),
    };
    let mut connection = pool.get().await?;
    diesel::insert_into(crate::schema::export_task_outputs::table)
        .values(row)
        .execute(&mut connection)
        .await?;
    Ok(())
}

/// Insert one computed-object materialization fixture.
pub async fn create_computed_object_data(
    pool: &PostgresPool,
    request: TestComputedObjectDataCreate,
) -> Result<(), PostgresStorageError> {
    if request.evaluation_revision <= 0 {
        return Err(PostgresStorageError::invalid_input(
            "computed evaluation revision must be positive",
        ));
    }
    if request.source_data_sha256.len() != 64
        || !request
            .source_data_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PostgresStorageError::invalid_input(
            "computed source-data SHA-256 must contain 64 hexadecimal characters",
        ));
    }
    let row = TestComputedObjectDataCreateRow {
        object_id: request.object_id.id(),
        class_id: request.class_id.id(),
        evaluation_revision: request.evaluation_revision,
        source_data_sha256: request.source_data_sha256,
        values: request.values,
        errors: request.errors,
    };
    let mut connection = pool.get().await?;
    diesel::insert_into(crate::schema::object_computed_data::table)
        .values(row)
        .execute(&mut connection)
        .await?;
    Ok(())
}

/// Load one computed-object materialization fixture.
pub async fn load_computed_object_data(
    pool: &PostgresPool,
    object_id_value: hubuum_domain::ObjectId,
) -> Result<TestComputedObjectData, PostgresStorageError> {
    use crate::schema::object_computed_data::dsl::{object_computed_data, object_id};

    let mut connection = pool.get().await?;
    let row = object_computed_data
        .filter(object_id.eq(object_id_value.id()))
        .select(TestComputedObjectDataRow::as_select())
        .first::<TestComputedObjectDataRow>(&mut connection)
        .await?;
    Ok(TestComputedObjectData {
        object_id: hubuum_domain::ObjectId::new(row.object_id)?,
        class_id: hubuum_domain::ClassId::new(row.class_id)?,
        evaluation_revision: row.evaluation_revision,
        source_data_sha256: row.source_data_sha256,
        values: row.values,
        errors: row.errors,
        computed_at: row.computed_at,
    })
}

/// Load one group by its exact persisted name.
pub async fn load_group_by_name(
    pool: &PostgresPool,
    group_name: impl Into<String>,
) -> Result<StorageIdentityGroup, PostgresStorageError> {
    crate::operations::group::load_group_by_name_for_test(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        group_name.into(),
    )
    .await
}

/// Load the persisted hash and lifecycle columns for one authentication test.
pub async fn load_token_by_hash(
    pool: &PostgresPool,
    token_hash: impl Into<String>,
) -> Result<PersistedTestToken, PostgresStorageError> {
    use crate::schema::tokens::dsl::{token, tokens};

    let mut connection = pool.get().await?;
    let row = tokens
        .filter(token.eq(token_hash.into()))
        .select(PersistedTestTokenRow::as_select())
        .first::<PersistedTestTokenRow>(&mut connection)
        .await?;
    Ok(persisted_test_token(row))
}

/// Load every active persisted token for one principal at a deterministic instant.
pub async fn load_active_tokens_for_principal(
    pool: &PostgresPool,
    principal_id: hubuum_domain::PrincipalId,
    observed_at: NaiveDateTime,
    legacy_valid_after: NaiveDateTime,
) -> Result<Vec<PersistedTestToken>, PostgresStorageError> {
    use crate::operations::authentication::active_token_predicate;
    use crate::schema::tokens::dsl::{principal_id as stored_principal_id, tokens};

    let mut connection = pool.get().await?;
    tokens
        .filter(stored_principal_id.eq(principal_id.id()))
        .filter(active_token_predicate(observed_at, legacy_valid_after))
        .select(PersistedTestTokenRow::as_select())
        .load::<PersistedTestTokenRow>(&mut connection)
        .await
        .map(|rows| rows.into_iter().map(persisted_test_token).collect())
        .map_err(Into::into)
}

fn persisted_test_token(row: PersistedTestTokenRow) -> PersistedTestToken {
    PersistedTestToken {
        id: row.id,
        token_hash: row.token,
        principal_id: row.principal_id,
        name: row.name,
        description: row.description,
        issued: row.issued,
        expires_at: row.expires_at,
        last_used_at: row.last_used_at,
        revoked_at: row.revoked_at,
        permission_scoped: row.permission_scoped,
        resource_scoped: row.resource_scoped,
        revision: row.revision.into_domain(),
    }
}

/// Load one persisted remote-call artifact by its task identifier.
pub async fn load_remote_call_result(
    pool: &PostgresPool,
    task_id_value: TaskId,
) -> Result<PersistedRemoteCallResult, PostgresStorageError> {
    use crate::schema::remote_call_results::dsl::{remote_call_results, task_id};

    let mut connection = pool.get().await?;
    let row = remote_call_results
        .filter(task_id.eq(task_id_value.id()))
        .select(PersistedRemoteCallResultRow::as_select())
        .first::<PersistedRemoteCallResultRow>(&mut connection)
        .await?;
    Ok(PersistedRemoteCallResult {
        id: row.id,
        task_id: row.task_id,
        target_id: row.target_id,
        subject_type: row.subject_type,
        subject_id: row.subject_id,
        method: row.method,
        rendered_url: row.rendered_url,
        response_status: row.response_status,
        response_headers: row.response_headers,
        response_body_preview: row.response_body_preview,
        duration_ms: row.duration_ms,
        success: row.success,
        error: row.error,
        created_at: row.created_at,
    })
}

/// Append one validated event using an adapter-owned transaction.
pub async fn append_event(
    pool: &PostgresPool,
    event: &NewEvent,
) -> Result<StorageRecordedEvent, PostgresStorageError> {
    crate::with_transaction(pool, async |connection| {
        crate::operations::event_record::append_event(connection, event).await
    })
    .await
}

/// Append one validated event inside a caller-owned adapter transaction.
pub async fn append_event_on_connection(
    connection: &mut PostgresConnection,
    event: &NewEvent,
) -> Result<StorageRecordedEvent, PostgresStorageError> {
    crate::operations::event_record::append_event(connection, event).await
}

/// Load exact typed audit events for one entity.
pub async fn list_events(
    pool: &PostgresPool,
    entity_type: EntityType,
    entity_id: EventEntityId,
    action: Option<Action>,
) -> Result<Vec<StorageRecordedEvent>, PostgresStorageError> {
    crate::operations::event_record::list_events_for_test(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        entity_type,
        entity_id,
        action,
    )
    .await
}

/// Count exact typed audit events for one entity.
pub async fn count_events(
    pool: &PostgresPool,
    entity_type: EntityType,
    entity_id: EventEntityId,
    action: Option<Action>,
) -> Result<i64, PostgresStorageError> {
    crate::operations::event_record::count_events_for_test(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        entity_type,
        entity_id,
        action,
    )
    .await
}

/// Load exact typed audit events for one entity type.
pub async fn list_events_by_type(
    pool: &PostgresPool,
    entity_type: EntityType,
) -> Result<Vec<StorageRecordedEvent>, PostgresStorageError> {
    crate::operations::event_record::list_events_by_type_for_test(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        entity_type,
    )
    .await
}

/// Count deliveries durably associated with one event.
pub async fn count_event_deliveries(
    pool: &PostgresPool,
    event_sequence: EventSequence,
) -> Result<i64, PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

    let mut connection = pool.get().await?;
    event_deliveries
        .filter(event_id.eq(event_sequence.get()))
        .count()
        .get_result::<i64>(&mut connection)
        .await
        .map_err(Into::into)
}

/// Backdate an event for retention integration scenarios.
pub async fn set_event_occurred_at(
    pool: &PostgresPool,
    event_sequence: EventSequence,
    value: NaiveDateTime,
) -> Result<(), PostgresStorageError> {
    use crate::schema::events::dsl::{events, id, occurred_at};

    let mut connection = pool.get().await?;
    ensure_one_row(
        diesel::update(events.filter(id.eq(event_sequence.get())))
            .set(occurred_at.eq(value))
            .execute(&mut connection)
            .await?,
        "event",
    )
}

/// Mark one event as dispatched for retention integration scenarios.
pub async fn mark_event_dispatched(
    pool: &PostgresPool,
    event_sequence: EventSequence,
) -> Result<(), PostgresStorageError> {
    use crate::schema::events::dsl::{dispatched_at, events, id};

    let mut connection = pool.get().await?;
    ensure_one_row(
        diesel::update(events.filter(id.eq(event_sequence.get())))
            .set(dispatched_at.eq(Some(chrono::Utc::now().naive_utc())))
            .execute(&mut connection)
            .await?,
        "event",
    )
}

/// Expire one event fan-out claim without exposing coordination columns.
pub async fn expire_event_fanout_claim(
    pool: &PostgresPool,
    event_sequence: EventSequence,
) -> Result<(), PostgresStorageError> {
    use crate::schema::events::dsl::{events, fanout_locked_until, id};

    let mut connection = pool.get().await?;
    ensure_one_row(
        diesel::update(events.filter(id.eq(event_sequence.get())))
            .set(fanout_locked_until.eq(Some(
                chrono::Utc::now().naive_utc() - chrono::Duration::seconds(1),
            )))
            .execute(&mut connection)
            .await?,
        "event",
    )
}

/// Expire one delivery claim without exposing coordination columns.
pub async fn expire_event_delivery_claim(
    pool: &PostgresPool,
    delivery_id_value: EventDeliveryId,
) -> Result<(), PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id, locked_until};

    let mut connection = pool.get().await?;
    ensure_one_row(
        diesel::update(event_deliveries.filter(id.eq(delivery_id_value.id())))
            .set(locked_until.eq(Some(
                chrono::Utc::now().naive_utc() - chrono::Duration::seconds(1),
            )))
            .execute(&mut connection)
            .await?,
        "event delivery",
    )
}

/// Make one failed delivery immediately eligible for retry.
pub async fn make_event_delivery_due(
    pool: &PostgresPool,
    delivery_id_value: EventDeliveryId,
) -> Result<(), PostgresStorageError> {
    use crate::schema::event_deliveries::dsl::{event_deliveries, id, next_attempt_at};

    let mut connection = pool.get().await?;
    ensure_one_row(
        diesel::update(event_deliveries.filter(id.eq(delivery_id_value.id())))
            .set(next_attempt_at.eq(chrono::Utc::now().naive_utc() - chrono::Duration::seconds(1)))
            .execute(&mut connection)
            .await?,
        "event delivery",
    )
}

fn ensure_one_row(updated: usize, resource: &str) -> Result<(), PostgresStorageError> {
    if updated == 1 {
        Ok(())
    } else {
        Err(PostgresStorageError::not_found(format!(
            "{resource} not found"
        )))
    }
}

/// Fan out one known event without exposing a connection or row type.
pub async fn fanout_event(
    pool: &PostgresPool,
    event_sequence: EventSequence,
) -> Result<usize, PostgresStorageError> {
    crate::operations::event_fanout::fanout_event(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        event_sequence.get(),
    )
    .await
}

/// Load the delivery created for one known event.
pub async fn load_event_delivery_for_event(
    pool: &PostgresPool,
    event_sequence: EventSequence,
) -> Result<StorageEventDelivery, PostgresStorageError> {
    crate::operations::event_delivery::load_event_delivery_for_event_for_test(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        event_sequence,
    )
    .await
}

/// Claim one known delivery through the production adapter operation.
pub async fn claim_event_delivery_by_id(
    pool: &PostgresPool,
    delivery_id: EventDeliveryId,
    settings: EventDeliverySettings,
) -> Result<StorageEventDeliveryWorkItem, PostgresStorageError> {
    crate::operations::event_delivery::claim_event_delivery_by_id(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        delivery_id.id(),
        settings,
    )
    .await
}

/// Set one delivery status for administrator/request compatibility setup.
pub async fn set_event_delivery_status(
    pool: &PostgresPool,
    delivery_id: EventDeliveryId,
    status: EventDeliveryStatus,
) -> Result<(), PostgresStorageError> {
    crate::operations::event_delivery::set_event_delivery_status_for_test(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        delivery_id,
        status,
    )
    .await
}

/// Set one opaque delivery claim token for stale-claim request tests.
pub async fn set_event_delivery_claim_token(
    pool: &PostgresPool,
    delivery_id: EventDeliveryId,
    claim_token: uuid::Uuid,
) -> Result<(), PostgresStorageError> {
    crate::operations::event_delivery::set_event_delivery_claim_token_for_test(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        delivery_id,
        claim_token,
    )
    .await
}

#[derive(QueryableByName)]
struct BackendTermination {
    #[diesel(sql_type = Bool)]
    terminated: bool,
}

#[derive(QueryableByName)]
struct BackendAbsence {
    #[diesel(sql_type = Bool)]
    absent: bool,
}

/// Terminate one PostgreSQL server session from a separately acquired pooled
/// connection.
///
/// Returning `Ok` means PostgreSQL confirmed that the target session existed
/// and received the termination signal. Missing targets are errors so a
/// connection-loss test cannot pass without actually losing its connection.
pub async fn terminate_backend(
    pool: &PostgresPool,
    backend_pid: i32,
) -> Result<(), PostgresStorageError> {
    if backend_pid <= 0 {
        return Err(PostgresStorageError::invalid_input(
            "PostgreSQL backend PID must be positive",
        ));
    }

    let mut administrator = pool.get().await?;
    let result = diesel::sql_query("SELECT pg_terminate_backend($1) AS terminated")
        .bind::<Integer, _>(backend_pid)
        .get_result::<BackendTermination>(&mut administrator)
        .await?;

    if result.terminated {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let absence = diesel::sql_query(
                "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1) AS absent",
            )
            .bind::<Integer, _>(backend_pid)
            .get_result::<BackendAbsence>(&mut administrator)
            .await?;
            if absence.absent {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(PostgresStorageError::database(format!(
                    "PostgreSQL backend PID {backend_pid} remained active after termination"
                )));
            }
            sleep(Duration::from_millis(10)).await;
        }
    } else {
        Err(PostgresStorageError::database(format!(
            "PostgreSQL did not terminate backend PID {backend_pid}"
        )))
    }
}

/// Move one task to the front of deterministic integration-test claim order.
pub async fn prioritize_task(
    pool: &PostgresPool,
    task_id: TaskId,
) -> Result<(), PostgresStorageError> {
    use crate::schema::tasks::dsl::{created_at, id, tasks};

    let mut connection = pool.get().await?;
    let timestamp = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .ok_or_else(|| PostgresStorageError::internal("invalid deterministic task timestamp"))?;
    let updated = diesel::update(tasks.filter(id.eq(task_id.id())))
        .set(created_at.eq(timestamp))
        .execute(&mut connection)
        .await?;
    if updated == 1 {
        Ok(())
    } else {
        Err(PostgresStorageError::not_found(format!(
            "task {} was not available to prioritize",
            task_id.id()
        )))
    }
}

/// Delete one integration-test task and require that it existed.
pub async fn delete_task(pool: &PostgresPool, task_id: TaskId) -> Result<(), PostgresStorageError> {
    use crate::schema::tasks::dsl::{id, tasks};

    let mut connection = pool.get().await?;
    let deleted = diesel::delete(tasks.filter(id.eq(task_id.id())))
        .execute(&mut connection)
        .await?;
    if deleted == 1 {
        Ok(())
    } else {
        Err(PostgresStorageError::not_found(format!(
            "task {} was not available to delete",
            task_id.id()
        )))
    }
}

/// Delete one integration-test restore job and require that it existed.
pub async fn delete_restore_job(
    pool: &PostgresPool,
    restore_job_id: RestoreJobId,
) -> Result<(), PostgresStorageError> {
    use crate::schema::restore_jobs::dsl::{id, restore_jobs};

    let mut connection = pool.get().await?;
    let deleted = diesel::delete(restore_jobs.filter(id.eq(restore_job_id.id())))
        .execute(&mut connection)
        .await?;
    if deleted == 1 {
        Ok(())
    } else {
        Err(PostgresStorageError::not_found(format!(
            "restore job {} was not available to delete",
            restore_job_id.id()
        )))
    }
}

/// Count collections with one exact name without exposing adapter schema types.
pub async fn count_collections_by_name(
    pool: &PostgresPool,
    collection_name: &str,
) -> Result<i64, PostgresStorageError> {
    use crate::schema::collections::dsl::{collections, name};

    let mut connection = pool.get().await?;
    Ok(collections
        .filter(name.eq(collection_name))
        .count()
        .get_result(&mut connection)
        .await?)
}

/// Count collection audit events with one exact entity name.
pub async fn count_collection_events_by_name(
    pool: &PostgresPool,
    collection_name: &str,
) -> Result<i64, PostgresStorageError> {
    use crate::schema::events::dsl::{entity_name, entity_type, events};

    let mut connection = pool.get().await?;
    Ok(events
        .filter(entity_type.eq(EntityType::Collection.as_str()))
        .filter(entity_name.eq(collection_name))
        .count()
        .get_result(&mut connection)
        .await?)
}

/// Put one task into an adapter-owned live-lease fixture state.
pub async fn assign_task_lease(
    pool: &PostgresPool,
    task_id: TaskId,
    status_value: StorageTaskStatus,
    claim_token: uuid::Uuid,
    expires_at: NaiveDateTime,
) -> Result<(), PostgresStorageError> {
    use crate::schema::tasks::dsl::{
        created_at, id, lease_expires_at, lease_token, started_at, status, tasks, updated_at,
    };

    let mut connection = pool.get().await?;
    let updated = diesel::update(tasks.filter(id.eq(task_id.id())))
        .set((
            status.eq(status_value.as_str()),
            started_at.eq(created_at.nullable()),
            lease_token.eq(Some(claim_token)),
            lease_expires_at.eq(Some(expires_at)),
            updated_at.eq(created_at),
        ))
        .execute(&mut connection)
        .await?;
    if updated == 1 {
        Ok(())
    } else {
        Err(PostgresStorageError::not_found(format!(
            "task {} was not available for lease assignment",
            task_id.id()
        )))
    }
}

/// Delete several integration-test tasks while keeping SQL in the adapter.
pub async fn delete_tasks(
    pool: &PostgresPool,
    task_ids: &[TaskId],
) -> Result<(), PostgresStorageError> {
    for task_id in task_ids {
        delete_task(pool, *task_id).await?;
    }
    Ok(())
}

/// Delete several integration-test restore jobs while keeping SQL in the adapter.
pub async fn delete_restore_jobs(
    pool: &PostgresPool,
    restore_job_ids: &[RestoreJobId],
) -> Result<(), PostgresStorageError> {
    for restore_job_id in restore_job_ids {
        delete_restore_job(pool, *restore_job_id).await?;
    }
    Ok(())
}

/// One persisted group membership used to verify external identity reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedGroupMembership {
    group_id: GroupId,
    external_key: Option<String>,
}

impl PersistedGroupMembership {
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    #[must_use]
    pub fn external_key(&self) -> Option<&str> {
        self.external_key.as_deref()
    }
}

/// Adapter-owned persisted evidence for one external identity scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalIdentityPersistence {
    principal_count: i64,
    memberships: Vec<PersistedGroupMembership>,
}

impl ExternalIdentityPersistence {
    #[must_use]
    pub const fn principal_count(&self) -> i64 {
        self.principal_count
    }

    #[must_use]
    pub fn memberships(&self) -> &[PersistedGroupMembership] {
        &self.memberships
    }
}

/// Inspect external identity reconciliation without exposing Diesel rows.
pub async fn external_identity_persistence(
    pool: &PostgresPool,
    identity_scope_name: &str,
    principal_id: PrincipalId,
) -> Result<ExternalIdentityPersistence, PostgresStorageError> {
    use crate::schema::{group_memberships, groups, identity_scopes, principals};

    let mut connection = pool.get().await?;
    let principal_count = principals::table
        .inner_join(identity_scopes::table)
        .filter(identity_scopes::name.eq(identity_scope_name))
        .count()
        .get_result::<i64>(&mut connection)
        .await?;
    let memberships = group_memberships::table
        .inner_join(groups::table)
        .filter(group_memberships::principal_id.eq(principal_id.id()))
        .select((groups::id, groups::external_key))
        .load::<(i32, Option<String>)>(&mut connection)
        .await?
        .into_iter()
        .map(|(group_id, external_key)| {
            Ok(PersistedGroupMembership {
                group_id: GroupId::new(group_id)?,
                external_key,
            })
        })
        .collect::<Result<Vec<_>, PostgresStorageError>>()?;
    Ok(ExternalIdentityPersistence {
        principal_count,
        memberships,
    })
}

/// Cancel externally submitted pending tasks for one principal.
pub async fn cancel_pending_tasks_for_principal(
    pool: &PostgresPool,
    principal_id: hubuum_domain::PrincipalId,
) -> Result<Vec<String>, PostgresStorageError> {
    crate::operations::service_account::cancel_pending_tasks_for_principal(
        &crate::PostgresRuntime::unobserved(pool.clone()),
        principal_id.id(),
    )
    .await
}
