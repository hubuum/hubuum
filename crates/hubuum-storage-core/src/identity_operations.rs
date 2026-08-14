use std::fmt;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use hubuum_events_core::EventContext;
use hubuum_query::QueryOptions;

use crate::{AuthenticationTokenScope, StorageError, StorageRecordMetadata};

/// One identity scope owned by the selected storage backend.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageIdentityScope {
    id: i32,
    name: String,
    provider_kind: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageIdentityScope {
    #[must_use]
    pub fn new(
        id: i32,
        name: impl Into<String>,
        provider_kind: impl Into<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            provider_kind: provider_kind.into(),
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn provider_kind(&self) -> &str {
        &self.provider_kind
    }

    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

/// Validated application request to create or reconcile one identity scope.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageIdentityScopeEnsure {
    name: String,
    provider_kind: String,
}

impl StorageIdentityScopeEnsure {
    #[must_use]
    pub fn new(name: impl Into<String>, provider_kind: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            provider_kind: provider_kind.into(),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn provider_kind(&self) -> &str {
        &self.provider_kind
    }
}

/// One effective principal-to-group membership.
#[derive(Clone, PartialEq, Eq)]
pub struct StoragePrincipalGroup {
    principal_id: i32,
    group_id: i32,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

/// Group projection returned for one principal's effective memberships.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageIdentityGroup {
    id: i32,
    name: String,
    description: String,
    identity_scope_id: i32,
    managed_by: String,
    external_key: Option<String>,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageIdentityGroup {
    #[must_use]
    pub fn builder(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        description: impl Into<String>,
        identity_scope_id: i32,
        managed_by: impl Into<String>,
    ) -> StorageIdentityGroupBuilder {
        StorageIdentityGroupBuilder {
            metadata,
            name: name.into(),
            description: description.into(),
            identity_scope_id,
            managed_by: managed_by.into(),
            external_key: None,
            last_sync_attempted_at: None,
            last_sync_success_at: None,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn identity_scope_id(&self) -> i32 {
        self.identity_scope_id
    }

    #[must_use]
    pub fn managed_by(&self) -> &str {
        &self.managed_by
    }

    #[must_use]
    pub fn external_key(&self) -> Option<&str> {
        self.external_key.as_deref()
    }

    #[must_use]
    pub const fn last_sync_attempted_at(&self) -> Option<NaiveDateTime> {
        self.last_sync_attempted_at
    }

    #[must_use]
    pub const fn last_sync_success_at(&self) -> Option<NaiveDateTime> {
        self.last_sync_success_at
    }

    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

pub struct StorageIdentityGroupBuilder {
    metadata: StorageRecordMetadata,
    name: String,
    description: String,
    identity_scope_id: i32,
    managed_by: String,
    external_key: Option<String>,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
}

impl StorageIdentityGroupBuilder {
    #[must_use]
    pub fn external_key(mut self, value: Option<String>) -> Self {
        self.external_key = value;
        self
    }

    #[must_use]
    pub const fn last_sync_attempted_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.last_sync_attempted_at = value;
        self
    }

    #[must_use]
    pub const fn last_sync_success_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.last_sync_success_at = value;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageIdentityGroup {
        StorageIdentityGroup {
            id: self.metadata.id(),
            name: self.name,
            description: self.description,
            identity_scope_id: self.identity_scope_id,
            managed_by: self.managed_by,
            external_key: self.external_key,
            last_sync_attempted_at: self.last_sync_attempted_at,
            last_sync_success_at: self.last_sync_success_at,
            created_at: self.metadata.created_at(),
            updated_at: self.metadata.updated_at(),
            revision: self.metadata.revision(),
        }
    }
}

impl fmt::Debug for StorageIdentityGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageIdentityGroup")
            .field("id", &"<redacted>")
            .field("name", &"<redacted>")
            .field("managed_by", &self.managed_by)
            .field("has_external_key", &self.external_key.is_some())
            .finish()
    }
}

/// Stable group-membership list request for one principal.
#[derive(Clone, PartialEq)]
pub struct StoragePrincipalGroupListQuery {
    principal_id: i32,
    options: QueryOptions,
}

impl StoragePrincipalGroupListQuery {
    #[must_use]
    pub const fn new(principal_id: i32, options: QueryOptions) -> Self {
        Self {
            principal_id,
            options,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, QueryOptions) {
        (self.principal_id, self.options)
    }
}

impl fmt::Debug for StoragePrincipalGroupListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoragePrincipalGroupListQuery")
            .field("principal_id", &"<redacted>")
            .field("filter_count", &self.options.filters.len())
            .field("sort_count", &self.options.sort.len())
            .field("limit", &self.options.limit)
            .field("has_cursor", &self.options.cursor.is_some())
            .field("include_total", &self.options.include_total)
            .finish()
    }
}

/// Stable request for one page of groups and an optional exact total.
///
/// Record and count options are supplied separately because cursor pagination
/// applies only to the returned page. `None` skips the aggregate entirely.
#[derive(Clone, PartialEq)]
pub struct StorageGroupListQuery {
    records: QueryOptions,
    count: Option<QueryOptions>,
}

impl StorageGroupListQuery {
    #[must_use]
    pub const fn new(records: QueryOptions, count: Option<QueryOptions>) -> Self {
        Self { records, count }
    }

    #[must_use]
    pub fn into_parts(self) -> (QueryOptions, Option<QueryOptions>) {
        (self.records, self.count)
    }
}

impl fmt::Debug for StorageGroupListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageGroupListQuery")
            .field("filter_count", &self.records.filters.len())
            .field("sort_count", &self.records.sort.len())
            .field("limit", &self.records.limit)
            .field("has_cursor", &self.records.cursor.is_some())
            .field("include_total", &self.count.is_some())
            .finish()
    }
}

impl StoragePrincipalGroup {
    #[must_use]
    pub const fn new(
        principal_id: i32,
        group_id: i32,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        revision: i64,
    ) -> Self {
        Self {
            principal_id,
            group_id,
            created_at,
            updated_at,
            revision,
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub const fn group_id(&self) -> i32 {
        self.group_id
    }

    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

/// Retained-token lifecycle subset selected by an identity endpoint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageTokenListState {
    #[default]
    Active,
    Expired,
    Revoked,
    All,
}

/// Deterministic time inputs used to classify retained token metadata.
///
/// The application owns the clock and the configured lifetime of legacy
/// tokens without a persisted expiry. Carrying both values across the storage
/// boundary keeps adapters independent of global configuration and ensures
/// that point, batch, and list operations use identical lifecycle semantics.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StorageTokenObservation {
    observed_at: NaiveDateTime,
    legacy_valid_after: NaiveDateTime,
}

impl StorageTokenObservation {
    pub fn new(
        observed_at: NaiveDateTime,
        legacy_valid_after: NaiveDateTime,
    ) -> Result<Self, StorageTokenObservationError> {
        if legacy_valid_after > observed_at {
            return Err(StorageTokenObservationError);
        }
        Ok(Self {
            observed_at,
            legacy_valid_after,
        })
    }

    #[must_use]
    pub const fn into_parts(self) -> (NaiveDateTime, NaiveDateTime) {
        (self.observed_at, self.legacy_valid_after)
    }
}

impl fmt::Debug for StorageTokenObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenObservation")
            .field("observed_at", &"<redacted>")
            .field("legacy_valid_after", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageTokenObservationError;

impl fmt::Display for StorageTokenObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("legacy token validity cutoff cannot be after the observation time")
    }
}

impl std::error::Error for StorageTokenObservationError {}

/// Backend-neutral token-list request.
#[derive(Clone, PartialEq)]
pub struct StorageTokenListQuery {
    principal_id: i32,
    options: QueryOptions,
    state: StorageTokenListState,
    observation: StorageTokenObservation,
}

impl StorageTokenListQuery {
    #[must_use]
    pub const fn new(
        principal_id: i32,
        options: QueryOptions,
        state: StorageTokenListState,
        observation: StorageTokenObservation,
    ) -> Self {
        Self {
            principal_id,
            options,
            state,
            observation,
        }
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }

    #[must_use]
    pub const fn state(&self) -> StorageTokenListState {
        self.state
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        QueryOptions,
        StorageTokenListState,
        StorageTokenObservation,
    ) {
        (
            self.principal_id,
            self.options,
            self.state,
            self.observation,
        )
    }
}

impl fmt::Debug for StorageTokenListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenListQuery")
            .field("principal_id", &"<redacted>")
            .field("filter_count", &self.options.filters.len())
            .field("sort_count", &self.options.sort.len())
            .field("limit", &self.options.limit)
            .field("has_cursor", &self.options.cursor.is_some())
            .field("include_total", &self.options.include_total)
            .field("state", &self.state)
            .field("observation", &self.observation)
            .finish()
    }
}

/// Hash-free metadata for one retained bearer token.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageTokenMetadata {
    id: i32,
    principal_id: i32,
    name: Option<String>,
    description: Option<String>,
    issued: NaiveDateTime,
    expires_at: Option<NaiveDateTime>,
    last_used_at: Option<NaiveDateTime>,
    revoked_at: Option<NaiveDateTime>,
    active: bool,
    expired: bool,
    scope: Option<AuthenticationTokenScope>,
    revision: i64,
}

impl fmt::Debug for StorageTokenMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageTokenMetadata")
            .field("id", &"<redacted>")
            .field("principal_id", &"<redacted>")
            .field("active", &self.active)
            .field("expired", &self.expired)
            .field("has_scope", &self.scope.is_some())
            .finish()
    }
}

impl StorageTokenMetadata {
    #[must_use]
    pub const fn builder(
        id: i32,
        principal_id: i32,
        issued: NaiveDateTime,
        revision: i64,
    ) -> StorageTokenMetadataBuilder {
        StorageTokenMetadataBuilder {
            id,
            principal_id,
            name: None,
            description: None,
            issued,
            expires_at: None,
            last_used_at: None,
            revoked_at: None,
            active: false,
            expired: false,
            scope: None,
            revision,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub const fn principal_id(&self) -> i32 {
        self.principal_id
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[must_use]
    pub const fn issued(&self) -> NaiveDateTime {
        self.issued
    }

    #[must_use]
    pub const fn expires_at(&self) -> Option<NaiveDateTime> {
        self.expires_at
    }

    #[must_use]
    pub const fn last_used_at(&self) -> Option<NaiveDateTime> {
        self.last_used_at
    }

    #[must_use]
    pub const fn revoked_at(&self) -> Option<NaiveDateTime> {
        self.revoked_at
    }

    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }

    #[must_use]
    pub const fn is_expired(&self) -> bool {
        self.expired
    }

    #[must_use]
    pub const fn scope(&self) -> Option<&AuthenticationTokenScope> {
        self.scope.as_ref()
    }

    #[must_use]
    pub fn into_scope(self) -> Option<AuthenticationTokenScope> {
        self.scope
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }
}

/// Builder for retained token metadata.
pub struct StorageTokenMetadataBuilder {
    id: i32,
    principal_id: i32,
    name: Option<String>,
    description: Option<String>,
    issued: NaiveDateTime,
    expires_at: Option<NaiveDateTime>,
    last_used_at: Option<NaiveDateTime>,
    revoked_at: Option<NaiveDateTime>,
    active: bool,
    expired: bool,
    scope: Option<AuthenticationTokenScope>,
    revision: i64,
}

impl StorageTokenMetadataBuilder {
    #[must_use]
    pub fn name(mut self, value: Option<String>) -> Self {
        self.name = value;
        self
    }

    #[must_use]
    pub fn description(mut self, value: Option<String>) -> Self {
        self.description = value;
        self
    }

    #[must_use]
    pub const fn expires_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.expires_at = value;
        self
    }

    #[must_use]
    pub const fn last_used_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.last_used_at = value;
        self
    }

    #[must_use]
    pub const fn revoked_at(mut self, value: Option<NaiveDateTime>) -> Self {
        self.revoked_at = value;
        self
    }

    #[must_use]
    pub const fn active(mut self, value: bool) -> Self {
        self.active = value;
        self
    }

    #[must_use]
    pub const fn expired(mut self, value: bool) -> Self {
        self.expired = value;
        self
    }

    #[must_use]
    pub fn scope(mut self, value: Option<AuthenticationTokenScope>) -> Self {
        self.scope = value;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageTokenMetadata {
        StorageTokenMetadata {
            id: self.id,
            principal_id: self.principal_id,
            name: self.name,
            description: self.description,
            issued: self.issued,
            expires_at: self.expires_at,
            last_used_at: self.last_used_at,
            revoked_at: self.revoked_at,
            active: self.active,
            expired: self.expired,
            scope: self.scope,
            revision: self.revision,
        }
    }
}

/// One backend-selected identity page and optional exact total.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageIdentityPage<T> {
    rows: Vec<T>,
    total: Option<i64>,
}

impl<T> StorageIdentityPage<T> {
    #[must_use]
    pub const fn new(rows: Vec<T>, total: Option<i64>) -> Self {
        Self { rows, total }
    }

    #[must_use]
    pub fn into_parts(self) -> (Vec<T>, Option<i64>) {
        (self.rows, self.total)
    }
}

/// Service-account row without its separately stored principal name.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageServiceAccount {
    id: i32,
    description: String,
    owner_group_id: i32,
    created_by: Option<i32>,
    disabled_at: Option<NaiveDateTime>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
}

impl StorageServiceAccount {
    #[must_use]
    pub fn new(
        id: i32,
        description: impl Into<String>,
        owner_group_id: i32,
        created_by: Option<i32>,
        disabled_at: Option<NaiveDateTime>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
    ) -> Self {
        Self {
            id,
            description: description.into(),
            owner_group_id,
            created_by,
            disabled_at,
            created_at,
            updated_at,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn owner_group_id(&self) -> i32 {
        self.owner_group_id
    }

    #[must_use]
    pub const fn created_by(&self) -> Option<i32> {
        self.created_by
    }

    #[must_use]
    pub const fn disabled_at(&self) -> Option<NaiveDateTime> {
        self.disabled_at
    }

    #[must_use]
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn is_disabled(&self) -> bool {
        self.disabled_at.is_some()
    }
}

/// Service-account list row with its principal and identity-scope projection.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageServiceAccountListItem {
    service_account: StorageServiceAccount,
    identity_scope: String,
    name: String,
    revision: i64,
}

/// Strong service-account point with its revision-owned principal fields.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageServiceAccountPoint {
    service_account: StorageServiceAccount,
    identity_scope_id: i32,
    name: String,
    revision: i64,
}

/// Result of atomically disabling one service account.
///
/// Backends own credential revocation and queued-work cancellation because
/// those writes must commit with the account state change. The application
/// receives only the non-sensitive task kinds needed for backend-neutral
/// terminal-task metrics.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageServiceAccountDisableOutcome {
    service_account: StorageServiceAccount,
    cancelled_task_kinds: Vec<String>,
}

impl StorageServiceAccountDisableOutcome {
    #[must_use]
    pub fn new(service_account: StorageServiceAccount, cancelled_task_kinds: Vec<String>) -> Self {
        Self {
            service_account,
            cancelled_task_kinds,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageServiceAccount, Vec<String>) {
        (self.service_account, self.cancelled_task_kinds)
    }
}

impl StorageServiceAccountPoint {
    #[must_use]
    pub fn new(
        service_account: StorageServiceAccount,
        identity_scope_id: i32,
        name: impl Into<String>,
        revision: i64,
    ) -> Self {
        Self {
            service_account,
            identity_scope_id,
            name: name.into(),
            revision,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageServiceAccount, i32, String, i64) {
        (
            self.service_account,
            self.identity_scope_id,
            self.name,
            self.revision,
        )
    }
}

impl StorageServiceAccountListItem {
    #[must_use]
    pub fn new(
        service_account: StorageServiceAccount,
        identity_scope: impl Into<String>,
        name: impl Into<String>,
        revision: i64,
    ) -> Self {
        Self {
            service_account,
            identity_scope: identity_scope.into(),
            name: name.into(),
            revision,
        }
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageServiceAccount, String, String, i64) {
        (
            self.service_account,
            self.identity_scope,
            self.name,
            self.revision,
        )
    }
}

/// Manageable-service-account query with authorization pushdown inputs.
#[derive(Clone, PartialEq)]
pub struct StorageServiceAccountListQuery {
    requestor_id: i32,
    administrator: bool,
    options: QueryOptions,
}

impl StorageServiceAccountListQuery {
    #[must_use]
    pub const fn new(requestor_id: i32, administrator: bool, options: QueryOptions) -> Self {
        Self {
            requestor_id,
            administrator,
            options,
        }
    }

    #[must_use]
    pub const fn requestor_id(&self) -> i32 {
        self.requestor_id
    }

    #[must_use]
    pub const fn is_administrator(&self) -> bool {
        self.administrator
    }

    #[must_use]
    pub const fn options(&self) -> &QueryOptions {
        &self.options
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, bool, QueryOptions) {
        (self.requestor_id, self.administrator, self.options)
    }
}

impl fmt::Debug for StorageServiceAccountListQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageServiceAccountListQuery")
            .field("requestor_id", &"<redacted>")
            .field("administrator", &self.administrator)
            .field("filter_count", &self.options.filters.len())
            .field("sort_count", &self.options.sort.len())
            .field("limit", &self.options.limit)
            .field("has_cursor", &self.options.cursor.is_some())
            .field("include_total", &self.options.include_total)
            .finish()
    }
}

/// Service-account creation owned by the identity storage contract.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageServiceAccountCreate {
    name: String,
    description: String,
    owner_group_id: i32,
    created_by: Option<i32>,
    event_context: EventContext,
}

impl StorageServiceAccountCreate {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        owner_group_id: i32,
        created_by: Option<i32>,
        event_context: EventContext,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            owner_group_id,
            created_by,
            event_context,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn owner_group_id(&self) -> i32 {
        self.owner_group_id
    }

    #[must_use]
    pub const fn created_by(&self) -> Option<i32> {
        self.created_by
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }

    #[must_use]
    pub fn into_parts(self) -> (String, String, i32, Option<i32>, EventContext) {
        (
            self.name,
            self.description,
            self.owner_group_id,
            self.created_by,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageServiceAccountCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageServiceAccountCreate")
            .field("name", &"<redacted>")
            .field("description", &"<redacted>")
            .field("owner_group_id", &"<redacted>")
            .field("created_by", &self.created_by.map(|_| "<redacted>"))
            .finish()
    }
}

/// Service-account patch owned by the identity storage contract.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageServiceAccountUpdate {
    id: i32,
    description: Option<String>,
    owner_group_id: Option<i32>,
    event_context: EventContext,
}

impl StorageServiceAccountUpdate {
    #[must_use]
    pub fn new(
        id: i32,
        description: Option<String>,
        owner_group_id: Option<i32>,
        event_context: EventContext,
    ) -> Self {
        Self {
            id,
            description,
            owner_group_id,
            event_context,
        }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[must_use]
    pub const fn owner_group_id(&self) -> Option<i32> {
        self.owner_group_id
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, Option<String>, Option<i32>, EventContext) {
        (
            self.id,
            self.description,
            self.owner_group_id,
            self.event_context,
        )
    }
}

impl fmt::Debug for StorageServiceAccountUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageServiceAccountUpdate")
            .field("id", &"<redacted>")
            .field("has_description", &self.description.is_some())
            .field("has_owner_group_id", &self.owner_group_id.is_some())
            .finish()
    }
}

/// Point mutation for disabling or deleting one service account.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageServiceAccountMutation {
    id: i32,
    event_context: EventContext,
}

impl StorageServiceAccountMutation {
    #[must_use]
    pub const fn new(id: i32, event_context: EventContext) -> Self {
        Self { id, event_context }
    }

    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub const fn event_context(&self) -> &EventContext {
        &self.event_context
    }

    #[must_use]
    pub fn into_parts(self) -> (i32, EventContext) {
        (self.id, self.event_context)
    }
}

impl fmt::Debug for StorageServiceAccountMutation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageServiceAccountMutation")
            .field("id", &"<redacted>")
            .finish()
    }
}

/// Refresh state for one provider-managed human principal.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageExternalPrincipalState {
    identity_scope: String,
    username: String,
    external_subject: String,
    last_sync_attempted_at: Option<NaiveDateTime>,
    last_sync_success_at: Option<NaiveDateTime>,
}

impl StorageExternalPrincipalState {
    #[must_use]
    pub fn new(
        identity_scope: impl Into<String>,
        username: impl Into<String>,
        external_subject: impl Into<String>,
        last_sync_attempted_at: Option<NaiveDateTime>,
        last_sync_success_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            identity_scope: identity_scope.into(),
            username: username.into(),
            external_subject: external_subject.into(),
            last_sync_attempted_at,
            last_sync_success_at,
        }
    }

    #[must_use]
    pub fn identity_scope(&self) -> &str {
        &self.identity_scope
    }

    #[must_use]
    pub fn username(&self) -> &str {
        &self.username
    }

    #[must_use]
    pub fn external_subject(&self) -> &str {
        &self.external_subject
    }

    #[must_use]
    pub const fn last_sync_attempted_at(&self) -> Option<NaiveDateTime> {
        self.last_sync_attempted_at
    }

    #[must_use]
    pub const fn last_sync_success_at(&self) -> Option<NaiveDateTime> {
        self.last_sync_success_at
    }
}

impl fmt::Debug for StorageExternalPrincipalState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExternalPrincipalState")
            .field("identity_scope", &"<redacted>")
            .field("username", &"<redacted>")
            .field("external_subject", &"<redacted>")
            .field("has_attempt", &self.last_sync_attempted_at.is_some())
            .field("has_success", &self.last_sync_success_at.is_some())
            .finish()
    }
}

/// One external directory group attached to a synchronized human.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageExternalGroup {
    key: String,
    name: String,
    description: Option<String>,
}

impl StorageExternalGroup {
    #[must_use]
    pub fn new(
        key: impl Into<String>,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Self {
        Self {
            key: key.into(),
            name: name.into(),
            description,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    #[must_use]
    pub fn into_parts(self) -> (String, String, Option<String>) {
        (self.key, self.name, self.description)
    }
}

/// Complete external-directory synchronization input.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageExternalUserSync {
    identity_scope: String,
    provider_kind: String,
    subject: String,
    name: String,
    proper_name: Option<String>,
    email: Option<String>,
    groups: Vec<StorageExternalGroup>,
}

impl StorageExternalUserSync {
    #[must_use]
    pub fn builder(
        identity_scope: impl Into<String>,
        provider_kind: impl Into<String>,
        subject: impl Into<String>,
        name: impl Into<String>,
    ) -> StorageExternalUserSyncBuilder {
        StorageExternalUserSyncBuilder {
            identity_scope: identity_scope.into(),
            provider_kind: provider_kind.into(),
            subject: subject.into(),
            name: name.into(),
            proper_name: None,
            email: None,
            groups: Vec::new(),
        }
    }

    #[must_use]
    pub fn identity_scope(&self) -> &str {
        &self.identity_scope
    }

    #[must_use]
    pub fn provider_kind(&self) -> &str {
        &self.provider_kind
    }

    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn proper_name(&self) -> Option<&str> {
        self.proper_name.as_deref()
    }

    #[must_use]
    pub fn email(&self) -> Option<&str> {
        self.email.as_deref()
    }

    #[must_use]
    pub fn groups(&self) -> &[StorageExternalGroup] {
        &self.groups
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        String,
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Vec<StorageExternalGroup>,
    ) {
        (
            self.identity_scope,
            self.provider_kind,
            self.subject,
            self.name,
            self.proper_name,
            self.email,
            self.groups,
        )
    }
}

impl fmt::Debug for StorageExternalUserSync {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageExternalUserSync")
            .field("identity_scope", &"<redacted>")
            .field("provider_kind", &self.provider_kind)
            .field("subject", &"<redacted>")
            .field("name", &"<redacted>")
            .field("has_proper_name", &self.proper_name.is_some())
            .field("has_email", &self.email.is_some())
            .field("group_count", &self.groups.len())
            .finish()
    }
}

/// Builder for external-directory synchronization input.
pub struct StorageExternalUserSyncBuilder {
    identity_scope: String,
    provider_kind: String,
    subject: String,
    name: String,
    proper_name: Option<String>,
    email: Option<String>,
    groups: Vec<StorageExternalGroup>,
}

impl StorageExternalUserSyncBuilder {
    #[must_use]
    pub fn proper_name(mut self, value: Option<String>) -> Self {
        self.proper_name = value;
        self
    }

    #[must_use]
    pub fn email(mut self, value: Option<String>) -> Self {
        self.email = value;
        self
    }

    #[must_use]
    pub fn groups(mut self, value: Vec<StorageExternalGroup>) -> Self {
        self.groups = value;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageExternalUserSync {
        StorageExternalUserSync {
            identity_scope: self.identity_scope,
            provider_kind: self.provider_kind,
            subject: self.subject,
            name: self.name,
            proper_name: self.proper_name,
            email: self.email,
            groups: self.groups,
        }
    }
}

/// Password-free human row returned after external synchronization.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageSyncedHuman {
    id: i32,
    proper_name: Option<String>,
    email: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    anonymized_at: Option<NaiveDateTime>,
}

/// Initial local administrator bootstrap request.
///
/// The password value is already hashed by the application before it crosses
/// the storage boundary. Debug output deliberately reveals neither the group
/// name nor the credential hash.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageDefaultAdminBootstrap {
    admin_group_name: String,
    password_hash: String,
}

impl StorageDefaultAdminBootstrap {
    #[must_use]
    pub fn new(admin_group_name: impl Into<String>, password_hash: impl Into<String>) -> Self {
        Self {
            admin_group_name: admin_group_name.into(),
            password_hash: password_hash.into(),
        }
    }

    #[must_use]
    pub fn admin_group_name(&self) -> &str {
        &self.admin_group_name
    }

    #[must_use]
    pub fn password_hash(&self) -> &str {
        &self.password_hash
    }
}

impl fmt::Debug for StorageDefaultAdminBootstrap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageDefaultAdminBootstrap")
            .field("admin_group_name", &"<redacted>")
            .field("has_password_hash", &!self.password_hash.is_empty())
            .finish()
    }
}

/// Administrator-requested local password replacement.
///
/// The application hashes the new credential before dispatch. Implementations
/// atomically replace it and revoke active bearer tokens.
#[derive(Clone, PartialEq, Eq)]
pub struct StorageLocalPasswordReset {
    principal_name: String,
    password_hash: String,
}

impl StorageLocalPasswordReset {
    #[must_use]
    pub fn new(principal_name: impl Into<String>, password_hash: impl Into<String>) -> Self {
        Self {
            principal_name: principal_name.into(),
            password_hash: password_hash.into(),
        }
    }

    #[must_use]
    pub fn principal_name(&self) -> &str {
        &self.principal_name
    }

    #[must_use]
    pub fn password_hash(&self) -> &str {
        &self.password_hash
    }
}

impl fmt::Debug for StorageLocalPasswordReset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageLocalPasswordReset")
            .field("principal_name", &"<redacted>")
            .field("has_password_hash", &!self.password_hash.is_empty())
            .finish()
    }
}

impl StorageSyncedHuman {
    #[must_use]
    pub const fn new(
        id: i32,
        proper_name: Option<String>,
        email: Option<String>,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
        anonymized_at: Option<NaiveDateTime>,
    ) -> Self {
        Self {
            id,
            proper_name,
            email,
            created_at,
            updated_at,
            anonymized_at,
        }
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        Option<String>,
        Option<String>,
        NaiveDateTime,
        NaiveDateTime,
        Option<NaiveDateTime>,
    ) {
        (
            self.id,
            self.proper_name,
            self.email,
            self.created_at,
            self.updated_at,
            self.anonymized_at,
        )
    }
}

/// Complete identity and IAM operations every selectable backend must provide.
#[async_trait]
pub trait IdentityStorage: Send + Sync {
    /// Return whether the backend is empty enough to require its initial local
    /// administrator. This is an optimization; the atomic bootstrap operation
    /// must repeat the check under its backend-native coordination primitive.
    async fn default_admin_bootstrap_required(&self) -> Result<bool, StorageError>;

    /// Atomically create the initial local administrator when still required.
    /// Concurrent callers must produce at most one administrator and all later
    /// callers return `false`.
    async fn bootstrap_default_admin(
        &self,
        request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError>;

    /// Replace one local human credential and atomically revoke every active
    /// bearer token owned by that principal. Returns the revoked token count.
    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<usize, StorageError>;

    /// Create the named scope if absent, or reconcile its provider kind when it
    /// already exists, and return the authoritative stored row.
    async fn ensure_identity_scope(
        &self,
        request: StorageIdentityScopeEnsure,
    ) -> Result<StorageIdentityScope, StorageError>;

    /// Resolve one scope ID to its name, returning `NotFound` when it does not
    /// exist.
    async fn identity_scope_name(&self, scope_id: i32) -> Result<String, StorageError>;

    /// Resolve every distinct requested scope ID.
    ///
    /// An empty request returns an empty result. A non-empty request must fail
    /// rather than return a partial mapping when any ID cannot be resolved.
    async fn identity_scope_names(
        &self,
        scope_ids: Vec<i32>,
    ) -> Result<Vec<(i32, String)>, StorageError>;

    /// Load one effective principal-to-group membership with its authoritative
    /// revision, returning `NotFound` when no membership source remains.
    async fn load_principal_group(
        &self,
        principal_id: i32,
        group_id: i32,
    ) -> Result<StoragePrincipalGroup, StorageError>;

    /// List every effective group membership for one principal with stable
    /// filtering, cursor pagination, and optional exact total.
    async fn list_principal_groups(
        &self,
        query: StoragePrincipalGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError>;

    /// List groups with stable filtering, cursor pagination, and an optional
    /// exact total in one operation-shaped backend capability.
    async fn list_groups(
        &self,
        query: StorageGroupListQuery,
    ) -> Result<StorageIdentityPage<StorageIdentityGroup>, StorageError>;

    /// Return hash-free retained token metadata using the requested lifecycle
    /// state, filters, stable cursor page, and optional exact total.
    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StorageIdentityPage<StorageTokenMetadata>, StorageError>;

    /// Return whether the principal is both human and an effective member of
    /// the service account owner group.
    async fn is_human_owner_group_member(
        &self,
        principal_id: i32,
        owner_group_id: i32,
    ) -> Result<bool, StorageError>;

    /// Return `true` only for a disabled service-account principal.
    ///
    /// Human principals and IDs without a service-account row return `false`.
    async fn principal_is_disabled(&self, principal_id: i32) -> Result<bool, StorageError>;

    /// Load the service-account row for one principal ID.
    async fn load_service_account(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccount, StorageError>;

    /// Load one service account together with the principal-owned name, scope,
    /// and revision needed for a strong point response.
    async fn load_service_account_point(
        &self,
        service_account_id: i32,
    ) -> Result<StorageServiceAccountPoint, StorageError>;

    /// List service accounts manageable by the requestor, applying owner-group
    /// authorization, administrator override, filtering, stable paging, and an
    /// optional exact total inside the backend.
    async fn list_manageable_service_accounts(
        &self,
        query: StorageServiceAccountListQuery,
    ) -> Result<StorageIdentityPage<StorageServiceAccountListItem>, StorageError>;

    /// Atomically create one local service account, its principal projection,
    /// and the required lifecycle event.
    async fn create_service_account(
        &self,
        request: StorageServiceAccountCreate,
    ) -> Result<StorageServiceAccount, StorageError>;

    /// Atomically apply a service-account patch and its required lifecycle
    /// event, preserving no-op revision behavior.
    async fn update_service_account(
        &self,
        request: StorageServiceAccountUpdate,
    ) -> Result<StorageServiceAccount, StorageError>;

    /// Atomically disable a service account, revoke its active credentials,
    /// cancel its pending work, and emit the required lifecycle event.
    async fn disable_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<StorageServiceAccountDisableOutcome, StorageError>;

    /// Atomically delete an eligible service account and emit the required
    /// lifecycle event, enforcing backend-owned deletion constraints.
    async fn delete_service_account(
        &self,
        request: StorageServiceAccountMutation,
    ) -> Result<(), StorageError>;

    /// Return refresh state only for a provider-managed external human.
    ///
    /// Missing, local, and unmanaged principals return `None`; inconsistent
    /// external records fail closed.
    async fn external_principal_state(
        &self,
        principal_id: i32,
    ) -> Result<Option<StorageExternalPrincipalState>, StorageError>;

    /// Record the current external refresh attempt time for one principal.
    async fn mark_external_sync_attempted(&self, principal_id: i32) -> Result<(), StorageError>;

    /// Atomically reconcile one external human, its profile, effective external
    /// group memberships, synchronization timestamps, and lifecycle events.
    async fn sync_external_user(
        &self,
        request: StorageExternalUserSync,
    ) -> Result<StorageSyncedHuman, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_group_builder_keeps_record_metadata_and_optional_sync_state() {
        let created_at = NaiveDateTime::default();
        let updated_at = created_at + chrono::Duration::seconds(1);
        let group = StorageIdentityGroup::builder(
            StorageRecordMetadata::new(7, created_at, updated_at, 3),
            "operators",
            "Operations team",
            2,
            "local",
        )
        .external_key(Some("directory-secret".to_string()))
        .last_sync_success_at(Some(updated_at))
        .build();

        assert_eq!(group.id(), 7);
        assert_eq!(group.name(), "operators");
        assert_eq!(group.created_at(), created_at);
        assert_eq!(group.updated_at(), updated_at);
        assert_eq!(group.revision(), 3);
        assert_eq!(group.last_sync_success_at(), Some(updated_at));
        let debug = format!("{group:?}");
        assert!(!debug.contains("operators"));
        assert!(!debug.contains("directory-secret"));
    }

    #[test]
    fn query_debug_output_redacts_identity_and_cursor_values() {
        let options = QueryOptions {
            filters: Vec::new(),
            sort: Vec::new(),
            limit: Some(20),
            cursor: Some("sensitive-cursor".to_string()),
            include_total: true,
        };
        let debug = format!(
            "{:?}",
            StorageServiceAccountListQuery::new(42, false, options)
        );

        assert!(!debug.contains("42"));
        assert!(!debug.contains("sensitive-cursor"));
        assert!(debug.contains("has_cursor: true"));
    }

    #[test]
    fn external_sync_debug_output_is_redacted_and_bounded() {
        let request = StorageExternalUserSync::builder(
            "secret-scope",
            "ldap",
            "secret-subject",
            "secret-name",
        )
        .email(Some("secret@example.com".to_string()))
        .groups(vec![StorageExternalGroup::new(
            "secret-key",
            "secret-group",
            None,
        )])
        .build();
        let debug = format!("{request:?}");

        assert!(!debug.contains("secret-scope"));
        assert!(!debug.contains("secret-subject"));
        assert!(!debug.contains("secret-name"));
        assert!(!debug.contains("secret@example.com"));
        assert!(debug.contains("group_count: 1"));
    }

    #[test]
    fn default_admin_bootstrap_debug_redacts_group_and_hash() {
        let request =
            StorageDefaultAdminBootstrap::new("sensitive-admin-group", "sensitive-password-hash");
        let debug = format!("{request:?}");

        assert!(!debug.contains("sensitive-admin-group"));
        assert!(!debug.contains("sensitive-password-hash"));
        assert!(debug.contains("has_password_hash: true"));
    }

    #[test]
    fn local_password_reset_debug_redacts_name_and_hash() {
        let request =
            StorageLocalPasswordReset::new("sensitive-principal-name", "sensitive-password-hash");
        let debug = format!("{request:?}");

        assert!(!debug.contains("sensitive-principal-name"));
        assert!(!debug.contains("sensitive-password-hash"));
        assert!(debug.contains("has_password_hash: true"));
    }

    #[test]
    fn token_observation_rejects_a_future_legacy_cutoff() {
        let observed_at = NaiveDateTime::default();

        let result = StorageTokenObservation::new(
            observed_at,
            observed_at + chrono::Duration::microseconds(1),
        );

        assert_eq!(result, Err(StorageTokenObservationError));
    }

    #[test]
    fn token_observation_debug_redacts_timestamps() {
        let observed_at = NaiveDateTime::default();
        let observation =
            StorageTokenObservation::new(observed_at, observed_at - chrono::Duration::hours(24))
                .unwrap();

        let debug = format!("{observation:?}");

        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("1970"));
    }
}
