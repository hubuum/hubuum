//! Inactive public-path, collection-only query service. A caller must already
//! own a registered Legacy runtime; this module never advertises online expiry.
use chrono::{DateTime, Utc};
use hubuum_artifacts::ArtifactError;
use hubuum_budget::ByteLimit;
use hubuum_domain::{CollectionId, PrincipalIdentity, ResourceId, ResourceIdentity};
use hubuum_storage_core::{
    AuthorizationDataStorage, ExecutionStorage, HistoryArchiveAuthorization, HistoryArchiveCapture,
    HistoryArchiveCaptureRequest, HistoryArchiveCatalogGeneration, HistoryArchiveCoverageState,
    HistoryArchivePoint, HistoryArchiveRow, HistoryArchiveSelection, HistoryArchiveStorage,
    LogicalIdentityStorage, StorageAuthorizationResource, StorageAuthorizationResourceKey,
    StorageAuthorizationResourcesQuery, StorageBackupHistorySection, StorageErrorKind,
    StorageExecutionPermit, StorageIdentityResourceKind, StorageIdentitySubject,
    StorageIdentityTarget,
};
use serde::Serialize;
use uuid::Uuid;

use super::access::authorize_history_snapshot;
use super::segments::{RetainedHistorySegment, SegmentScope};
use crate::errors::ApiError;
use crate::models::{HistoryAuthorizationSnapshot, Permissions, TokenScope};
use crate::permissions::AppContext;
use crate::storage::{StorageHandle, storage_handle};
use crate::traits::AuthzSubject;

const ATTEMPT_BYTES: usize = 16 * 1024 * 1024;

pub struct HistoryArchiveRequest {
    collection: CollectionId,
    family: StorageBackupHistorySection,
    point: HistoryArchivePoint,
    available_until: DateTime<Utc>,
}
impl HistoryArchiveRequest {
    pub const fn collection(
        collection: CollectionId,
        point: HistoryArchivePoint,
        available_until: DateTime<Utc>,
    ) -> Self {
        Self {
            collection,
            family: StorageBackupHistorySection::CollectionHistory,
            point,
            available_until,
        }
    }
    /// Other families remain explicitly unsupported, never complete-empty.
    pub const fn with_family(mut self, family: StorageBackupHistorySection) -> Self {
        self.family = family;
        self
    }
}

pub enum HistoryArchiveAdmission {
    Captured(HistoryArchiveQuery),
    BudgetLimited,
}

/// Opaque request handle: neither the captured payload nor coverage can be read
/// without a fresh configured authorization and original-fence check.
pub struct HistoryArchiveQuery {
    id: Uuid,
    principal: PrincipalIdentity,
    collection: CollectionId,
    resource: ResourceIdentity,
    permit: StorageExecutionPermit,
    storage: StorageHandle,
}

async fn principal<S: AuthzSubject + ?Sized>(
    context: &AppContext,
    subject: &S,
) -> Result<PrincipalIdentity, ApiError> {
    let identity = storage_handle(context)
        .resolve_logical_identity(StorageIdentityTarget::Principal(
            subject.validated_principal_id()?,
        ))
        .await?;
    match identity.subject() {
        Some(StorageIdentitySubject::Principal { identity }) => identity
            .known_identity()
            .ok_or_else(|| ApiError::ServiceUnavailable("Principal identity unavailable".into())),
        _ => Err(ApiError::ServiceUnavailable(
            "Principal identity unavailable".into(),
        )),
    }
}
async fn authorize<S: AuthzSubject + ?Sized>(
    context: &AppContext,
    subject: &S,
    scopes: Option<&TokenScope>,
    collection: CollectionId,
    resource: ResourceIdentity,
    captured: Option<&HistoryArchiveAuthorization>,
) -> Result<(), ApiError> {
    let storage = storage_handle(context);
    let current_name = storage
        .load_authorization_resources(StorageAuthorizationResourcesQuery::new([
            StorageAuthorizationResourceKey::Collection(collection),
        ]))
        .await?
        .into_iter()
        .find_map(|resource| match resource {
            StorageAuthorizationResource::Collection { id, name } if id == collection => Some(name),
            _ => None,
        })
        .ok_or_else(|| ApiError::NotFound("History request unavailable".into()))?;
    let identity = storage
        .resolve_logical_identity(StorageIdentityTarget::Resource {
            kind: StorageIdentityResourceKind::Collection,
            id: ResourceId::from(collection),
        })
        .await?;
    if identity
        .subject()
        .and_then(StorageIdentitySubject::record_identity)
        .is_none_or(|id| id.resource() != resource)
    {
        return Err(ApiError::NotFound("History request unavailable".into()));
    }
    authorize_history_snapshot(
        context,
        subject,
        scopes,
        Permissions::ReadCollection,
        HistoryAuthorizationSnapshot::collection(collection.id(), current_name),
    )
    .await?;
    if let Some(captured) = captured {
        if captured.collection() != collection {
            return Err(ApiError::NotFound("History request unavailable".into()));
        }
        authorize_history_snapshot(
            context,
            subject,
            scopes,
            Permissions::ReadCollection,
            HistoryAuthorizationSnapshot::collection(collection.id(), captured.name().into()),
        )
        .await?;
    }
    Ok(())
}

impl HistoryArchiveQuery {
    pub const fn id(&self) -> Uuid {
        self.id
    }
    pub async fn capture<S: AuthzSubject + ?Sized>(
        context: &AppContext,
        subject: &S,
        scopes: Option<&TokenScope>,
        request: HistoryArchiveRequest,
    ) -> Result<HistoryArchiveAdmission, ApiError> {
        let storage = storage_handle(context);
        let permit = storage.admit_application_work().await?;
        if permit.owner().is_none() {
            return Err(ApiError::ServiceUnavailable(
                "Archive reads require a registered Legacy runtime".into(),
            ));
        }
        let scope = storage
            .current_execution_scope()
            .with_application_access(permit.clone());
        storage
            .run_in_scope(scope, async {
                let evidence = storage
                    .resolve_logical_identity(StorageIdentityTarget::Resource {
                        kind: StorageIdentityResourceKind::Collection,
                        id: ResourceId::from(request.collection),
                    })
                    .await?;
                let resource = evidence
                    .subject()
                    .and_then(StorageIdentitySubject::record_identity)
                    .ok_or_else(|| {
                        ApiError::ServiceUnavailable("Collection identity unavailable".into())
                    })?
                    .resource();
                authorize(context, subject, scopes, request.collection, resource, None).await?;
                let principal = principal(context, subject).await?;
                let catalog = storage.history_archive_catalog(resource, &permit).await?;
                let selection =
                    HistoryArchiveSelection::new(request.family, resource, request.point);
                let capture_request = HistoryArchiveCaptureRequest::new(
                    principal,
                    selection,
                    catalog.generation(),
                    request.available_until,
                );
                let capture = match storage
                    .capture_history_archive(capture_request.clone(), &permit)
                    .await
                {
                    Ok(capture) => capture,
                    Err(error) if error.kind() == StorageErrorKind::InputTooLarge => {
                        let Some(evidence) = error.history_archive_budget_evidence() else {
                            return Err(error.into());
                        };
                        evidence.validate_request(&capture_request, &permit, Utc::now())?;
                        authorize(
                            context,
                            subject,
                            scopes,
                            request.collection,
                            resource,
                            evidence.authorization(),
                        )
                        .await?;
                        // Configured authorization can perform external work. Recheck
                        // the same live owner/fence and original generation afterwards.
                        let current = storage.history_archive_catalog(resource, &permit).await?;
                        if current.generation() != capture_request.generation()
                            || self::principal(context, subject).await? != principal
                        {
                            return Err(ApiError::NotFound("History request unavailable".into()));
                        }
                        evidence.validate_request(&capture_request, &permit, Utc::now())?;
                        return Ok(HistoryArchiveAdmission::BudgetLimited);
                    }
                    Err(error) => return Err(error.into()),
                };
                authorize(
                    context,
                    subject,
                    scopes,
                    request.collection,
                    resource,
                    capture.authorization(),
                )
                .await?;
                Ok(HistoryArchiveAdmission::Captured(Self {
                    id: capture.id(),
                    principal,
                    collection: request.collection,
                    resource,
                    permit: permit.clone(),
                    storage: storage.clone(),
                }))
            })
            .await
    }
    async fn checked<S: AuthzSubject + ?Sized>(
        &self,
        context: &AppContext,
        subject: &S,
        scopes: Option<&TokenScope>,
    ) -> Result<HistoryArchiveCapture, ApiError> {
        if !self.storage.same_instance(&storage_handle(context))
            || principal(context, subject).await? != self.principal
        {
            return Err(ApiError::NotFound("History request unavailable".into()));
        }
        let capture = self
            .storage
            .history_archive_capture(self.id, self.principal, &self.permit)
            .await?
            .ok_or_else(|| ApiError::Gone("History request expired or unavailable".into()))?;
        authorize(
            context,
            subject,
            scopes,
            self.collection,
            self.resource,
            capture.authorization(),
        )
        .await?;
        Ok(capture)
    }
    pub async fn execute<S: AuthzSubject + ?Sized>(
        self,
        context: &AppContext,
        subject: &S,
        scopes: Option<&TokenScope>,
    ) -> Result<HistoryArchiveResult, ApiError> {
        let storage = self.storage.clone();
        let scope = storage
            .current_execution_scope()
            .with_application_access(self.permit.clone());
        storage
            .run_in_scope(scope, async move {
                let mut capture = self.checked(context, subject, scopes).await?;
                let mut coverage = capture.coverage(Utc::now());
                let mut row = None;
                let mut charged = 0;
                if coverage == HistoryArchiveCoverageState::Complete {
                    if capture.archive().is_some() {
                        capture = match self
                            .storage
                            .reserve_history_archive_read(
                                self.id,
                                self.principal,
                                ATTEMPT_BYTES as u64,
                                &self.permit,
                            )
                            .await
                        {
                            Ok(capture) => capture,
                            Err(error) if error.kind() == StorageErrorKind::InputTooLarge => {
                                return Ok(HistoryArchiveResult {
                                    query: self,
                                    row: None,
                                    coverage: HistoryArchiveCoverageState::BudgetLimited,
                                    charged_read_bytes: 0,
                                });
                            }
                            Err(error) => return Err(error.into()),
                        };
                        let descriptor = capture.archive().expect("captured pin");
                        let store = self.storage.artifact_store();
                        let mut budget = ByteLimit::new(ATTEMPT_BYTES).start();
                        let read = async {
                            let segment = RetainedHistorySegment::open(
                                store.as_ref(),
                                descriptor.location(),
                                &mut budget,
                            )
                            .await?;
                            if segment.scope()
                                != &SegmentScope::new(
                                    StorageBackupHistorySection::CollectionHistory,
                                    self.resource,
                                )
                            {
                                return Err(ArtifactError::Corrupt(
                                    "Archive scope mismatch".into(),
                                ));
                            }
                            let record = segment
                                .exact(
                                    store.as_ref(),
                                    self.resource,
                                    descriptor.identity().version(),
                                    &mut budget,
                                )
                                .await?
                                .ok_or_else(|| {
                                    ArtifactError::Corrupt("Pinned archive version missing".into())
                                })?;
                            let expected = capture.hot().ok_or_else(|| {
                                ArtifactError::Corrupt("Pinned primary capture missing".into())
                            })?;
                            if record.payload() != expected.payload()
                                || record.dependencies() != descriptor.dependencies()
                                || record.interval().valid_from() != expected.valid_from()
                                || Some(record.interval().valid_to()) != expected.valid_to()
                                || !descriptor.matches_row(expected).map_err(|_| {
                                    ArtifactError::Corrupt(
                                        "Pinned version commitment invalid".into(),
                                    )
                                })?
                            {
                                return Err(ArtifactError::Corrupt(
                                    "Pinned archive differs from captured version".into(),
                                ));
                            }
                            Ok(expected.clone())
                        }
                        .await;
                        charged = budget.used();
                        match read {
                            Ok(value) => row = Some(value),
                            Err(ArtifactError::Limit(_)) => {
                                coverage = HistoryArchiveCoverageState::BudgetLimited
                            }
                            Err(ArtifactError::Corrupt(_)) => {
                                coverage = HistoryArchiveCoverageState::Corrupt
                            }
                            Err(_) => coverage = HistoryArchiveCoverageState::Unavailable,
                        }
                    } else {
                        row = capture.hot().cloned();
                    }
                }
                let latest = self.checked(context, subject, scopes).await?;
                if latest.coverage(Utc::now()) == HistoryArchiveCoverageState::Expired {
                    coverage = HistoryArchiveCoverageState::Expired;
                    row = None;
                }
                Ok(HistoryArchiveResult {
                    query: self,
                    row,
                    coverage,
                    charged_read_bytes: charged,
                })
            })
            .await
    }
}

pub struct HistoryArchiveResult {
    query: HistoryArchiveQuery,
    row: Option<HistoryArchiveRow>,
    coverage: HistoryArchiveCoverageState,
    charged_read_bytes: usize,
}
/// Authorized projection, deliberately distinct from the opaque retained result.
#[derive(Serialize)]
pub struct HistoryArchiveView {
    capture_id: Uuid,
    scope: HistoryArchiveSelection,
    generation: HistoryArchiveCatalogGeneration,
    captured_at: DateTime<Utc>,
    available_until: DateTime<Utc>,
    coverage: HistoryArchiveCoverageState,
    row: Option<HistoryArchiveRow>,
    charged_read_bytes: usize,
}
impl HistoryArchiveView {
    pub const fn coverage(&self) -> HistoryArchiveCoverageState {
        self.coverage
    }
    pub const fn row(&self) -> Option<&HistoryArchiveRow> {
        self.row.as_ref()
    }
    pub const fn generation(&self) -> HistoryArchiveCatalogGeneration {
        self.generation
    }
    pub const fn charged_read_bytes(&self) -> usize {
        self.charged_read_bytes
    }
}
impl HistoryArchiveResult {
    pub async fn access<S: AuthzSubject + ?Sized>(
        self,
        context: &AppContext,
        subject: &S,
        scopes: Option<&TokenScope>,
    ) -> Result<HistoryArchiveView, ApiError> {
        let storage = self.query.storage.clone();
        let scope = storage
            .current_execution_scope()
            .with_application_access(self.query.permit.clone());
        storage
            .run_in_scope(scope, async move {
                let capture = self.query.checked(context, subject, scopes).await?;
                let expired = capture.coverage(Utc::now()) == HistoryArchiveCoverageState::Expired;
                Ok(HistoryArchiveView {
                    capture_id: capture.id(),
                    scope: capture.selection().clone(),
                    generation: capture.generation(),
                    captured_at: capture.captured_at(),
                    available_until: capture.available_until(),
                    coverage: if expired {
                        HistoryArchiveCoverageState::Expired
                    } else {
                        self.coverage
                    },
                    row: if expired { None } else { self.row },
                    charged_read_bytes: self.charged_read_bytes,
                })
            })
            .await
    }
}
