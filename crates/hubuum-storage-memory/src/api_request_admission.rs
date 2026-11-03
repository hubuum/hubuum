use super::*;
use hubuum_storage_core::{
    ApiRequestAdmissionStorage, ApiRequestClass, ApiRequestPolicy, ApiRequestRejection,
    StorageApiRequestPermit, StorageApiRequestPolicy, StorageApiRequestPolicyUpdate,
    StorageIdentitySubject, StorageIdentityTarget, TaskPolicyRevision,
};
use std::sync::Mutex;
use std::time::{Duration as StdDuration, Instant};

#[derive(Clone)]
pub(super) struct MemoryApiRequests(Arc<Mutex<AdmissionState>>);

struct AdmissionState {
    policy: StorageApiRequestPolicy,
    owners: BTreeMap<Uuid, RequestOwner>,
    admitted_attempts: u128,
    charged_milliseconds: u128,
}

struct RequestOwner {
    principal: PrincipalIdentity,
    incarnation: Uuid,
    deadline: Instant,
}

impl Default for MemoryApiRequests {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(AdmissionState {
            policy: StorageApiRequestPolicy::new(
                TaskPolicyRevision::new(1).expect("valid initial revision"),
                ApiRequestPolicy::default(),
            ),
            owners: BTreeMap::new(),
            admitted_attempts: 0,
            charged_milliseconds: 0,
        })))
    }
}

impl MemoryApiRequests {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, AdmissionState>, StorageError> {
        self.0
            .lock()
            .map_err(|_| StorageError::api_request(ApiRequestRejection::ReconciliationRequired))
    }

    pub(super) fn check(
        &self,
        permit: &StorageApiRequestPermit,
        incarnation: Uuid,
    ) -> Result<(), StorageError> {
        let state = self.lock()?;
        let owner = state
            .owners
            .get(&permit.owner())
            .filter(|owner| owner.incarnation == incarnation)
            .ok_or_else(|| {
                StorageError::api_request(ApiRequestRejection::ReconciliationRequired)
            })?;
        if owner.deadline <= Instant::now() {
            return Err(StorageError::api_request(
                ApiRequestRejection::DeadlineExceeded,
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl ApiRequestAdmissionStorage for MemoryStorage {
    async fn api_request_policy(&self) -> Result<StorageApiRequestPolicy, StorageError> {
        Ok(self.state.read().await?.api_requests.lock()?.policy.clone())
    }

    async fn replace_api_request_policy(
        &self,
        update: StorageApiRequestPolicyUpdate,
    ) -> Result<StorageApiRequestPolicy, StorageError> {
        let state = self.state.write().await?;
        let mut control = state.api_requests.lock()?;
        if control.policy.revision() != update.expected_revision() {
            return Err(StorageError::conflict(
                "API request policy changed; refetch before updating",
            ));
        }
        control.policy = StorageApiRequestPolicy::new(
            update
                .expected_revision()
                .next()
                .map_err(invalid_contract_value)?,
            update.policy().clone(),
        );
        Ok(control.policy.clone())
    }

    async fn admit_api_request(
        &self,
        principal: PrincipalId,
        _class: ApiRequestClass,
        input_bytes: u64,
    ) -> Result<StorageApiRequestPermit, StorageError> {
        let state = self.state.write().await?;
        if state
            .ensure_application_permit(&StorageExecutionPermit::new(
                None,
                state.application_control.fence(),
            ))
            .is_err()
        {
            return Err(StorageError::api_request(
                ApiRequestRejection::ReconciliationRequired,
            ));
        }
        let identity = state
            .logical_identities
            .record(StorageIdentityTarget::Principal(principal))?
            .subject()
            .and_then(|subject| match subject {
                StorageIdentitySubject::Principal { identity } => identity.known_identity(),
                _ => None,
            })
            .ok_or_else(|| StorageError::api_request(ApiRequestRejection::IdentityUnavailable))?;
        let incarnation = state.application_control.fence().incarnation();
        let mut control = state.api_requests.lock()?;
        // Expiry alone cannot prove HTTP-side work has stopped. The supervisor
        // settles the owner explicitly; abandoned owners fail closed.
        let policy = control.policy.clone();
        if input_bytes > policy.policy().input_bytes().bytes() {
            return Err(StorageError::api_request(ApiRequestRejection::InputBytes));
        }
        if control
            .owners
            .values()
            .filter(|owner| owner.principal == identity)
            .count() as u64
            >= policy.policy().principal().get()
        {
            return Err(StorageError::api_request(ApiRequestRejection::Principal));
        }
        if control.owners.len() as u64 >= policy.policy().deployment().get() {
            return Err(StorageError::api_request(ApiRequestRejection::Deployment));
        }
        let owner = Uuid::new_v4();
        let milliseconds = policy.policy().duration().milliseconds();
        control.admitted_attempts = control
            .admitted_attempts
            .checked_add(1)
            .ok_or_else(|| StorageError::internal("API attempt accounting exhausted"))?;
        control.charged_milliseconds = control
            .charged_milliseconds
            .checked_add(u128::from(milliseconds))
            .ok_or_else(|| StorageError::internal("API time accounting exhausted"))?;
        control.owners.insert(
            owner,
            RequestOwner {
                principal: identity,
                incarnation,
                deadline: Instant::now() + StdDuration::from_millis(milliseconds),
            },
        );
        StorageApiRequestPermit::try_new(owner, policy.revision(), policy.policy().clone())
            .map_err(invalid_contract_value)
    }

    async fn settle_api_request(
        &self,
        permit: StorageApiRequestPermit,
    ) -> Result<(), StorageError> {
        self.state
            .write_control()
            .await
            .api_requests
            .lock()?
            .owners
            .remove(&permit.owner());
        Ok(())
    }
}
