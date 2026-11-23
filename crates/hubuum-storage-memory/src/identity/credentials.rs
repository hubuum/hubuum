use super::*;

#[async_trait]
impl AuthenticationStorage for MemoryStorage {
    async fn record_credential_creation(
        &self,
        token: TokenId,
        creation: StorageCredentialCreation,
    ) -> Result<(), StorageError> {
        self.state.write().await?.record_creation(token, creation)
    }
    async fn observe_credential_source(
        &self,
        token: TokenId,
        source: ObservedSource,
        policy: CredentialActivityPolicy,
    ) -> Result<(), StorageError> {
        self.state
            .write_authentication()
            .await?
            .observe_source(token, source, policy)
    }
    async fn credential_activity(
        &self,
        query: StorageActivityQuery,
    ) -> Result<StorageCredentialActivity, StorageError> {
        self.state.read().await?.read_activity(query)
    }
    async fn purge_credential_sources(
        &self,
        policy: CredentialActivityPolicy,
        limit: usize,
    ) -> Result<usize, StorageError> {
        Ok(self.state.write().await?.purge_sources(policy, limit))
    }

    async fn create_auth_session(
        &self,
        request: StorageSessionCreate,
    ) -> Result<StorageMutationOutcome<StorageSessionIssued>, StorageError> {
        let mut live = self.state.write().await?;
        let mut staged = live.clone();
        let result = staged.create_session(request)?;
        *live = staged;
        Ok(result)
    }
    async fn refresh_principal(
        &self,
        credential: RefreshCredential,
    ) -> Result<PrincipalId, StorageError> {
        self.state.read().await?.refresh_owner(&credential)
    }
    async fn rotate_auth_session(
        &self,
        request: StorageSessionRotate,
    ) -> Result<StorageMutationOutcome<StorageSessionRotation>, StorageError> {
        let mut live = self.state.write().await?;
        let mut staged = live.clone();
        let result = staged.rotate_session(request)?;
        *live = staged;
        Ok(result)
    }
    async fn revoke_auth_sessions(
        &self,
        target: SessionRevocation,
        context: EventContext,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let mut live = self.state.write().await?;
        let mut staged = live.clone();
        let result = staged.revoke_sessions(target, context)?;
        *live = staged;
        Ok(result)
    }
    async fn purge_auth_sessions(&self, limit: usize) -> Result<usize, StorageError> {
        let mut state = self.state.write().await?;
        let purged = state.purge_sessions(limit);
        state.cleanup_credential_activity();
        Ok(purged)
    }

    async fn authenticate_bearer_token(
        &self,
        attempt: StorageAuthenticationAttempt,
    ) -> Result<StorageAuthenticatedToken, StorageError> {
        let (credentials, migration_target, observed_at, legacy_valid_after) = attempt.into_parts();
        let observation = StorageTokenObservation::try_new(observed_at, legacy_valid_after)
            .map_err(invalid_contract_value)?;
        let mut state = self.state.write_authentication().await?;
        let matching = state
            .tokens
            .values()
            .filter(|record| {
                credentials
                    .iter()
                    .any(|credential| record.matches_credential(credential))
            })
            .map(|record| record.id)
            .collect::<Vec<_>>();
        if matching.len() > 1 {
            return Err(StorageError::internal(
                "multiple token rows matched one bearer credential",
            ));
        }
        let token_id = matching
            .first()
            .copied()
            .ok_or_else(|| StorageError::authentication_required("Invalid bearer token"))?;
        let token = state
            .tokens
            .get(&token_id.id())
            .cloned()
            .ok_or_else(|| StorageError::internal("authenticated token disappeared"))?;
        let metadata = token.metadata(observation)?;
        if !metadata.is_active() || !state.session_allows_token(token_id, observed_at) {
            return Err(StorageError::authentication_required(
                "Bearer token is expired or revoked",
            ));
        }
        let principal = state
            .principals
            .get(&token.principal_id.id())
            .ok_or_else(|| {
                StorageError::authentication_required("Token principal was not found")
            })?;
        if principal.kind().is_service_account()
            && state
                .service_accounts
                .get(&principal.id().id())
                .is_some_and(|account| account.is_disabled())
        {
            return Err(StorageError::authentication_required(
                "Token principal is disabled",
            ));
        }
        let permission_scoped = token
            .scope
            .clone()
            .map(StorageAuthenticationTokenScope::into_parts)
            .is_some_and(|parts| parts.0.is_some());
        let resource_scoped = token
            .scope
            .clone()
            .map(StorageAuthenticationTokenScope::into_parts)
            .is_some_and(|parts| parts.1.is_some());
        let migration_target = migration_target.filter(|_| {
            token.token_format == StorageTokenFormat::Legacy && token.token_hash_key_id.is_none()
        });
        if migration_target.as_ref().is_some_and(|target| {
            state.tokens.values().any(|candidate| {
                candidate.id != token_id && target.matches_lookup_value(&candidate.token_hash)
            })
        }) {
            return Err(StorageError::internal(
                "legacy token migration target conflicts with another token row",
            ));
        }
        let record = state
            .tokens
            .get_mut(&token_id.id())
            .ok_or_else(|| StorageError::internal("authenticated token disappeared"))?;
        let migration_outcome = if let Some(target) = migration_target {
            record.migrate_legacy_digest(target);
            StorageTokenMigrationOutcome::Migrated
        } else {
            StorageTokenMigrationOutcome::NotNeeded
        };
        record.last_used_at = Some(observed_at);
        StorageAuthenticatedToken::builder(
            token.id,
            token.principal_id,
            token.issued,
            token.revision,
        )
        .name(token.name)
        .description(token.description)
        .expires_at(token.expires_at)
        .last_used_at(Some(observed_at))
        .permission_scoped(permission_scoped)
        .resource_scoped(resource_scoped)
        .migration_outcome(migration_outcome)
        .try_build()
        .map_err(invalid_contract_value)
    }

    async fn get_authentication_identity(
        &self,
        principal_id: PrincipalId,
    ) -> Result<StorageAuthenticationIdentity, StorageError> {
        let state = self.state.read().await?;
        let principal = state.principals.get(&principal_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Principal {} was not found", principal_id.id()))
        })?;
        let projection = StorageAuthenticationPrincipal::new(
            principal.id(),
            principal.kind(),
            principal.name(),
            principal.identity_scope_id(),
        );
        let human = if principal.kind().is_human() {
            let parts = state
                .users
                .get(&principal_id.id())
                .ok_or_else(|| StorageError::internal("human principal has no user record"))?
                .user
                .clone()
                .into_parts();
            Some(
                StorageAuthenticationHuman::try_new(
                    parts.id(),
                    parts.proper_name().map(ToOwned::to_owned),
                    parts.email().map(ToOwned::to_owned),
                    parts.created_at(),
                    parts.updated_at(),
                    parts.anonymized_at(),
                )
                .map_err(invalid_contract_value)?,
            )
        } else {
            None
        };
        StorageAuthenticationIdentity::try_new(projection, human).map_err(invalid_contract_value)
    }

    async fn get_authentication_token_scope(
        &self,
        query: StorageAuthenticationTokenScopeQuery,
    ) -> Result<Option<StorageAuthenticationTokenScope>, StorageError> {
        if !query.is_scoped() {
            return Ok(None);
        }
        let persisted = self
            .state
            .read()
            .await?
            .tokens
            .get(&query.token_id().id())
            .and_then(|record| record.scope.clone());
        let (persisted_permissions, persisted_resources) = persisted
            .map(StorageAuthenticationTokenScope::into_parts)
            .unwrap_or_default();
        Ok(Some(StorageAuthenticationTokenScope::new(
            query
                .is_permission_scoped()
                .then(|| persisted_permissions.unwrap_or_default()),
            query.is_resource_scoped().then(|| {
                persisted_resources.unwrap_or_else(StorageAuthenticationResourceScope::default)
            }),
        )))
    }
}

#[async_trait]
impl LocalIdentityCredentialStorage for MemoryStorage {
    async fn is_default_admin_bootstrap_required(&self) -> Result<bool, StorageError> {
        Ok(self.state.read().await?.users.is_empty())
    }

    async fn bootstrap_default_admin(
        &self,
        _request: StorageDefaultAdminBootstrap,
    ) -> Result<bool, StorageError> {
        Ok(false)
    }

    async fn reset_local_password(
        &self,
        request: StorageLocalPasswordReset,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (name, password_hash, context) = request.into_parts();
        let mut state = self.state.write().await?;
        let user_id = state
            .users
            .iter()
            .find(|(_, record)| {
                record.name == name
                    && state
                        .identity_scopes
                        .get(&record.identity_scope_id.id())
                        .is_some_and(|scope| scope.name() == LOCAL_IDENTITY_SCOPE)
            })
            .map(|(id, _)| *id)
            .ok_or_else(|| StorageError::not_found(format!("User '{name}' was not found")))?;
        let record = state
            .users
            .get(&user_id)
            .cloned()
            .ok_or_else(|| StorageError::internal("password reset user disappeared"))?;
        let parts = record.user.into_parts();
        let now = Utc::now();
        let user = StorageUser::try_new(
            parts.id(),
            Some(password_hash),
            parts.proper_name().map(ToOwned::to_owned),
            parts.email().map(ToOwned::to_owned),
            parts.created_at(),
            now,
            parts.anonymized_at(),
        )
        .map_err(invalid_contract_value)?;
        state
            .users
            .get_mut(&user_id)
            .expect("password reset user exists")
            .user = user;
        let principal = state
            .principals
            .get(&user_id)
            .cloned()
            .ok_or_else(|| StorageError::internal("password reset principal is missing"))?;
        state.store_principal(
            user_id,
            advanced_principal(
                &principal,
                principal.name(),
                principal.settings().clone(),
                now,
            )?,
        );
        state.invalidate_principal_sessions(user_id);
        for token in state.tokens.values_mut() {
            if token.principal_id.id() == user_id && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
            }
        }
        let receipt = state.append_simple_event(
            EntityType::User,
            user_id,
            Some(&name),
            Action::Updated,
            &context,
            format!("User '{name}' password reset"),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }
}

#[async_trait]
impl TokenStorage for MemoryStorage {
    async fn create_credential_approval(
        &self,
        request: StorageCredentialApprovalCreate,
    ) -> Result<StorageMutationOutcome<StorageCredentialApprovalMetadata>, StorageError> {
        let mut live = self.state.write().await?;
        let mut staged = live.clone();
        let result = staged.create_credential_approval(request)?;
        *live = staged;
        Ok(result)
    }
    async fn get_credential_approval(
        &self,
        id: i32,
    ) -> Result<StorageCredentialApprovalMetadata, StorageError> {
        self.state
            .read()
            .await?
            .credential_approvals
            .metadata(id)
            .cloned()
            .ok_or_else(|| StorageError::not_found("Credential approval not found"))
    }

    async fn list_retained_tokens(
        &self,
        query: StorageTokenListQuery,
    ) -> Result<StoragePage<StorageTokenMetadata>, StorageError> {
        let (principal_id, options, list_state, observation) = query.into_parts();
        let state = self.state.read().await?;
        let rows = state
            .tokens
            .values()
            .filter(|record| record.principal_id == principal_id)
            .map(|record| record.metadata(observation))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|metadata| token_state_matches(metadata, list_state))
            .collect();
        page(rows, &options)
    }

    async fn token_key_usage(
        &self,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenKeyUsage>, StorageError> {
        #[derive(Default)]
        struct Usage {
            active: i64,
            revoked: i64,
            expired: i64,
            latest_validation: Option<DateTime<Utc>>,
            earliest_expiry: Option<DateTime<Utc>>,
            latest_expiry: Option<DateTime<Utc>>,
        }

        let state = self.state.read().await?;
        let mut usage = BTreeMap::<Option<StorageTokenHashKeyId>, Usage>::new();
        for token in state.tokens.values() {
            let metadata = token.metadata(observation)?;
            let item = usage.entry(token.token_hash_key_id.clone()).or_default();
            if metadata.revoked_at().is_some() {
                item.revoked += 1;
            } else if metadata.is_expired() {
                item.expired += 1;
            } else {
                item.active += 1;
            }
            item.latest_validation = item.latest_validation.max(token.last_used_at);
            item.earliest_expiry = match (item.earliest_expiry, token.expires_at) {
                (Some(current), Some(candidate)) => Some(current.min(candidate)),
                (None, candidate) => candidate,
                (current, None) => current,
            };
            item.latest_expiry = item.latest_expiry.max(token.expires_at);
        }
        for session in state.sessions.values() {
            let expiry = session.idle.min(session.absolute);
            for (digest, _, _) in session.refresh.values() {
                let item = usage.entry(digest.key_id().cloned()).or_default();
                if session.revoked {
                    item.revoked += 1;
                } else if expiry <= observation.into_parts().0 {
                    item.expired += 1;
                } else {
                    item.active += 1;
                }
                item.earliest_expiry = Some(
                    item.earliest_expiry
                        .map_or(expiry, |current| current.min(expiry)),
                );
                item.latest_expiry = item.latest_expiry.max(Some(expiry));
            }
        }
        usage
            .into_iter()
            .map(|(key_id, usage)| {
                StorageTokenKeyUsage::try_new(
                    key_id,
                    usage.active,
                    usage.revoked,
                    usage.expired,
                    usage.latest_validation,
                    usage.earliest_expiry,
                    usage.latest_expiry,
                )
                .map_err(invalid_contract_value)
            })
            .collect()
    }

    async fn create_token(
        &self,
        request: StorageTokenCreate,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        let mut live = self.state.write().await?;
        let mut staged = live.clone();
        let result = staged.issue_token(request)?;
        *live = staged;
        Ok(result)
    }

    async fn renew_token(
        &self,
        mut request: StorageTokenRenew,
    ) -> Result<StorageMutationOutcome<StorageTokenMetadata>, StorageError> {
        let claim = request.take_credential_claim();
        let (source_id, principal_id, digest, expires_at, policy, context) = request.into_parts();
        let mut live = self.state.write().await?;
        let mut staged = live.clone();
        let approval_audit = staged.consume_credential_claim(
            claim.as_ref(),
            StorageCredentialOperation::RenewToken,
            Some(principal_id),
            &context,
        )?;
        let source = staged
            .tokens
            .get(&source_id.id())
            .cloned()
            .filter(|source| source.principal_id == principal_id)
            .ok_or_else(|| StorageError::not_found("Source token not found"))?;
        if staged.session_for_token(source_id).is_some() {
            return Err(StorageError::invalid_input(
                "Interactive access tokens must be renewed through session refresh",
            ));
        }
        if source.revoked_at.is_some() {
            return Err(StorageError::conflict("Revoked tokens cannot be renewed"));
        }
        let result = staged.issue_token(
            StorageTokenCreate::new(principal_id, digest, policy, context)
                .name(source.name)
                .description(source.description)
                .expires_at(expires_at)
                .scope(source.scope),
        )?;
        let result = match (result, approval_audit) {
            (StorageMutationOutcome::Committed { value, audits }, Some(approval)) => {
                let mut receipts = audits.into_vec();
                receipts.push(approval);
                StorageMutationOutcome::committed_with_audits(
                    value,
                    StorageAuditReceipts::try_from_vec(receipts).map_err(invalid_contract_value)?,
                )
            }
            (result, _) => result,
        };
        *live = staged;
        Ok(result)
    }

    async fn get_token_metadata(
        &self,
        principal_id: PrincipalId,
        token_id: TokenId,
        observation: StorageTokenObservation,
    ) -> Result<StorageTokenMetadata, StorageError> {
        let state = self.state.read().await?;
        let token = state.tokens.get(&token_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Token {} was not found", token_id.id()))
        })?;
        if token.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                token_id.id(),
                principal_id.id()
            )));
        }
        token.metadata(observation)
    }

    async fn load_token_metadata_by_ids(
        &self,
        token_ids: Vec<TokenId>,
        observation: StorageTokenObservation,
    ) -> Result<Vec<StorageTokenMetadata>, StorageError> {
        let state = self.state.read().await?;
        token_ids
            .into_iter()
            .map(|id| {
                state
                    .tokens
                    .get(&id.id())
                    .ok_or_else(|| {
                        StorageError::not_found(format!("Token {} was not found", id.id()))
                    })?
                    .metadata(observation)
            })
            .collect()
    }

    async fn revoke_token(
        &self,
        request: StorageTokenRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (token_id, principal_id, context) = request.into_parts();
        let mut state = self.state.write().await?;
        let token = state.tokens.get_mut(&token_id.id()).ok_or_else(|| {
            StorageError::not_found(format!("Token {} was not found", token_id.id()))
        })?;
        if token.principal_id != principal_id {
            return Err(StorageError::not_found(format!(
                "Token {} was not found for principal {}",
                token_id.id(),
                principal_id.id()
            )));
        }
        if token.revoked_at.is_some() {
            return Ok(StorageMutationOutcome::unchanged(0));
        }
        if state.session_for_token(token_id).is_some() {
            return state.revoke_sessions(SessionRevocation::Current(token_id), context);
        }
        let token = state
            .tokens
            .get_mut(&token_id.id())
            .expect("validated token exists");
        token.revoked_at = Some(Utc::now());
        token.revision = token
            .revision
            .checked_advance()
            .map_err(|error| StorageError::internal(error.to_string()))?;
        let receipt = state.append_simple_event(
            EntityType::Token,
            token_id.id(),
            None,
            Action::Revoked,
            &context,
            format!("Token {} revoked", token_id.id()),
        )?;
        Ok(StorageMutationOutcome::committed(1, receipt))
    }

    async fn revoke_token_by_hash(
        &self,
        request: StorageTokenHashRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (principal_id, credentials, context) = request.into_parts();
        let token_id = self
            .state
            .read()
            .await?
            .tokens
            .values()
            .find(|token| {
                credentials
                    .iter()
                    .any(|credential| token.matches_credential(credential))
                    && principal_id.is_none_or(|id| token.principal_id == id)
            })
            .map(|token| token.id);
        let Some(token_id) = token_id else {
            return Ok(StorageMutationOutcome::unchanged(0));
        };
        let owner = self
            .state
            .read()
            .await?
            .tokens
            .get(&token_id.id())
            .expect("token selected by id exists")
            .principal_id;
        self.revoke_token(StorageTokenRevoke::new(token_id, owner, context))
            .await
    }

    async fn revoke_all_principal_tokens(
        &self,
        request: StoragePrincipalTokensRevoke,
    ) -> Result<StorageMutationOutcome<usize>, StorageError> {
        let (principal_id, context) = request.into_parts();
        let mut state = self.state.write().await?;
        let now = Utc::now();
        let mut revoked = Vec::new();
        state.invalidate_principal_sessions(principal_id.id());
        for token in state.tokens.values_mut() {
            if token.principal_id == principal_id && token.revoked_at.is_none() {
                token.revoked_at = Some(now);
                token.revision = token
                    .revision
                    .checked_advance()
                    .map_err(|error| StorageError::internal(error.to_string()))?;
                revoked.push(token.id);
            }
        }
        if revoked.is_empty() {
            return Ok(StorageMutationOutcome::unchanged(0));
        }
        let receipts = revoked
            .into_iter()
            .map(|token_id| {
                state.append_simple_event(
                    EntityType::Token,
                    token_id.id(),
                    None,
                    Action::Revoked,
                    &context,
                    format!("Token {} revoked", token_id.id()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let count = receipts.len();
        let audits = StorageAuditReceipts::try_from_vec(receipts)
            .map_err(|error| StorageError::internal(error.to_string()))?;
        Ok(StorageMutationOutcome::committed_with_audits(count, audits))
    }
}
