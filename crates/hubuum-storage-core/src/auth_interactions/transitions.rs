use super::*;

impl StorageInteractionRecord {
    /// This check is deliberately separate from mutation so an unauthorized
    /// caller cannot expire, fail or consume another client's interaction.
    pub fn authorizes(&self, command: &StorageInteractionCommand) -> bool {
        match (&command.authorization, &command.action) {
            (StorageInteractionAuthorization::Redemption(access), action) => {
                self.allows(access)
                    && matches!(
                        action,
                        StorageInteractionAction::Poll
                            | StorageInteractionAction::Cancel
                            | StorageInteractionAction::Inspect
                    )
                    || self.allows(access)
                        && matches!(action, StorageInteractionAction::Claim(_))
                        && self.lifecycle.mode() == InteractionMode::Credentials
            }
            (StorageInteractionAuthorization::Browser { id, binding }, action) => {
                *id == self.lifecycle.binding().id()
                    && self.lifecycle.browser() == Some(binding)
                    && matches!(
                        action,
                        StorageInteractionAction::Claim(_)
                            | StorageInteractionAction::PrepareProtocol(_)
                            | StorageInteractionAction::ClaimProtocol { .. }
                            | StorageInteractionAction::Decide { .. }
                            | StorageInteractionAction::Inspect
                    )
            }
            (StorageInteractionAuthorization::Claim(claim), action) => {
                self.lifecycle.authentication_claim() == Some(claim)
                    && matches!(
                        action,
                        StorageInteractionAction::Authenticate { .. }
                            | StorageInteractionAction::AuthenticateExternal { .. }
                            | StorageInteractionAction::Reject
                            | StorageInteractionAction::Fail
                    )
            }
        }
    }

    pub fn apply(
        &mut self,
        command: StorageInteractionCommand,
        now: DateTime<Utc>,
    ) -> StorageInteractionOutcome {
        if !self.authorizes(&command) {
            return StorageInteractionOutcome::Rejected(InteractionTransitionError::Invalid);
        }
        let result = self.apply_transition(command, now);
        self.discard_inactive_evidence(now);
        result.unwrap_or_else(StorageInteractionOutcome::Rejected)
    }

    fn apply_transition(
        &mut self,
        command: StorageInteractionCommand,
        now: DateTime<Utc>,
    ) -> Result<StorageInteractionOutcome, InteractionTransitionError> {
        if !self.authorizes(&command) {
            return Err(InteractionTransitionError::Invalid);
        }
        match (command.authorization, command.action) {
            (
                StorageInteractionAuthorization::Browser { binding, .. },
                StorageInteractionAction::PrepareProtocol(protocol),
            ) => {
                if self.protocol.is_some()
                    || self.lifecycle.binding().provider().external_id().is_none()
                {
                    return Err(InteractionTransitionError::Invalid);
                }
                self.lifecycle.bind_browser(binding, now)?;
                self.protocol = Some(protocol);
            }
            (
                StorageInteractionAuthorization::Browser { binding, .. },
                StorageInteractionAction::ClaimProtocol { attempt, state },
            ) => {
                if self
                    .protocol
                    .as_ref()
                    .is_none_or(|protocol| protocol.state() != &state)
                {
                    return Err(InteractionTransitionError::Invalid);
                }
                let claim = self
                    .lifecycle
                    .claim_authentication(attempt, Some(&binding), now)?;
                let protocol = self.protocol.take().expect("validated protocol state");
                return Ok(StorageInteractionOutcome::ProtocolClaim(Box::new(
                    StorageProtocolClaim {
                        claim,
                        lifecycle: self.lifecycle.clone(),
                        protocol,
                    },
                )));
            }
            (StorageInteractionAuthorization::Claim(_), StorageInteractionAction::Fail) => {
                self.invalidate();
            }
            (_, StorageInteractionAction::Inspect) => {
                self.lifecycle.inspect_browser(now)?;
            }
            (_, StorageInteractionAction::Poll) => {
                self.lifecycle.poll(now)?;
            }
            (_, StorageInteractionAction::Cancel) => {
                self.lifecycle.cancel(now)?;
            }
            (authorization, StorageInteractionAction::Claim(attempt)) => {
                if self.protocol.is_some() {
                    return Err(InteractionTransitionError::Invalid);
                }
                let browser = match &authorization {
                    StorageInteractionAuthorization::Browser { binding, .. } => Some(binding),
                    _ => None,
                };
                return self
                    .lifecycle
                    .claim_authentication(attempt, browser, now)
                    .map(StorageInteractionOutcome::Claim);
            }
            (
                StorageInteractionAuthorization::Claim(claim),
                StorageInteractionAction::Authenticate {
                    authentication,
                    approval,
                },
            ) => {
                match &authentication.evidence {
                    StorageInteractionEvidence::Login(login) => {
                        if login.authenticated_at > now
                            || self.lifecycle.binding().authority().purpose()
                                == InteractionPurpose::CredentialApproval
                                && now - login.authenticated_at
                                    > Duration::seconds(crate::CREDENTIAL_APPROVAL_LIFETIME_SECONDS)
                        {
                            return Err(InteractionTransitionError::Invalid);
                        }
                    }
                    StorageInteractionEvidence::Verification(candidate) => {
                        let Some(crate::StorageRuntimeSettingsValue::Authentication(providers)) =
                            candidate.verification_assignment().value()
                        else {
                            return Err(InteractionTransitionError::Invalid);
                        };
                        let selected = providers
                            .providers()
                            .iter()
                            .find(|provider| {
                                provider.id() == self.lifecycle.binding().provider().external_id()
                            })
                            .and_then(|provider| {
                                StorageProviderIdentityBinding::from_provider(provider).ok()
                            });
                        if selected.as_ref() != self.provider.as_ref()
                            || candidate.state(now) != StorageSecurityCandidateState::Verified
                        {
                            return Err(InteractionTransitionError::Invalid);
                        }
                    }
                }
                self.lifecycle.finish_authentication(
                    &claim,
                    authentication.proof,
                    approval,
                    now,
                )?;
                self.authenticated = Some(authentication.evidence);
            }
            (StorageInteractionAuthorization::Claim(claim), StorageInteractionAction::Reject) => {
                self.lifecycle.reject_authentication(&claim, now)?;
            }
            (
                StorageInteractionAuthorization::Browser { binding, .. },
                StorageInteractionAction::Decide { approve },
            ) => {
                self.lifecycle.decide_browser(&binding, approve, now)?;
            }
            _ => return Err(InteractionTransitionError::Invalid),
        }
        Ok(StorageInteractionOutcome::View(self.view(now)))
    }
}
