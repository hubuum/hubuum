use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    lifecycle: Value,
    redemption: InteractionDigest,
    user_code: Option<InteractionDigest>,
    actor: Option<StorageInteractionActorLease>,
    provider: Option<ProviderDocument>,
    authenticated: Option<EvidenceDocument>,
    #[serde(default)]
    protocol: Option<StorageProtectedAuthenticationState>,
    #[serde(default)]
    retained: Option<crate::StorageRetainedAuthentication>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderDocument {
    name: String,
    id: Uuid,
    kind: String,
    namespace: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum EvidenceDocument {
    Login { login: StorageInteractionLogin },
    Verification { candidate: Value },
}

impl StorageInteractionRecord {
    /// Adapter-only persistence document; never project this to an API response.
    pub fn to_document(&self) -> Result<Value, StorageValidationError> {
        let authenticated = self.authenticated.as_ref().map(|evidence| match evidence {
            StorageInteractionEvidence::Login(login) => EvidenceDocument::Login {
                login: login.clone(),
            },
            StorageInteractionEvidence::Verification(candidate) => EvidenceDocument::Verification {
                candidate: candidate.to_document(),
            },
        });
        let value = serde_json::to_value(Document {
            lifecycle: self.lifecycle.to_document(),
            redemption: self.redemption.clone(),
            user_code: self.user_code.clone(),
            actor: self.actor.clone(),
            provider: self.provider.as_ref().map(|p| ProviderDocument {
                name: p.name().to_owned(),
                id: p.id(),
                kind: p.kind().as_str().to_owned(),
                namespace: p.namespace().to_owned(),
            }),
            authenticated,
            protocol: self.protocol.clone(),
            retained: self.retained.clone(),
        })
        .map_err(|_| invalid())?;
        if value.to_string().len() > MAX_AUTH_INTERACTION_DOCUMENT_BYTES {
            return Err(invalid());
        }
        Ok(value)
    }

    pub fn from_document(value: Value) -> Result<Self, StorageValidationError> {
        if value.to_string().len() > MAX_AUTH_INTERACTION_DOCUMENT_BYTES {
            return Err(invalid());
        }
        let document: Document = serde_json::from_value(value).map_err(|_| invalid())?;
        let lifecycle =
            InteractionLifecycle::from_document(document.lifecycle).map_err(|_| invalid())?;
        let authenticated = document
            .authenticated
            .map(|evidence| -> Result<_, StorageValidationError> {
                let proof = lifecycle.authenticated_subject().ok_or_else(invalid)?;
                match evidence {
                    EvidenceDocument::Login { login } => {
                        login.validate(lifecycle.binding())?;
                        if lifecycle.binding().provider().external_id().is_none()
                            && proof.subject() != login.subject.principal_id().id().to_string()
                        {
                            return Err(invalid());
                        }
                        Ok(StorageInteractionEvidence::Login(login))
                    }
                    EvidenceDocument::Verification { candidate } => {
                        let candidate = StorageSecurityCandidate::from_document(candidate)?;
                        Ok(StorageInteractionAuthentication::verification(
                            proof.clone(),
                            candidate,
                        )?
                        .evidence)
                    }
                }
            })
            .transpose()?;
        let record = Self {
            lifecycle,
            redemption: document.redemption,
            user_code: document.user_code,
            actor: document.actor,
            provider: document
                .provider
                .map(|p| {
                    StorageProviderIdentityBinding::try_new(
                        p.name,
                        p.id,
                        crate::StorageAuthenticationProviderKind::parse(&p.kind)?,
                        p.namespace,
                    )
                })
                .transpose()?,
            authenticated,
            protocol: document.protocol,
            retained: document.retained,
        };
        record.validate_bindings()?;
        if record.authenticated.is_some() != record.lifecycle.authenticated_subject().is_some()
            || (record.lifecycle.mode() == InteractionMode::Browser) != record.user_code.is_some()
            || record.user_code.as_ref() == Some(&record.redemption)
        {
            return Err(invalid());
        }
        Ok(record)
    }
}
