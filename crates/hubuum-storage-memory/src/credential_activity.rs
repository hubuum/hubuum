use super::*;
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ActivityOwner {
    Token(i32),
    Session(Uuid),
}
#[derive(Clone, Default)]
pub(super) struct MemoryActivity {
    creation: Option<StorageCredentialCreation>,
    sources: BTreeMap<ObservedSource, StorageCredentialSource>,
    overflow_since: Option<DateTime<Utc>>,
}
impl MemoryState {
    pub(super) fn cleanup_credential_activity(&mut self) {
        let tokens = &self.tokens;
        let sessions = &self.sessions;
        let principals = &self.principals;
        self.credential_activity.retain(|owner, activity| {
            if let Some(creation) = &activity.creation
                && creation
                    .issuer()
                    .is_some_and(|issuer| !principals.contains_key(&issuer.id()))
            {
                activity.creation = Some(StorageCredentialCreation::new(
                    None,
                    creation.method(),
                    creation.source(),
                    creation.issued_at(),
                ));
            }
            match owner {
                ActivityOwner::Token(id) => tokens.contains_key(id),
                ActivityOwner::Session(id) => sessions.contains_key(id),
            }
        });
    }

    fn activity_owner(&self, token: TokenId) -> Result<ActivityOwner, StorageError> {
        if !self.tokens.contains_key(&token.id()) {
            return Err(StorageError::not_found("Credential not found"));
        }
        Ok(self
            .session_for_token(token)
            .map_or(ActivityOwner::Token(token.id()), ActivityOwner::Session))
    }
    pub(super) fn record_creation(
        &mut self,
        token: TokenId,
        creation: StorageCredentialCreation,
    ) -> Result<(), StorageError> {
        let owner = self.activity_owner(token)?;
        self.credential_activity
            .entry(owner)
            .or_default()
            .creation
            .get_or_insert(creation);
        Ok(())
    }
    pub(super) fn observe_source(
        &mut self,
        token: TokenId,
        source: ObservedSource,
        policy: CredentialActivityPolicy,
    ) -> Result<(), StorageError> {
        if !policy.enabled() {
            return Ok(());
        }
        let owner = self.activity_owner(token)?;
        let activity = self.credential_activity.entry(owner).or_default();
        let now = Utc::now();
        activity
            .sources
            .retain(|_, entry| entry.last_seen() >= now - Duration::days(policy.retention_days()));
        if let Some(existing) = activity.sources.get_mut(&source) {
            if existing.last_seen() <= now - Duration::seconds(60) {
                *existing = StorageCredentialSource::new(
                    source,
                    existing.first_seen(),
                    now.max(existing.last_seen()),
                );
            }
        } else if activity.sources.len() >= policy.source_cap() {
            activity.overflow_since.get_or_insert(now);
        } else {
            activity
                .sources
                .insert(source, StorageCredentialSource::new(source, now, now));
        }
        Ok(())
    }
    pub(super) fn read_activity(
        &self,
        query: StorageActivityQuery,
    ) -> Result<StorageCredentialActivity, StorageError> {
        if self
            .tokens
            .get(&query.token_id().id())
            .is_none_or(|t| t.principal_id != query.principal_id())
        {
            return Err(StorageError::not_found("Credential not found"));
        }
        let owner = self.activity_owner(query.token_id())?;
        let session = match owner {
            ActivityOwner::Session(id) => Some(AuthSessionId::new(id)),
            _ => None,
        };
        let Some(activity) = self.credential_activity.get(&owner) else {
            return Ok(StorageCredentialActivity::new(
                session,
                None,
                vec![],
                0,
                None,
            ));
        };
        let cutoff = Utc::now() - Duration::days(query.policy().retention_days());
        let mut sources: Vec<_> = activity
            .sources
            .values()
            .filter(|s| s.last_seen() >= cutoff)
            .cloned()
            .collect();
        sources.sort_by_key(|s| (s.first_seen(), s.source()));
        let total = sources.len();
        let sources = sources
            .into_iter()
            .skip(query.offset())
            .take(query.limit())
            .collect();
        Ok(StorageCredentialActivity::new(
            session,
            activity.creation.clone(),
            sources,
            total,
            activity.overflow_since,
        ))
    }
    pub(super) fn purge_sources(
        &mut self,
        policy: CredentialActivityPolicy,
        limit: usize,
    ) -> usize {
        self.credential_activity.retain(|owner, _| match owner {
            ActivityOwner::Token(id) => self.tokens.contains_key(id),
            ActivityOwner::Session(id) => self.sessions.contains_key(id),
        });
        let cutoff = Utc::now() - Duration::days(policy.retention_days());
        let mut deleted = 0;
        for activity in self.credential_activity.values_mut() {
            activity.sources.retain(|_, entry| {
                if deleted < limit && entry.last_seen() < cutoff {
                    deleted += 1;
                    false
                } else {
                    true
                }
            });
        }
        deleted
    }
}
