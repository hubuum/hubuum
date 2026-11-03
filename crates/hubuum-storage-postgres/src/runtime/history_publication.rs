//! History authority entry precedes API-owner validation. Publication-owner
//! SHARE is acquired afterward; settlement never uses this productive path.

use diesel::sql_types::{Nullable, SmallInt, Uuid as SqlUuid};

use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum HistoryEntryMode {
    Read,
    Control,
    Admission,
}

pub(super) enum HistoryPublicationRequirement {
    Legacy,
    Owned,
}

impl PostgresRuntime {
    pub(super) async fn require_history_authority(
        &self,
        connection: &mut PostgresConnection,
        context: &TransactionLocalContext,
    ) -> Result<HistoryPublicationRequirement, PostgresStorageError> {
        // Unbound pools are supported only by focused adapter tests. Production
        // composition always captures its registered application permit.
        if self.runtime_owner.is_none()
            && context
                .application_access
                .as_ref()
                .is_none_or(|permit| permit.owner().is_none())
            && context.history_read.is_none()
            && context.history_mode == HistoryEntryMode::Read
        {
            return Ok(HistoryPublicationRequirement::Legacy);
        }
        if context.history_read.is_none()
            && context.history_mode == HistoryEntryMode::Read
            && context
                .application_access
                .as_ref()
                .and_then(StorageExecutionPermit::owner)
                .is_some_and(|owner| !owner.history_protocol().supports_online())
        {
            return Ok(HistoryPublicationRequirement::Legacy);
        }
        let owner = context
            .application_access
            .as_ref()
            .and_then(StorageExecutionPermit::owner);
        let protocol = diesel::select(
            diesel::dsl::sql::<SmallInt>("public.hubuum_history_read_authority(")
                .bind::<Nullable<SqlUuid>, _>(owner.map(RuntimeOwner::id))
                .sql(",")
                .bind::<Nullable<SqlUuid>, _>(owner.map(RuntimeOwner::ownership_token))
                .sql(",")
                .bind::<Bool, _>(context.history_mode == HistoryEntryMode::Control)
                .sql(")"),
        )
        .get_result::<i16>(connection)
        .await?;
        match protocol {
            0 => Ok(HistoryPublicationRequirement::Legacy),
            1 => Ok(HistoryPublicationRequirement::Owned),
            _ => Err(PostgresStorageError::unavailable(
                "Unknown history producer protocol",
            )),
        }
    }

    pub(super) async fn require_history_publication(
        &self,
        connection: &mut PostgresConnection,
        context: &TransactionLocalContext,
        requirement: HistoryPublicationRequirement,
    ) -> Result<(), PostgresStorageError> {
        if context.history_mode != HistoryEntryMode::Read {
            return Ok(());
        }
        let Some(publication) = &context.history_read else {
            return match requirement {
                HistoryPublicationRequirement::Legacy => Ok(()),
                HistoryPublicationRequirement::Owned => Err(PostgresStorageError::unavailable(
                    "History-aware productive work requires its original publication owner",
                )),
            };
        };
        if context.application_access.as_ref() != Some(publication.epoch().application()) {
            return Err(PostgresStorageError::unavailable(
                "History publication belongs to a different application scope",
            ));
        }
        let epoch = publication.epoch();
        let allowed = diesel::select(
            diesel::dsl::sql::<Bool>("public.hubuum_history_publication_execution(")
                .bind::<SqlUuid, _>(publication.id())
                .sql(",")
                .bind::<SqlUuid, _>(publication.ownership_token())
                .sql(",")
                .bind::<SqlUuid, _>(epoch.history().incarnation())
                .sql(",")
                .bind::<BigInt, _>(epoch.history().deletion_revision().value() as i64)
                .sql(",")
                .bind::<BigInt, _>(epoch.history().authority_revision().value() as i64)
                .sql(",")
                .bind::<BigInt, _>(epoch.generation().value() as i64)
                .sql(")"),
        )
        .get_result::<bool>(connection)
        .await?;
        if !allowed {
            return Err(PostgresStorageError::unavailable(
                "History publication owner is stale, retired or fenced",
            ));
        }
        Ok(())
    }

    pub(crate) async fn with_history_publication_admission<F, R, E>(
        &self,
        operation: F,
    ) -> Result<R, PostgresStorageError>
    where
        F: for<'connection> AsyncFnOnce(&'connection mut PostgresConnection) -> Result<R, E>
            + for<'connection> SendAsyncFn<
                &'connection mut PostgresConnection,
                Result<R, E>,
                Fut: Send,
            > + Send,
        R: Send,
        E: Send,
        PostgresStorageError: From<E>,
    {
        let mut context = TransactionLocalContext::ambient();
        if context.history_read.is_some() {
            return Err(PostgresStorageError::conflict(
                "Nested work cannot refresh a publication owner",
            ));
        }
        context.history_mode = HistoryEntryMode::Admission;
        self.with_transaction_context(operation, context).await
    }

    pub(crate) async fn with_history_control_transaction<F, R, E>(
        &self,
        fallback: StorageExecutionPermit,
        operation: F,
    ) -> Result<R, PostgresStorageError>
    where
        F: for<'connection> AsyncFnOnce(&'connection mut PostgresConnection) -> Result<R, E>
            + for<'connection> SendAsyncFn<
                &'connection mut PostgresConnection,
                Result<R, E>,
                Fut: Send,
            > + Send,
        R: Send,
        E: Send,
        PostgresStorageError: From<E>,
    {
        let mut context = TransactionLocalContext::ambient();
        context.application_access = context.application_access.or(Some(fallback));
        context.history_mode = HistoryEntryMode::Control;
        self.with_transaction_context(operation, context).await
    }
}
