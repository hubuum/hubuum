use super::*;

pub(super) fn capture(
    state: &MemoryState,
    sections: &mut StorageBackupStateSections,
    progress: &mut StorageBackupCaptureProgress,
) -> Result<(), StorageError> {
    sections.insert(StorageBackupStateSection::IdentityScopes, state.identity_scopes.values().map(|v| row(json!({"id": v.id().id(), "name": v.name(), "provider_kind": v.provider_kind(), "created_at": v.created_at(), "updated_at": v.updated_at(), "revision": v.revision().get()}))).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::Groups, state.groups.values().map(|v| row(json!({"id": v.id().id(), "name": v.name(), "description": v.description(), "identity_scope_id": v.identity_scope_id().id(), "managed_by": v.managed_by(), "external_key": v.external_key(), "last_sync_attempted_at": v.last_sync_attempted_at(), "last_sync_success_at": v.last_sync_success_at(), "created_at": v.created_at(), "updated_at": v.updated_at(), "revision": v.revision().get()}))).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::Principals, state.principals.values().map(|v| row(json!({"id": v.id().id(), "kind": v.kind().as_str(), "name": v.name(), "identity_scope_id": v.identity_scope_id().id(), "provider_managed": v.provider_managed(), "settings": v.settings(), "external_subject": v.external_subject(), "last_sync_attempted_at": v.last_sync_attempted_at(), "last_sync_success_at": v.last_sync_success_at(), "created_at": v.created_at(), "updated_at": v.updated_at(), "revision": v.revision().get()}))).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::ServiceAccounts, state.service_accounts.values().map(|v| row(json!({"id": v.id().id(), "kind": "service_account", "description": v.description(), "owner_group_id": v.owner_group_id().id(), "created_by": v.created_by().map(PrincipalId::id), "disabled_at": v.disabled_at(), "created_at": v.created_at(), "updated_at": v.updated_at()}))).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::GroupMemberships, state.memberships.values().map(|v| row(json!({"principal_id": v.principal_id().id(), "group_id": v.group_id().id(), "created_at": v.created_at(), "updated_at": v.updated_at(), "revision": v.revision().get()}))).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::CollectionPermissionGrants, state.authorization_grants.values().map(|v| row(json!({"id": v.id().id(), "collection_id": v.collection_id().id(), "group_id": v.group_id().id(), "permissions": v.permissions().iter().map(|p| permission_name(*p)).collect::<Vec<_>>(), "created_at": v.created_at(), "updated_at": v.updated_at()}))).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::Users, state.users.values().map(|record| {
        let v = record.user.clone().into_parts();
        row(json!({"id": v.id().id(), "kind": "human", "proper_name": v.proper_name(), "email": v.email(), "created_at": v.created_at(), "updated_at": v.updated_at(), "anonymized_at": v.anonymized_at()}))
    }).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    sections.insert(StorageBackupStateSection::CollectionAuthorization, state.collections.keys().map(|id| row(json!({"collection_id": id, "revision": state.authorization_revisions.get(id).copied().unwrap_or(ResourceRevision::INITIAL).get()}))).map(|row| capture_row(progress, row)).collect::<Result<_, _>>()?);
    let mut hierarchy = Vec::new();
    for collection in state.collections.values() {
        let mut ancestor = Some(collection.id());
        let mut depth = 0;
        let mut visited = BTreeSet::new();
        while let Some(id) = ancestor {
            if !visited.insert(id) {
                return Err(invalid("collection hierarchy cycle"));
            }
            hierarchy.push(capture_row(progress, row(json!({"ancestor_collection_id": id.id(), "descendant_collection_id": collection.id().id(), "depth": depth})))?);
            ancestor = state
                .collections
                .get(&id.id())
                .ok_or_else(|| invalid("collection parent"))?
                .parent_collection_id();
            depth += 1;
        }
    }
    hierarchy.sort_by_key(|r| {
        (
            r.get("ancestor_collection_id").and_then(Value::as_i64),
            r.get("descendant_collection_id").and_then(Value::as_i64),
        )
    });
    sections.insert(StorageBackupStateSection::CollectionHierarchy, hierarchy);
    let mut sources = Vec::new();
    let mut sourced_memberships = BTreeSet::new();
    for source in &state.membership_sources {
        progress.scan_row()?;
        let row = Row(source);
        let key = (row.integer("principal_id")?, row.integer("group_id")?);
        if state.memberships.contains_key(&key) {
            sources.push(retain_row(progress, Ok(source.clone()))?);
            sourced_memberships.insert(key);
        }
    }
    for membership in state.memberships.values() {
        let key = (membership.principal_id().id(), membership.group_id().id());
        if !sourced_memberships.contains(&key) {
            let group = state
                .groups
                .get(&key.1)
                .ok_or_else(|| invalid("membership group"))?;
            let external = state.external_memberships.contains(&key);
            sources.push(capture_row(progress, row(json!({"principal_id": key.0, "group_id": key.1,
                "source": if external { EXTERNAL_MEMBERSHIP_SOURCE } else { MANUAL_MEMBERSHIP_SOURCE },
                "source_scope_id": group.identity_scope_id().id(),
                "source_key": if external { group.external_key().unwrap_or_default() } else { "" },
                "created_at": membership.created_at(), "updated_at": membership.updated_at()})))?);
        }
    }
    sources.sort_by(|left, right| {
        for field in [
            "principal_id",
            "group_id",
            "source",
            "source_scope_id",
            "source_key",
        ] {
            let ordering = if matches!(field, "source" | "source_key") {
                left.get(field)
                    .and_then(Value::as_str)
                    .cmp(&right.get(field).and_then(Value::as_str))
            } else {
                left.get(field)
                    .and_then(Value::as_i64)
                    .cmp(&right.get(field).and_then(Value::as_i64))
            };
            if !ordering.is_eq() {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    });
    sections.insert(StorageBackupStateSection::GroupMembershipSources, sources);
    Ok(())
}

pub(super) fn restore(
    sections: &StorageBackupStateSections,
    state: &mut MemoryState,
) -> Result<(), StorageError> {
    macro_rules! id {
        ($kind:ident, $r:ident, $field:literal) => {
            $kind::new($r.integer($field)?).map_err(|_| invalid($field))?
        };
    }
    macro_rules! opt_id {
        ($kind:ident, $r:ident, $field:literal) => {
            $r.optional_integer($field)?
                .map($kind::new)
                .transpose()
                .map_err(|_| invalid($field))?
        };
    }
    state.identity_scopes.clear();
    state.groups.clear();
    state.principals.clear();
    state.users.clear();
    for row in &sections[&StorageBackupStateSection::IdentityScopes] {
        let r = Row(row);
        let value = StorageIdentityScope::try_new(
            id!(IdentityScopeId, r, "id"),
            r.text("name")?,
            r.text("provider_kind")?,
            r.time("created_at")?,
            r.time("updated_at")?,
            r.revision()?,
        )
        .map_err(invalid_contract_value)?;
        state.identity_scopes.insert(value.id().id(), value);
    }
    for row in &sections[&StorageBackupStateSection::Groups] {
        let r = Row(row);
        let value = StorageIdentityGroup::builder(
            r.metadata()?,
            r.text("name")?,
            r.text("description")?,
            id!(IdentityScopeId, r, "identity_scope_id"),
            r.text("managed_by")?,
        )
        .external_key(r.optional_text("external_key")?)
        .last_sync_attempted_at(r.optional_time("last_sync_attempted_at")?)
        .last_sync_success_at(r.optional_time("last_sync_success_at")?)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.groups.insert(value.id().id(), value);
    }
    for row in &sections[&StorageBackupStateSection::Principals] {
        let r = Row(row);
        let value = StoragePrincipal::builder(
            r.metadata()?,
            r.text("kind")?.parse().map_err(|_| invalid("kind"))?,
            r.text("name")?,
            id!(IdentityScopeId, r, "identity_scope_id"),
        )
        .provider_managed(r.boolean("provider_managed")?)
        .settings(r.value("settings")?.clone())
        .external_subject(r.optional_text("external_subject")?)
        .last_sync_attempted_at(r.optional_time("last_sync_attempted_at")?)
        .last_sync_success_at(r.optional_time("last_sync_success_at")?)
        .try_build()
        .map_err(invalid_contract_value)?;
        state.principals.insert(value.id().id(), value);
    }
    for row in &sections[&StorageBackupStateSection::Users] {
        let r = Row(row);
        let id = r.integer("id")?;
        let principal = state
            .principals
            .get(&id)
            .ok_or_else(|| invalid("user principal"))?;
        let user = StorageUser::try_new(
            id!(UserId, r, "id"),
            None,
            r.optional_text("proper_name")?,
            r.optional_text("email")?,
            r.time("created_at")?,
            r.time("updated_at")?,
            r.optional_time("anonymized_at")?,
        )
        .map_err(invalid_contract_value)?;
        state.users.insert(
            id,
            MemoryUserRecord {
                user,
                identity_scope_id: principal.identity_scope_id(),
                name: principal.name().to_string(),
                provider_managed: principal.provider_managed(),
                external_subject: principal.external_subject().map(ToOwned::to_owned),
                last_sync_attempted_at: principal.last_sync_attempted_at(),
                last_sync_success_at: principal.last_sync_success_at(),
            },
        );
    }
    for row in &sections[&StorageBackupStateSection::ServiceAccounts] {
        let r = Row(row);
        let value = StorageServiceAccount::try_new(
            id!(ServiceAccountId, r, "id"),
            r.text("description")?,
            id!(GroupId, r, "owner_group_id"),
            opt_id!(PrincipalId, r, "created_by"),
            r.optional_time("disabled_at")?,
            r.time("created_at")?,
            r.time("updated_at")?,
        )
        .map_err(invalid_contract_value)?;
        state.service_accounts.insert(value.id().id(), value);
    }
    state.memberships.clear();
    for row in &sections[&StorageBackupStateSection::GroupMemberships] {
        let r = Row(row);
        let value = StoragePrincipalGroup::try_new(
            id!(PrincipalId, r, "principal_id"),
            id!(GroupId, r, "group_id"),
            r.time("created_at")?,
            r.time("updated_at")?,
            r.revision()?,
        )
        .map_err(invalid_contract_value)?;
        state
            .memberships
            .insert((value.principal_id().id(), value.group_id().id()), value);
    }
    state.membership_sources = sections[&StorageBackupStateSection::GroupMembershipSources].clone();
    for row in &state.membership_sources {
        let r = Row(row);
        if r.text("source")? != "manual" {
            state
                .external_memberships
                .insert((r.integer("principal_id")?, r.integer("group_id")?));
        }
    }
    for row in &sections[&StorageBackupStateSection::CollectionAuthorization] {
        let r = Row(row);
        state
            .authorization_revisions
            .insert(r.integer("collection_id")?, r.revision()?);
    }
    for row in &sections[&StorageBackupStateSection::CollectionPermissionGrants] {
        let r = Row(row);
        let permissions = r
            .value("permissions")?
            .as_array()
            .ok_or_else(|| invalid("permissions"))?
            .iter()
            .map(|v| {
                let name = v.as_str().ok_or_else(|| invalid("permissions"))?;
                StorageAuthorizationPermission::ALL
                    .into_iter()
                    .find(|permission| permission_name(*permission) == name)
                    .ok_or_else(|| invalid("permissions"))
            })
            .collect::<Result<Vec<StorageAuthorizationPermission>, _>>()?;
        let value = StorageAuthorizationGrant::try_new(
            id!(AuthorizationGrantId, r, "id"),
            id!(CollectionId, r, "collection_id"),
            id!(GroupId, r, "group_id"),
            permissions,
            r.time("created_at")?,
            r.time("updated_at")?,
        )
        .map_err(invalid_contract_value)?;
        state
            .authorization_grants
            .insert((value.collection_id().id(), value.group_id().id()), value);
    }
    state.next_identity_scope_id = next_id(&state.identity_scopes)?;
    state.next_principal_id = next_id(&state.principals)?;
    state.next_group_id = next_id(&state.groups)?;
    state.next_authorization_grant_id = state
        .authorization_grants
        .values()
        .map(|v| v.id().id())
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| invalid("authorization grant sequence"))?;
    Ok(())
}

fn permission_name(permission: StorageAuthorizationPermission) -> String {
    let mut name = String::new();
    for ch in permission.as_str().chars() {
        if ch.is_ascii_uppercase() && !name.is_empty() {
            name.push('_');
        }
        name.push(ch.to_ascii_lowercase());
    }
    name
}
