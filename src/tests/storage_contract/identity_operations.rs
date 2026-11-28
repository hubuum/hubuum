use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_group_behavior() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let initial_name = prefix("group_contract");
        let renamed = prefix("group_contract_renamed");
        let created = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    initial_name,
                    Some("storage compatibility group".to_string()),
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create groups")
            .into_value();

        let loaded = backend
            .get_group(created.id())
            .await
            .expect("certified backend should load groups");
        assert_eq!(loaded.id(), created.id());
        assert_eq!(
            backend
                .resolve_group_identity_scope_name(created.id())
                .await
                .expect("certified backend should resolve group identity scopes"),
            LOCAL_IDENTITY_SCOPE
        );

        let updated = backend
            .update_group(
                created.id(),
                StorageGroupUpdate::new(Some(renamed.clone())),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should update groups")
            .into_value();
        assert_eq!(updated.name(), renamed);

        let list_options = QueryOptions::new(Vec::new(), Vec::new(), None, None, true)
            .expect("contract query must be valid");
        let (listed, total_count) = backend
            .list_groups(StorageGroupListQuery::new(list_options))
            .await
            .expect("certified backend should list and count groups")
            .into_parts();
        assert!(listed.iter().any(|group| group.id() == created.id()));
        assert!(total_count.is_some_and(|count| count >= listed.len() as i64));

        let user = create_backend_user(&backend, &prefix("group_contract_user")).await;
        backend
            .add_group_member(user.principal_id, created.id(), &EventContext::system())
            .await
            .expect("certified backend should add group members")
            .into_value();

        let members = backend
            .load_group_member_principals(created.id())
            .await
            .expect("certified backend should list group members");
        assert!(
            members
                .iter()
                .any(|member| member.id() == user.principal_id)
        );
        let query_options = QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
            .expect("contract query must be valid");
        let page = backend
            .list_group_members(created.id(), query_options.clone())
            .await
            .expect("certified backend should page group members");
        assert!(
            page.rows()
                .iter()
                .any(|member| member.principal().id() == user.principal_id)
        );
        assert_eq!(page.total(), Some(1));

        backend
            .remove_group_member(user.principal_id, created.id(), &EventContext::system())
            .await
            .expect("certified backend should remove group members")
            .into_value();
        let empty_page = backend
            .list_group_members(created.id(), query_options)
            .await
            .expect("certified backend should recount group members");
        assert_eq!(empty_page.total(), Some(0));
        assert_eq!(
            backend
                .delete_group(created.id(), &EventContext::system())
                .await
                .expect("certified backend should delete groups")
                .into_value(),
            1
        );
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_principal_behavior() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("principal_contract_user")).await;
        let event_context = EventContext::user(user.principal_id, None, None);
        let loaded = backend
            .get_principal(user.principal_id)
            .await
            .expect("certified backend should load principals");
        assert_eq!(loaded.id(), user.principal_id);

        let initial = backend
            .get_principal_settings(user.principal_id)
            .await
            .expect("certified backend should load principal settings");
        assert_eq!(initial.document(), &serde_json::json!({}));

        let replaced = backend
            .update_principal_settings(
                user.principal_id,
                StoragePrincipalSettingsMutation::Replace(serde_json::json!({
                    "theme": "light",
                    "notifications": {"email": true}
                })),
                &event_context,
            )
            .await
            .expect("certified backend should replace principal settings")
            .into_value();
        assert_eq!(replaced.document()["theme"], "light");

        let merged = backend
            .update_principal_settings(
                user.principal_id,
                StoragePrincipalSettingsMutation::MergePatch(serde_json::json!({
                    "notifications": {"push": true}
                })),
                &event_context,
            )
            .await
            .expect("certified backend should merge principal settings")
            .into_value();
        assert_eq!(merged.document()["notifications"]["email"], true);
        assert_eq!(merged.document()["notifications"]["push"], true);

        let patched = backend
            .update_principal_settings(
                user.principal_id,
                StoragePrincipalSettingsMutation::JsonPatch(serde_json::json!([
                    {"op": "replace", "path": "/theme", "value": "dark"}
                ])),
                &event_context,
            )
            .await
            .expect("certified backend should apply principal settings JSON Patch")
            .into_value();
        assert_eq!(patched.document()["theme"], "dark");

        let reset = backend
            .update_principal_settings(
                user.principal_id,
                StoragePrincipalSettingsMutation::Reset,
                &event_context,
            )
            .await
            .expect("certified backend should reset principal settings")
            .into_value();
        assert_eq!(reset.document(), &serde_json::json!({}));
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_authentication_projections() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("authentication_user")).await;
        let observed_at = chrono::Utc::now();
        let attempt = StorageAuthenticationAttempt::try_new(
            StorageAuthenticationCredential::new(&user.token_hash),
            observed_at,
            observed_at - chrono::Duration::days(1),
        )
        .expect("compatibility authentication window should be valid");
        let authenticated = backend
            .authenticate_bearer_token(attempt)
            .await
            .expect("certified backend should validate active bearer credentials");
        assert_eq!(authenticated.principal_id(), user.principal_id);
        assert!(!authenticated.is_scoped());

        let identity = backend
            .get_authentication_identity(user.principal_id)
            .await
            .expect("certified backend should supply authentication identity data");
        let (principal, human) = identity.into_parts();

        assert_eq!(principal.id(), user.principal_id);
        assert!(principal.is_human());
        assert!(human.is_some());

        let scope = backend
            .get_authentication_token_scope(StorageAuthenticationTokenScopeQuery::new(
                hubuum_domain::TokenId::new(i32::MAX).unwrap(),
                true,
                false,
            ))
            .await
            .expect("certified backend should preserve empty scope dimensions")
            .expect("an enabled scope dimension should produce a scope snapshot");
        let (permissions, resources) = scope.into_parts();
        assert_eq!(permissions, Some(Vec::new()));
        assert_eq!(resources, None);
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_migrates_legacy_token_digests_safely() {
    let _permit = postgres_permit().await;
    let active_key_id = StorageTokenHashKeyId::try_new("active").unwrap();
    let previous_key_id = StorageTokenHashKeyId::try_new("previous").unwrap();

    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("token_rotation_user")).await;
        let old_hash = prefix("legacy_previous_digest");
        let active_hash = prefix("legacy_active_digest");
        backend
            .create_token(StorageTokenCreate::new(
                user.principal_id,
                StorageTokenDigest::legacy_unidentified(&old_hash),
                StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                EventContext::system(),
            ))
            .await
            .expect("certified backend should create a legacy token row")
            .into_value();

        let observed_at = chrono::Utc::now();
        let candidate = |hash: &str, key_id: StorageTokenHashKeyId| {
            StorageAuthenticationCredential::from_digest(
                StorageTokenDigest::try_new(
                    hash,
                    StorageTokenFormat::Legacy,
                    StorageTokenHashAlgorithm::HmacSha256V1,
                    Some(key_id),
                )
                .unwrap(),
            )
        };
        let migration_target = StorageTokenDigest::try_new(
            &active_hash,
            StorageTokenFormat::Legacy,
            StorageTokenHashAlgorithm::HmacSha256V1,
            Some(active_key_id.clone()),
        )
        .unwrap();
        let migrated = backend
            .authenticate_bearer_token(
                StorageAuthenticationAttempt::try_candidates(
                    vec![
                        candidate(&active_hash, active_key_id.clone()),
                        candidate(&old_hash, previous_key_id.clone()),
                    ],
                    Some(migration_target),
                    observed_at,
                    observed_at - chrono::Duration::days(1),
                )
                .unwrap(),
            )
            .await
            .expect("certified backend should validate and migrate a legacy digest");
        assert_eq!(
            migrated.migration_outcome(),
            StorageTokenMigrationOutcome::Migrated
        );

        backend
            .authenticate_bearer_token(
                StorageAuthenticationAttempt::try_new(
                    candidate(&active_hash, active_key_id.clone()),
                    observed_at,
                    observed_at - chrono::Duration::days(1),
                )
                .unwrap(),
            )
            .await
            .expect("the migrated active-key digest should authenticate");
        backend
            .authenticate_bearer_token(
                StorageAuthenticationAttempt::try_new(
                    candidate(&old_hash, previous_key_id.clone()),
                    observed_at,
                    observed_at - chrono::Duration::days(1),
                )
                .unwrap(),
            )
            .await
            .expect_err("the retired legacy digest should no longer authenticate");

        let usage = backend
            .token_key_usage(
                StorageTokenObservation::try_new(
                    observed_at,
                    observed_at - chrono::Duration::days(1),
                )
                .unwrap(),
            )
            .await
            .expect("certified backend should report token key retirement evidence");
        assert!(
            usage
                .iter()
                .any(|item| { item.key_id() == Some(&active_key_id) && item.active() >= 1 })
        );

        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_prevents_versioned_key_fallback() {
    let _permit = postgres_permit().await;
    let active_key_id = StorageTokenHashKeyId::try_new("active").unwrap();
    let previous_key_id = StorageTokenHashKeyId::try_new("previous").unwrap();

    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("versioned_token_key_user")).await;
        let versioned_hash = prefix("versioned_previous_digest");
        backend
            .create_token(StorageTokenCreate::new(
                user.principal_id,
                StorageTokenDigest::try_new(
                    &versioned_hash,
                    StorageTokenFormat::Version1,
                    StorageTokenHashAlgorithm::HmacSha256V1,
                    Some(previous_key_id.clone()),
                )
                .unwrap(),
                StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
                EventContext::system(),
            ))
            .await
            .expect("certified backend should create a versioned token row")
            .into_value();
        let observed_at = chrono::Utc::now();
        let credential = |key_id| {
            StorageAuthenticationCredential::from_digest(
                StorageTokenDigest::try_new(
                    &versioned_hash,
                    StorageTokenFormat::Version1,
                    StorageTokenHashAlgorithm::HmacSha256V1,
                    Some(key_id),
                )
                .unwrap(),
            )
        };

        backend
            .authenticate_bearer_token(
                StorageAuthenticationAttempt::try_new(
                    credential(active_key_id.clone()),
                    observed_at,
                    observed_at - chrono::Duration::days(1),
                )
                .unwrap(),
            )
            .await
            .expect_err("versioned credentials must not fall back to another key ID");
        backend
            .authenticate_bearer_token(
                StorageAuthenticationAttempt::try_new(
                    credential(previous_key_id.clone()),
                    observed_at,
                    observed_at - chrono::Duration::days(1),
                )
                .unwrap(),
            )
            .await
            .expect("versioned credentials should use exactly their selected key ID");

        delete_backend_user(&backend, user).await;
    }
}

#[derive(Clone, Copy)]
enum InactiveLegacyTokenState {
    Revoked,
    Expired,
}

async fn assert_inactive_legacy_token_is_not_migrated(
    backend: &StorageHandle,
    state: InactiveLegacyTokenState,
) {
    let (fixture_name, active_key_name, old_hash_name, active_hash_name) = match state {
        InactiveLegacyTokenState::Revoked => (
            "revoked_token_rotation_user",
            "revoked-target",
            "revoked_legacy_digest",
            "revoked_active_digest",
        ),
        InactiveLegacyTokenState::Expired => (
            "expired_token_rotation_user",
            "expired-target",
            "expired_legacy_digest",
            "expired_active_digest",
        ),
    };
    let user = create_backend_user(backend, &prefix(fixture_name)).await;
    let active_key_id = StorageTokenHashKeyId::try_new(active_key_name).unwrap();
    let previous_key_id = StorageTokenHashKeyId::try_new("inactive-previous").unwrap();
    let old_hash = prefix(old_hash_name);
    let active_hash = prefix(active_hash_name);
    let expires_at = chrono::Utc::now() + chrono::Duration::minutes(1);
    let create = StorageTokenCreate::new(
        user.principal_id,
        StorageTokenDigest::legacy_unidentified(&old_hash),
        StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
        EventContext::system(),
    );
    let create = match state {
        InactiveLegacyTokenState::Revoked => create,
        InactiveLegacyTokenState::Expired => create.expires_at(Some(expires_at)),
    };
    let token = backend
        .create_token(create)
        .await
        .expect("certified backend should create an inactive-token fixture")
        .into_value();
    if matches!(state, InactiveLegacyTokenState::Revoked) {
        backend
            .revoke_token(StorageTokenRevoke::new(
                token.id(),
                user.principal_id,
                EventContext::system(),
            ))
            .await
            .expect("certified backend should revoke the migration fixture")
            .into_value();
    }
    let observed_at = match state {
        InactiveLegacyTokenState::Revoked => chrono::Utc::now(),
        InactiveLegacyTokenState::Expired => expires_at + chrono::Duration::seconds(1),
    };
    let candidate = |hash: &str, key_id: StorageTokenHashKeyId| {
        StorageAuthenticationCredential::from_digest(
            StorageTokenDigest::try_new(
                hash,
                StorageTokenFormat::Legacy,
                StorageTokenHashAlgorithm::HmacSha256V1,
                Some(key_id),
            )
            .unwrap(),
        )
    };
    let migration_target = StorageTokenDigest::try_new(
        &active_hash,
        StorageTokenFormat::Legacy,
        StorageTokenHashAlgorithm::HmacSha256V1,
        Some(active_key_id.clone()),
    )
    .unwrap();

    backend
        .authenticate_bearer_token(
            StorageAuthenticationAttempt::try_candidates(
                vec![
                    candidate(&active_hash, active_key_id.clone()),
                    candidate(&old_hash, previous_key_id),
                ],
                Some(migration_target),
                observed_at,
                observed_at - chrono::Duration::days(1),
            )
            .unwrap(),
        )
        .await
        .expect_err("inactive legacy credentials must not authenticate or migrate");

    let usage = backend
        .token_key_usage(
            StorageTokenObservation::try_new(observed_at, observed_at - chrono::Duration::days(1))
                .unwrap(),
        )
        .await
        .expect("certified backend should report inactive token key usage");
    assert!(
        usage
            .iter()
            .all(|item| item.key_id() != Some(&active_key_id)),
        "an inactive token must retain its pre-migration key identity"
    );

    delete_backend_user(backend, user).await;
}

#[actix_web::test]
async fn every_available_storage_backend_does_not_migrate_revoked_tokens() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        assert_inactive_legacy_token_is_not_migrated(&backend, InactiveLegacyTokenState::Revoked)
            .await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_does_not_migrate_expired_tokens() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        assert_inactive_legacy_token_is_not_migrated(&backend, InactiveLegacyTokenState::Expired)
            .await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_complete_identity_operations() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let username = prefix("identity_contract_user");
        let user = create_backend_user(&backend, &username).await;
        let owner_group = backend
            .create_group(
                StorageGroupCreate::new(
                    None,
                    prefix("identity_contract_owner_group"),
                    Some("identity compatibility owner group".to_string()),
                ),
                &EventContext::system(),
            )
            .await
            .expect("backend-local identity owner group should be created")
            .into_value();
        backend
            .add_group_member(user.principal_id, owner_group.id(), &EventContext::system())
            .await
            .expect("backend-local identity membership should be created")
            .into_value();
        assert!(
            !backend
                .is_default_admin_bootstrap_required()
                .await
                .expect("seeded certified backend should report bootstrap state")
        );
        assert!(
            !backend
                .bootstrap_default_admin(StorageDefaultAdminBootstrap::new(
                    "unused-contract-admin-group",
                    "unused-contract-password-hash",
                ))
                .await
                .expect("certified backend should coordinate administrator bootstrap")
        );
        let local_scope = backend
            .ensure_identity_scope(crate::storage::StorageIdentityScopeEnsure::new(
                LOCAL_IDENTITY_SCOPE,
                crate::models::LOCAL_PROVIDER_KIND,
            ))
            .await
            .expect("certified backend should reconcile identity scopes");
        assert_eq!(
            backend
                .resolve_identity_scope_name(local_scope.id())
                .await
                .expect("certified backend should resolve one identity scope"),
            LOCAL_IDENTITY_SCOPE
        );
        assert_eq!(
            backend
                .resolve_identity_scope_names(vec![local_scope.id()])
                .await
                .expect("certified backend should resolve identity scopes"),
            vec![(local_scope.id(), LOCAL_IDENTITY_SCOPE.to_string())]
        );

        let membership = backend
            .get_principal_group(user.principal_id, owner_group.id())
            .await
            .expect("certified backend should load effective memberships");
        assert_eq!(membership.principal_id(), user.principal_id);
        let group_options = prepare_db_pagination::<crate::models::Group>(
            &QueryOptions::new(Vec::new(), Vec::new(), Some(20), None, true)
                .expect("contract query must be valid"),
        )
        .expect("identity compatibility group query should be valid");
        let (groups, group_total) = backend
            .list_principal_groups(StoragePrincipalGroupListQuery::new(
                user.principal_id,
                group_options,
            ))
            .await
            .expect("certified backend should list principal groups")
            .into_parts();
        assert!(group_total.is_some_and(|total| total >= 1));
        assert!(
            groups
                .into_iter()
                .any(|group| group.id() == owner_group.id())
        );
        assert!(
            backend
                .is_human_owner_group_member(user.principal_id, owner_group.id())
                .await
                .expect("certified backend should evaluate human ownership")
        );

        let event_context = EventContext::user(user.principal_id, None, None);
        let contract_username = prefix("complete_user_contract");
        let contract_user = backend
            .create_user(StorageUserCreate::new(
                None,
                &contract_username,
                "complete-user-contract-password-hash",
                Some("Complete Contract".to_string()),
                Some("complete-contract@example.invalid".to_string()),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create users")
            .into_value();
        let contract_user_id = contract_user.into_parts().id();
        let contract_principal_id = principal_id(contract_user_id.id());
        assert_eq!(
            backend
                .get_user(contract_user_id)
                .await
                .expect("certified backend should load users")
                .into_parts()
                .id(),
            contract_user_id
        );
        assert_eq!(
            backend
                .get_user_by_name(LOCAL_IDENTITY_SCOPE.to_string(), contract_username.clone(),)
                .await
                .expect("certified backend should resolve scoped user names")
                .into_parts()
                .id(),
            contract_user_id
        );
        assert_eq!(
            backend
                .get_user_details(contract_user_id)
                .await
                .expect("certified backend should load user points")
                .into_parts()
                .id(),
            contract_user_id
        );
        let user_options = prepare_db_pagination::<crate::models::UserWithName>(
            &QueryOptions::new(
                vec![
                    ParsedQueryParam::from_parts(
                        FilterField::Id,
                        SearchOperator::Equals { is_negated: false },
                        contract_user_id.to_string(),
                    )
                    .unwrap(),
                ],
                Vec::new(),
                Some(100),
                None,
                true,
            )
            .expect("contract query must be valid"),
        )
        .expect("identity compatibility user query should be valid");
        let (users, user_total) = backend
            .list_users(StorageUserListQuery::new(user_options))
            .await
            .expect("certified backend should list users")
            .into_parts();
        assert!(user_total.is_some_and(|total| total >= 1));
        assert!(users.into_iter().any(|item| {
            item.into_parts().user().clone().into_parts().id() == contract_user_id
        }));
        backend
            .update_user(StorageUserUpdate::new(
                contract_user_id,
                None,
                Some("Updated Contract".to_string()),
                None,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update users")
            .into_value();
        backend
            .set_user_password(StorageUserPasswordUpdate::new(
                contract_user_id,
                "updated-complete-user-contract-password-hash",
                event_context.clone(),
            ))
            .await
            .expect("certified backend should replace local passwords")
            .into_value();

        let token_policy = StorageTokenIssuancePolicy::try_new(24, 24)
            .expect("contract token policy should be valid");
        let token_observed_at = chrono::Utc::now() + chrono::Duration::seconds(1);
        let token_observation = StorageTokenObservation::try_new(
            token_observed_at,
            token_observed_at - chrono::Duration::hours(24),
        )
        .expect("identity compatibility token observation should be valid");
        let first_hash = prefix("complete_token_hash");
        let first_token = backend
            .create_token(StorageTokenCreate::new(
                contract_principal_id,
                StorageTokenDigest::legacy_unidentified(&first_hash),
                token_policy,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create tokens")
            .into_value();
        let first_token_id = first_token.id();
        assert_eq!(
            backend
                .get_token_metadata(contract_principal_id, first_token_id, token_observation)
                .await
                .expect("certified backend should load token metadata")
                .id(),
            first_token_id
        );
        let batch = backend
            .load_token_metadata_by_ids(vec![first_token_id, first_token_id], token_observation)
            .await
            .expect("certified backend should preserve token batch order");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].id(), batch[1].id());

        let second_hash = prefix("complete_renewed_token_hash");
        let renewed = backend
            .renew_token(StorageTokenRenew::new(
                first_token_id,
                contract_principal_id,
                StorageTokenDigest::legacy_unidentified(&second_hash),
                None,
                token_policy,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should renew tokens")
            .into_value();
        assert_ne!(renewed.id(), first_token_id);
        assert_eq!(
            backend
                .revoke_token(StorageTokenRevoke::new(
                    first_token_id,
                    contract_principal_id,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should revoke principal-scoped tokens")
                .into_value(),
            1
        );
        assert_eq!(
            backend
                .revoke_token_by_hash(StorageTokenHashRevoke::new(
                    Some(contract_principal_id),
                    second_hash,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should revoke HMAC-keyed tokens")
                .into_value(),
            1
        );
        let third_hash = prefix("complete_revoke_all_token_hash");
        backend
            .create_token(StorageTokenCreate::new(
                contract_principal_id,
                StorageTokenDigest::legacy_unidentified(third_hash),
                token_policy,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a token for bulk revocation")
            .into_value();
        assert_eq!(
            backend
                .revoke_all_principal_tokens(StoragePrincipalTokensRevoke::new(
                    contract_principal_id,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should revoke all principal tokens")
                .into_value(),
            1
        );
        backend
            .anonymize_user(StorageUserAnonymize::new(
                contract_user_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should anonymize users")
            .into_value();
        assert_eq!(
            backend
                .delete_user(StorageUserDelete::new(
                    contract_user_id,
                    event_context.clone(),
                ))
                .await
                .expect("certified backend should delete users")
                .into_value(),
            1
        );

        let token_options = prepare_db_pagination::<crate::models::PrincipalToken>(
            &QueryOptions::new(Vec::new(), Vec::new(), Some(20), None, true)
                .expect("contract query must be valid"),
        )
        .expect("identity compatibility token query should be valid");
        let (tokens, token_total) = backend
            .list_retained_tokens(StorageTokenListQuery::new(
                user.principal_id,
                token_options,
                StorageTokenListState::Active,
                token_observation,
            ))
            .await
            .expect("certified backend should list retained tokens")
            .into_parts();
        assert_eq!(token_total, Some(1));
        assert_eq!(tokens[0].principal_id(), user.principal_id);
        let password_reset = backend
            .reset_local_password(StorageLocalPasswordReset::new(
                &username,
                "identity-contract-password-hash",
                event_context.clone(),
            ))
            .await
            .expect("certified backend should reset local credentials");
        assert!(password_reset.is_committed());
        assert_eq!(password_reset.audits().map(|audits| audits.len()), Some(1));
        assert_eq!(password_reset.into_value(), 1);

        let service_account_name = prefix("identity_contract_sa");
        let created = backend
            .create_service_account(StorageServiceAccountCreate::new(
                &service_account_name,
                "identity contract",
                owner_group.id(),
                Some(user.principal_id),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create service accounts")
            .into_value();
        let loaded = backend
            .get_service_account(created.id())
            .await
            .expect("certified backend should load service accounts");
        assert_eq!(loaded.owner_group_id(), owner_group.id());
        let point = backend
            .get_service_account_details(created.id())
            .await
            .expect("certified backend should load service-account points");
        assert_eq!(point.into_parts().2, service_account_name);

        let service_account_options =
            prepare_db_pagination::<crate::models::ServiceAccountWithName>(
                &QueryOptions::new(Vec::new(), Vec::new(), Some(100), None, true)
                    .expect("contract query must be valid"),
            )
            .expect("identity compatibility service-account query should be valid");
        let (accounts, account_total) = backend
            .list_manageable_service_accounts(StorageServiceAccountListQuery::new(
                user.principal_id,
                true,
                service_account_options,
            ))
            .await
            .expect("certified backend should list manageable service accounts")
            .into_parts();
        assert!(account_total.is_some_and(|total| total >= 1));
        assert!(accounts.into_iter().any(|account| {
            let (account, _, _, _) = account.into_parts();
            account.id() == created.id()
        }));

        let updated = backend
            .update_service_account(StorageServiceAccountUpdate::new(
                created.id(),
                Some("updated identity contract".to_string()),
                None,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update service accounts")
            .into_value();
        assert_eq!(updated.description(), "updated identity contract");
        assert!(
            !backend
                .is_service_account_disabled(principal_id(created.id().id()))
                .await
                .expect("certified backend should read principal lifecycle")
        );
        let queued_task = backend
            .create_task(
                StorageTaskCreateRequest::builder(
                    StorageTaskKind::Import,
                    principal_id(created.id().id()),
                    serde_json::json!({"items": []}),
                    0,
                )
                .idempotency_key(Some(
                    IdempotencyKey::new(prefix("identity_contract_sa_task"))
                        .expect("identity contract task key should be valid"),
                ))
                .request_hash(Some(prefix("identity_contract_sa_task_hash")))
                .scope_snapshot(StorageTaskScopeSnapshot::unscoped())
                .try_build()
                .expect("identity contract task request should be valid"),
            )
            .await
            .map(expect_legacy_task)
            .expect("certified backend should queue service-account work");
        let (disabled, cancelled_task_kinds) = backend
            .disable_service_account(StorageServiceAccountMutation::new(
                created.id(),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should disable service accounts")
            .into_value()
            .into_parts();
        assert!(disabled.is_disabled());
        assert_eq!(cancelled_task_kinds, vec![StorageTaskKind::Import]);
        let (cancelled_task, _) = backend
            .get_task_access(queued_task.id())
            .await
            .expect("certified backend should expose the cancelled task")
            .into_parts();
        assert_eq!(cancelled_task.status(), StorageTaskStatus::Cancelled);
        assert!(cancelled_task.finished_at().is_some());
        assert!(cancelled_task.request_payload().is_none());
        assert!(cancelled_task.request_redacted_at().is_some());
        assert!(
            backend
                .is_service_account_disabled(principal_id(created.id().id()))
                .await
                .expect("certified backend should observe disabled principals")
        );

        let external_scope = prefix("identity_contract_scope");
        let external_name = prefix("identity_contract_external");
        let external = backend
            .sync_external_user(
                crate::storage::StorageExternalUserSync::builder(
                    &external_scope,
                    "compatibility_provider",
                    prefix("identity_contract_subject"),
                    &external_name,
                )
                .groups(vec![crate::storage::StorageExternalGroup::new(
                    prefix("identity_contract_group_key"),
                    prefix("identity_contract_group"),
                    None,
                )])
                .build(),
            )
            .await
            .expect("certified backend should synchronize external identities")
            .into_value();
        let external_id = external.id();
        let external_state = backend
            .get_external_principal_state(principal_id(external_id.id()))
            .await
            .expect("certified backend should load external identity state")
            .expect("synchronized external identity should have refresh state");
        assert_eq!(external_state.identity_scope(), external_scope);
        backend
            .record_external_sync_failure(
                principal_id(external_id.id()),
                StorageExternalSyncFailure::Unavailable,
            )
            .await
            .expect("certified backend should record external sync attempts");

        backend
            .delete_service_account(StorageServiceAccountMutation::new(
                created.id(),
                event_context,
            ))
            .await
            .expect("certified backend should delete service accounts")
            .into_value();
        backend
            .remove_group_member(user.principal_id, owner_group.id(), &EventContext::system())
            .await
            .expect("backend-local identity membership should be removed")
            .into_value();
        backend
            .delete_group(owner_group.id(), &EventContext::system())
            .await
            .expect("backend-local identity owner group should be removed")
            .into_value();
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn postgres_external_sync_preserves_identity_and_reconciles_membership_sources() {
    use hubuum_storage_core::{StorageExternalGroup, StorageExternalUserSync};

    let _permit = postgres_permit().await;
    let pool = pool();
    let backend = StorageHandle::postgres(pool.get_ref().clone());
    let identity_scope = prefix("external_contract_scope");
    let initial_subject = prefix("external_contract_subject");
    let replacement_subject = prefix("external_contract_subject_reformatted");
    let initial_name = prefix("external_contract_user");
    let renamed = prefix("external_contract_user_renamed");
    let first_group_key = prefix("external_contract_group_first_key");
    let first_group_name = prefix("external_contract_group_first");
    let second_group_key = prefix("external_contract_group_second_key");
    let second_group_name = prefix("external_contract_group_second");
    let request = |subject: &str, name: &str, key: &str, group_name: &str| {
        StorageExternalUserSync::builder(&identity_scope, "compatibility_provider", subject, name)
            .proper_name(Some(format!("{name} Example")))
            .email(Some(format!("{name}@example.org")))
            .groups(vec![StorageExternalGroup::new(
                key,
                group_name,
                Some("directory-owned group".to_string()),
            )])
            .build()
    };

    let first_id = backend
        .sync_external_user(request(
            &initial_subject,
            &initial_name,
            &first_group_key,
            &first_group_name,
        ))
        .await
        .expect("initial external identity sync should succeed")
        .into_value()
        .id();
    let manual_group = backend
        .create_group(
            StorageGroupCreate::new(
                None,
                prefix("external_contract_manual_group"),
                Some("manual membership must survive provider sync".to_string()),
            ),
            &EventContext::system(),
        )
        .await
        .expect("manual group should be created")
        .into_value();
    let _ = backend
        .add_group_member(
            principal_id(first_id.id()),
            manual_group.id(),
            &EventContext::system(),
        )
        .await
        .expect("manual membership should be created")
        .into_value();

    let renamed_id = backend
        .sync_external_user(request(
            &initial_subject,
            &renamed,
            &second_group_key,
            &second_group_name,
        ))
        .await
        .expect("renamed external identity sync should succeed")
        .into_value()
        .id();
    let replacement_error = backend
        .sync_external_user(request(
            &replacement_subject,
            &renamed,
            &second_group_key,
            &second_group_name,
        ))
        .await
        .err()
        .expect("a different external subject must not reuse the existing account");
    assert_eq!(renamed_id, first_id);
    assert_eq!(replacement_error.kind(), StorageErrorKind::Conflict);

    let state = backend
        .get_external_principal_state(principal_id(first_id.id()))
        .await
        .expect("external principal state should load")
        .expect("external principal should remain provider-managed");
    assert_eq!(state.identity_scope(), identity_scope);
    assert_eq!(state.username(), renamed);
    assert_eq!(state.external_subject(), initial_subject);

    let persistence = hubuum_storage_postgres::test_support::external_identity_persistence(
        pool.get_ref(),
        &identity_scope,
        principal_id(first_id.id()),
    )
    .await
    .expect("external sync state should be queryable");
    assert_eq!(persistence.principal_count(), 1);
    assert!(
        persistence
            .memberships()
            .iter()
            .any(|membership| membership.group_id() == manual_group.id())
    );
    assert!(
        persistence
            .memberships()
            .iter()
            .any(|membership| membership.external_key() == Some(second_group_key.as_str()))
    );
    assert!(
        !persistence
            .memberships()
            .iter()
            .any(|membership| membership.external_key() == Some(first_group_key.as_str()))
    );
}
