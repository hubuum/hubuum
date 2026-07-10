use chrono::NaiveDateTime;
use diesel::prelude::*;
use hubuum_auth_core::AuthenticatedExternalUser;
use std::collections::HashSet;

use crate::db::traits::identity::ensure_identity_scope;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::{
    EXTERNAL_MEMBERSHIP_SOURCE, LOCAL_PROVIDER_KIND, Principal, PrincipalKind, User,
};

pub struct ExternalPrincipalState {
    pub identity_scope: String,
    pub external_subject: String,
    pub last_sync_attempted_at: Option<NaiveDateTime>,
    pub last_sync_success_at: Option<NaiveDateTime>,
}

pub async fn external_principal_state(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<Option<ExternalPrincipalState>, ApiError> {
    use crate::schema::{identity_scopes, principals, users};

    let row = with_connection(pool, |conn| {
        users::table
            .inner_join(principals::table.on(users::id.eq(principals::id)))
            .inner_join(
                identity_scopes::table.on(principals::identity_scope_id.eq(identity_scopes::id)),
            )
            .filter(users::id.eq(principal_id_value))
            .select((
                identity_scopes::provider_kind,
                principals::provider_managed,
                principals::external_subject,
                principals::last_sync_attempted_at,
                principals::last_sync_success_at,
                identity_scopes::name,
            ))
            .first::<(
                String,
                bool,
                Option<String>,
                Option<NaiveDateTime>,
                Option<NaiveDateTime>,
                String,
            )>(conn)
            .optional()
    })?;

    let Some((
        provider,
        provider_managed,
        external_subject,
        last_sync_attempted_at,
        last_sync_success_at,
        identity_scope,
    )) = row
    else {
        return Ok(None);
    };
    if provider == LOCAL_PROVIDER_KIND || !provider_managed {
        return Ok(None);
    }
    let Some(external_subject) = external_subject else {
        return Err(ApiError::ServiceUnavailable(
            "External user is missing provider subject".to_string(),
        ));
    };
    Ok(Some(ExternalPrincipalState {
        identity_scope,
        external_subject,
        last_sync_attempted_at,
        last_sync_success_at,
    }))
}

pub fn mark_external_sync_attempted(
    pool: &DbPool,
    principal_id_value: i32,
) -> Result<(), ApiError> {
    use crate::schema::principals;
    let attempted_at = now();
    with_connection(pool, |conn| {
        diesel::update(principals::table.filter(principals::id.eq(principal_id_value)))
            .set(principals::last_sync_attempted_at.eq(attempted_at))
            .execute(conn)
    })?;
    Ok(())
}

pub async fn sync_external_user(
    pool: &DbPool,
    scope_name: &str,
    provider_kind: &str,
    authenticated: AuthenticatedExternalUser,
) -> Result<User, ApiError> {
    let scope = ensure_identity_scope(pool, scope_name, provider_kind).await?;
    let sync_time = now();
    let profile = authenticated.profile;
    let groups = authenticated.groups;
    let synced_group_count = groups.len();

    with_transaction(pool, |conn| -> Result<User, ApiError> {
        use crate::schema::{group_membership_sources, group_memberships, groups as groups_table};
        use crate::schema::{principals, users};

        let existing_by_subject = principals::table
            .filter(principals::identity_scope_id.eq(scope.id))
            .filter(principals::external_subject.eq(&profile.subject))
            .select(principals::all_columns)
            .first::<Principal>(conn)
            .optional()?;

        let principal = if let Some(existing) = existing_by_subject {
            if existing.name == profile.name {
                existing
            } else {
                diesel::update(principals::table.filter(principals::id.eq(existing.id)))
                    .set((
                        principals::name.eq(&profile.name),
                        principals::provider_managed.eq(true),
                        principals::last_sync_attempted_at.eq(sync_time),
                        principals::last_sync_success_at.eq(sync_time),
                    ))
                    .get_result::<Principal>(conn)?
            }
        } else {
            let inserted = diesel::insert_into(principals::table)
                .values((
                    principals::identity_scope_id.eq(scope.id),
                    principals::kind.eq(PrincipalKind::Human.as_str()),
                    principals::name.eq(&profile.name),
                    principals::provider_managed.eq(true),
                    principals::external_subject.eq(&profile.subject),
                    principals::last_sync_attempted_at.eq(sync_time),
                    principals::last_sync_success_at.eq(sync_time),
                ))
                .on_conflict_do_nothing()
                .get_result::<Principal>(conn)
                .optional()?;

            match inserted {
                Some(principal) => principal,
                None => {
                    let principal = principals::table
                        .filter(principals::identity_scope_id.eq(scope.id))
                        .filter(principals::name.eq(&profile.name))
                        .first::<Principal>(conn)?;
                    if principal.provider_managed
                        && principal.external_subject.as_deref() == Some(profile.subject.as_str())
                    {
                        principal
                    } else {
                        return Err(ApiError::Conflict(
                            "identity scope already contains a different principal with this name"
                                .to_string(),
                        ));
                    }
                }
            }
        };

        if principal.kind != PrincipalKind::Human.as_str() {
            return Err(ApiError::Conflict(
                "external identity subject belongs to a non-human principal".to_string(),
            ));
        }

        let user = diesel::insert_into(users::table)
            .values((
                users::id.eq(principal.id),
                users::password.eq::<Option<String>>(None),
                users::proper_name.eq(&profile.proper_name),
                users::email.eq(&profile.email),
            ))
            .on_conflict(users::id)
            .do_update()
            .set((
                users::proper_name.eq(&profile.proper_name),
                users::email.eq(&profile.email),
            ))
            .get_result::<User>(conn)?;

        diesel::update(principals::table.filter(principals::id.eq(principal.id)))
            .set((
                principals::provider_managed.eq(true),
                principals::external_subject.eq(&profile.subject),
                principals::last_sync_attempted_at.eq(sync_time),
                principals::last_sync_success_at.eq(sync_time),
            ))
            .execute(conn)?;

        let mut synced_group_ids = Vec::new();
        for group in groups {
            let description = group.description.unwrap_or_default();
            let saved = diesel::insert_into(groups_table::table)
                .values((
                    groups_table::identity_scope_id.eq(scope.id),
                    groups_table::groupname.eq(&group.name),
                    groups_table::description.eq(&description),
                    groups_table::managed_by.eq(provider_kind),
                    groups_table::external_key.eq(&group.key),
                    groups_table::last_sync_attempted_at.eq(sync_time),
                    groups_table::last_sync_success_at.eq(sync_time),
                ))
                .on_conflict((groups_table::identity_scope_id, groups_table::external_key))
                .do_update()
                .set((
                    groups_table::groupname.eq(&group.name),
                    groups_table::description.eq(&description),
                    groups_table::managed_by.eq(provider_kind),
                    groups_table::last_sync_attempted_at.eq(sync_time),
                    groups_table::last_sync_success_at.eq(sync_time),
                ))
                .get_result::<crate::models::Group>(conn)?;
            synced_group_ids.push(saved.id);

            diesel::insert_into(group_memberships::table)
                .values((
                    group_memberships::principal_id.eq(user.id),
                    group_memberships::group_id.eq(saved.id),
                ))
                .on_conflict_do_nothing()
                .execute(conn)?;
            let source_key = saved.external_key.clone().unwrap_or_default();
            diesel::insert_into(group_membership_sources::table)
                .values((
                    group_membership_sources::principal_id.eq(user.id),
                    group_membership_sources::group_id.eq(saved.id),
                    group_membership_sources::source.eq(EXTERNAL_MEMBERSHIP_SOURCE),
                    group_membership_sources::source_scope_id.eq(scope.id),
                    group_membership_sources::source_key.eq(&source_key),
                ))
                .on_conflict_do_nothing()
                .execute(conn)?;
        }

        diesel::delete(
            group_membership_sources::table
                .filter(group_membership_sources::principal_id.eq(user.id))
                .filter(group_membership_sources::source.eq(EXTERNAL_MEMBERSHIP_SOURCE))
                .filter(group_membership_sources::source_scope_id.eq(scope.id))
                .filter(diesel::dsl::not(
                    group_membership_sources::group_id.eq_any(&synced_group_ids),
                )),
        )
        .execute(conn)?;

        let retained: HashSet<i32> = group_membership_sources::table
            .filter(group_membership_sources::principal_id.eq(user.id))
            .select(group_membership_sources::group_id)
            .load::<i32>(conn)?
            .into_iter()
            .collect();
        let current: Vec<i32> = group_memberships::table
            .filter(group_memberships::principal_id.eq(user.id))
            .select(group_memberships::group_id)
            .load(conn)?;
        for group_id in current {
            if !retained.contains(&group_id) {
                diesel::delete(
                    group_memberships::table
                        .filter(group_memberships::principal_id.eq(user.id))
                        .filter(group_memberships::group_id.eq(group_id)),
                )
                .execute(conn)?;
            }
        }

        let event_context = EventContext::system();
        let event = NewEvent::new(
            EntityType::ExternalIdentitySync,
            Action::Succeeded,
            event_context.actor_kind(),
            format!(
                "External identity '{}' synced in scope '{}'",
                profile.name, scope_name
            ),
        )?
        .with_context(&event_context)
        .with_entity_id(user.id)
        .with_entity_name(profile.name.clone())
        .with_metadata(serde_json::json!({
            "principal_id": user.id,
            "identity_scope": scope_name,
            "provider_kind": provider_kind,
            "external_subject": profile.subject,
            "synced_group_count": synced_group_count,
        }));
        emit_event(conn, &event)?;

        Ok(user)
    })
}

fn now() -> NaiveDateTime {
    chrono::Utc::now().naive_utc()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::LDAP_PROVIDER_KIND;
    use crate::models::group::NewGroup;
    use crate::schema::{group_memberships, groups as groups_table, identity_scopes, principals};
    use crate::tests::TestScope;
    use hubuum_auth_core::{ExternalGroup, ExternalUserProfile};

    fn external_user(
        subject: &str,
        name: &str,
        groups: Vec<ExternalGroup>,
    ) -> AuthenticatedExternalUser {
        AuthenticatedExternalUser {
            profile: ExternalUserProfile {
                subject: subject.to_string(),
                name: name.to_string(),
                proper_name: Some(format!("{name} Example")),
                email: Some(format!("{name}@example.org")),
            },
            groups,
        }
    }

    fn external_group(key: &str, name: &str) -> ExternalGroup {
        ExternalGroup {
            key: key.to_string(),
            name: name.to_string(),
            description: Some(format!("{name} directory group")),
        }
    }

    #[actix_rt::test]
    async fn sync_external_user_preserves_principal_when_source_name_changes() {
        let scope = TestScope::new();
        let identity_scope = scope.scoped_name("directory");
        let subject = format!(
            "uid={},ou=people,dc=example,dc=org",
            scope.scoped_name("stable_subject")
        );
        let initial_name = scope.scoped_name("external_alice");
        let renamed = scope.scoped_name("external_alice_renamed");

        let user = sync_external_user(
            &scope.pool,
            &identity_scope,
            LDAP_PROVIDER_KIND,
            external_user(&subject, &initial_name, Vec::new()),
        )
        .await
        .unwrap();
        let synced_after_rename = sync_external_user(
            &scope.pool,
            &identity_scope,
            LDAP_PROVIDER_KIND,
            external_user(&subject, &renamed, Vec::new()),
        )
        .await
        .unwrap();

        assert_eq!(synced_after_rename.id, user.id);
        assert_eq!(
            synced_after_rename.name(&scope.pool).await.unwrap(),
            renamed
        );

        let principal_count = with_connection(scope.pool.get_ref(), |conn| {
            principals::table
                .inner_join(identity_scopes::table)
                .filter(identity_scopes::name.eq(&identity_scope))
                .count()
                .get_result::<i64>(conn)
        })
        .unwrap();
        assert_eq!(principal_count, 1);
    }

    #[actix_rt::test]
    async fn sync_external_user_reconciles_external_memberships_and_keeps_manual_memberships() {
        let scope = TestScope::new();
        let identity_scope = scope.scoped_name("directory");
        let subject = format!(
            "uid={},ou=people,dc=example,dc=org",
            scope.scoped_name("membership_subject")
        );
        let username = scope.scoped_name("membership_user");
        let first_group_key = scope.scoped_name("external_alpha_key");
        let first_group_name = scope.scoped_name("external_alpha");
        let second_group_key = scope.scoped_name("external_beta_key");
        let second_group_name = scope.scoped_name("external_beta");

        let user = sync_external_user(
            &scope.pool,
            &identity_scope,
            LDAP_PROVIDER_KIND,
            external_user(
                &subject,
                &username,
                vec![external_group(&first_group_key, &first_group_name)],
            ),
        )
        .await
        .unwrap();
        let first_group_id = group_id_by_external_key(scope.pool.get_ref(), &first_group_key);

        let manual_group = NewGroup {
            identity_scope: None,
            groupname: scope.scoped_name("manual_group"),
            description: Some("Manual group".to_string()),
        }
        .save_without_events(&scope.pool)
        .await
        .unwrap();
        manual_group
            .add_member_without_events(&scope.pool, &user)
            .await
            .unwrap();

        let synced = sync_external_user(
            &scope.pool,
            &identity_scope,
            LDAP_PROVIDER_KIND,
            external_user(
                &subject,
                &username,
                vec![external_group(&second_group_key, &second_group_name)],
            ),
        )
        .await
        .unwrap();
        let second_group_id = group_id_by_external_key(scope.pool.get_ref(), &second_group_key);

        assert_eq!(synced.id, user.id);
        let memberships = with_connection(scope.pool.get_ref(), |conn| {
            group_memberships::table
                .filter(group_memberships::principal_id.eq(user.id))
                .select(group_memberships::group_id)
                .load::<i32>(conn)
        })
        .unwrap();
        assert!(memberships.contains(&manual_group.id));
        assert!(memberships.contains(&second_group_id));
        assert!(!memberships.contains(&first_group_id));
    }

    fn group_id_by_external_key(pool: &DbPool, external_key: &str) -> i32 {
        with_connection(pool, |conn| {
            groups_table::table
                .filter(groups_table::external_key.eq(external_key))
                .select(groups_table::id)
                .first::<i32>(conn)
        })
        .unwrap()
    }
}
