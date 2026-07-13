//! Live-Treetop parity tests.
//!
//! These tests exercise `TreetopPermissionBackend` against an actual
//! Treetop server. They are gated on the `HUBUUM_TREETOP_TEST_URL` env
//! var — when unset, every test exits with a "skipping" message rather
//! than failing.
//!
//! See `docs/treetop/test-fixture.md` for the Cedar entities, principals,
//! and policies the external Treetop instance must have for these tests
//! to assert correct behavior. The test setup uses fixed numeric IDs
//! (`TEST_USER_ID`, `TEST_ADMIN_GROUP_ID`, etc.) that the docs reference.

#![cfg(test)]

use std::env::var;

use actix_web::test as actix_test;

use crate::config::{PermissionBackendKind, get_config};
use crate::errors::ApiError;
use crate::models::Permissions;
use crate::permissions::backend::PermissionBackend;
use crate::permissions::treetop::TreetopPermissionBackend;
use crate::permissions::types::{PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef};
use crate::tests::get_test_pool;

/// Numeric IDs the external Treetop fixture is expected to recognize.
/// See docs/treetop/test-fixture.md.
const TEST_USER_ID: i32 = 9_001;
const TEST_USER_ID_SECOND: i32 = 9_002;
const TEST_ADMIN_GROUP_ID: i32 = 9_100;
const TEST_NORMAL_GROUP_ID: i32 = 9_101;
const TEST_NAMESPACE_ID: i32 = 9_201;

/// Read the URL or skip the test cleanly.
fn treetop_url() -> Option<String> {
    var("HUBUUM_TREETOP_TEST_URL").ok()
}

/// Build a TreetopPermissionBackend pointed at the live server. Uses the
/// shared test pool so the candidate-enumeration paths in the backend
/// (e.g. collections_user_can) have a real DB to query.
async fn live_backend(url: &str) -> Result<TreetopPermissionBackend, ApiError> {
    let pool = get_test_pool().get_ref().clone();
    // In test context, get_config() uses get_config_from_env() which reads
    // the environment. Clone it and override the treetop fields.
    let mut cfg = get_config().expect("failed to load test config").clone();
    cfg.treetop_url = Some(url.to_string());
    cfg.permission_backend = PermissionBackendKind::Treetop;
    TreetopPermissionBackend::connect(url, &cfg, pool).await
}

#[actix_test]
async fn live_health_check_succeeds() {
    let Some(url) = treetop_url() else {
        eprintln!("skipping live_health_check_succeeds: HUBUUM_TREETOP_TEST_URL not set");
        return;
    };
    let backend = live_backend(&url)
        .await
        .expect("connect + health check failed");
    // If we got here, the health check inside ::connect already succeeded.
    // Verify the backend reports the expected kind so we know it's the
    // real Treetop, not a mock substituted by mistake.
    assert_eq!(backend.kind(), "treetop");
}

#[actix_test]
async fn live_authorize_many_preserves_request_order() {
    let Some(url) = treetop_url() else {
        eprintln!(
            "skipping live_authorize_many_preserves_request_order: HUBUUM_TREETOP_TEST_URL not set"
        );
        return;
    };
    let backend = live_backend(&url).await.expect("connect failed");

    let principal = PrincipalRef::new(TEST_USER_ID, vec![TEST_NORMAL_GROUP_ID]);

    // Three requests where the external fixture grants ReadCollection on
    // TEST_NAMESPACE_ID to TEST_NORMAL_GROUP_ID, but nothing on a
    // never-granted-id (TEST_NAMESPACE_ID + 999), and no DeleteCollection
    // anywhere.
    let granted = PermissionRequest {
        resource: ResourceRef::collection(TEST_NAMESPACE_ID),
        permissions: vec![Permissions::ReadCollection],
    };
    let denied_by_resource = PermissionRequest {
        resource: ResourceRef::collection(TEST_NAMESPACE_ID + 999),
        permissions: vec![Permissions::ReadCollection],
    };
    let denied_by_action = PermissionRequest {
        resource: ResourceRef::collection(TEST_NAMESPACE_ID),
        permissions: vec![Permissions::DeleteCollection],
    };

    let decisions = backend
        .authorize_many(
            &principal,
            vec![granted, denied_by_resource, denied_by_action],
        )
        .await
        .expect("authorize_many failed");

    assert_eq!(
        decisions,
        vec![
            PermissionDecision::Allow,
            PermissionDecision::Deny,
            PermissionDecision::Deny,
        ],
        "Treetop must return decisions in input order"
    );
}

#[actix_test]
async fn live_is_admin_distinguishes_admin_from_normal() {
    let Some(url) = treetop_url() else {
        eprintln!(
            "skipping live_is_admin_distinguishes_admin_from_normal: HUBUUM_TREETOP_TEST_URL not set"
        );
        return;
    };
    let backend = live_backend(&url).await.expect("connect failed");

    let admin_principal = PrincipalRef::new(TEST_USER_ID, vec![TEST_ADMIN_GROUP_ID]);
    let normal_principal = PrincipalRef::new(TEST_USER_ID_SECOND, vec![TEST_NORMAL_GROUP_ID]);

    assert!(
        backend
            .is_admin(&admin_principal)
            .await
            .expect("is_admin admin failed"),
        "principal in TEST_ADMIN_GROUP_ID should be admin per the test fixture"
    );
    assert!(
        !backend
            .is_admin(&normal_principal)
            .await
            .expect("is_admin normal failed"),
        "principal in TEST_NORMAL_GROUP_ID should NOT be admin per the test fixture"
    );
}

#[actix_test]
async fn live_collections_user_can_reflects_external_policy() {
    let Some(url) = treetop_url() else {
        eprintln!(
            "skipping live_collections_user_can_reflects_external_policy: HUBUUM_TREETOP_TEST_URL not set"
        );
        return;
    };
    let backend = live_backend(&url).await.expect("connect failed");

    // Seed a local collection with TEST_NAMESPACE_ID via the test pool so
    // the candidate enumeration has at least one row to filter. If the id
    // is already taken, this is fine — the assertion below is membership-
    // based, not exact-equality.
    seed_collection_if_missing(TEST_NAMESPACE_ID).await;

    let principal = PrincipalRef::new(TEST_USER_ID, vec![TEST_NORMAL_GROUP_ID]);
    let visible = backend
        .collections_user_can(&principal, &[Permissions::ReadCollection])
        .await
        .expect("collections_user_can failed");

    assert!(
        visible.iter().any(|ns| ns.id == TEST_NAMESPACE_ID),
        "TEST_NAMESPACE_ID should appear when TEST_NORMAL_GROUP_ID has ReadCollection per the fixture"
    );
}

#[actix_test]
async fn live_group_permission_on_returns_grant_grid_for_known_group() {
    let Some(url) = treetop_url() else {
        eprintln!(
            "skipping live_group_permission_on_returns_grant_grid_for_known_group: HUBUUM_TREETOP_TEST_URL not set"
        );
        return;
    };
    let backend = live_backend(&url).await.expect("connect failed");
    seed_collection_if_missing(TEST_NAMESPACE_ID).await;

    let perm = backend
        .group_permission_on(TEST_NAMESPACE_ID, TEST_NORMAL_GROUP_ID)
        .await
        .expect("group_permission_on failed");

    let perm =
        perm.expect("the fixture grants at least ReadCollection — None means policy mismatch");
    assert!(
        perm.has_read_collection,
        "ReadCollection grant should appear on the synthesized row"
    );
    assert_eq!(perm.collection_id, TEST_NAMESPACE_ID);
    assert_eq!(perm.group_id, TEST_NORMAL_GROUP_ID);
}

/// Seed a collection with a specific id, idempotently. Tests use a known
/// collection id so the external Cedar policy can reference it. If a
/// collection already exists with that id, this is a no-op.
async fn seed_collection_if_missing(collection_id: i32) {
    use crate::db::prelude::*;
    use crate::db::with_connection;
    use crate::schema::collections::dsl::{collections, id};
    use crate::schema::collections::{description, name};
    use diesel::dsl::exists;
    use diesel::result::Error as DieselError;
    use diesel::{insert_into, select};

    let pool = get_test_pool();
    let exists: bool = with_connection(&pool, async |conn| {
        select(exists(collections.filter(id.eq(collection_id))))
            .get_result(conn)
            .await
    })
    .await
    .expect("collections existence check failed");

    if !exists {
        // Insert with an explicit id. collections.id is a SERIAL but we
        // can override it via diesel insert. The test owns this id range
        // (9201+) so collisions with autogenerated ids should be rare.
        with_connection(&pool, async |conn| -> Result<usize, DieselError> {
            insert_into(collections)
                .values((
                    id.eq(collection_id),
                    name.eq(format!("treetop_parity_ns_{collection_id}")),
                    description.eq("treetop parity test fixture"),
                ))
                .execute(conn)
                .await
        })
        .await
        .expect("collection insert failed");
    }
}
