use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::sync::Arc;

use rstest::rstest;

use super::super::tests::Fixture;
use super::super::{CheckpointRepository, Head, digest};
use super::AuthenticatedPayloadCache;

fn cached_repository(
    fixture: &Fixture,
    cache: &Arc<AuthenticatedPayloadCache>,
) -> CheckpointRepository {
    CheckpointRepository::open(
        &fixture.repository.root,
        &fixture.repository.root.parent().unwrap().join("key"),
    )
    .unwrap()
    .with_payload_cache(Arc::clone(cache))
}

#[test]
fn reopened_sessions_reuse_only_the_exact_authenticated_payload() {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    let first = cached_repository(&fixture, &cache).into_session().unwrap();
    let decoded = Arc::clone(&first.checkpoint().payload);
    let identity = first.checkpoint().identity().to_owned();
    drop(first);

    let second = cached_repository(&fixture, &cache).into_session().unwrap();
    assert!(Arc::ptr_eq(&decoded, &second.checkpoint().payload));
    assert_eq!(second.checkpoint().identity(), identity);
}

#[test]
fn another_publisher_advances_a_warmed_cache_and_releases_its_old_entry() {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    let before = cached_repository(&fixture, &cache).latest().unwrap();
    let old_payload = Arc::downgrade(&before.payload);
    drop(before);
    assert!(old_payload.upgrade().is_some());

    let next = fixture.next();
    fixture.repository.publish(&next, false).unwrap();
    let after = cached_repository(&fixture, &cache).latest().unwrap();
    assert_eq!(after.state(), next.state());
    assert!(old_payload.upgrade().is_none());
}

enum FileChange {
    Missing,
    CorruptSameLength,
    Truncated,
    Symlink,
    PublicPermissions,
}

#[rstest]
#[case::missing(FileChange::Missing)]
#[case::corrupt_same_length(FileChange::CorruptSameLength)]
#[case::truncated(FileChange::Truncated)]
#[case::symlink(FileChange::Symlink)]
#[case::public_permissions(FileChange::PublicPermissions)]
fn warmed_cache_still_checks_the_independent_snapshot_file(#[case] change: FileChange) {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    let before = cached_repository(&fixture, &cache).latest().unwrap();
    let material = fixture
        .repository
        .root
        .join(format!("{}.json", before.identity()));
    match change {
        FileChange::Missing => fs::remove_file(&material).unwrap(),
        FileChange::CorruptSameLength => {
            let mut bytes = fs::read(&material).unwrap();
            bytes[0] ^= 1;
            fs::write(&material, bytes).unwrap();
        }
        FileChange::Truncated => {
            let bytes = fs::read(&material).unwrap();
            fs::write(&material, &bytes[..bytes.len() / 2]).unwrap();
        }
        FileChange::Symlink => {
            let original = fixture.repository.root.join("original-snapshot");
            fs::rename(&material, &original).unwrap();
            symlink(original, &material).unwrap();
        }
        FileChange::PublicPermissions => {
            fs::set_permissions(&material, fs::Permissions::from_mode(0o644)).unwrap();
        }
    }
    assert!(cached_repository(&fixture, &cache).latest().is_err());
}

#[rstest]
#[case::missing(false)]
#[case::unauthenticated(true)]
fn warmed_cache_still_requires_fresh_authenticated_head(#[case] corrupt: bool) {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    cached_repository(&fixture, &cache).latest().unwrap();
    let head = fixture.repository.root.join("HEAD");
    if corrupt {
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&head).unwrap()).unwrap();
        value["payload"]["revision"] = serde_json::json!(99);
        fs::write(&head, serde_json::to_vec(&value).unwrap()).unwrap();
    } else {
        fs::remove_file(head).unwrap();
    }
    assert!(cached_repository(&fixture, &cache).latest().is_err());
}

#[test]
fn authenticated_head_must_still_correspond_to_the_cached_payload() {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    cached_repository(&fixture, &cache).latest().unwrap();
    let path = fixture.repository.root.join("HEAD");
    let mut head: Head = fixture
        .repository
        .verified(&fs::read(&path).unwrap())
        .unwrap();
    head.revision += 1;
    fs::write(&path, fixture.repository.signed(head).unwrap()).unwrap();
    assert!(cached_repository(&fixture, &cache).latest().is_err());
}

#[test]
fn reused_payload_never_reuses_the_previous_checkpoint_reference() {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    fixture.repository.publish(&fixture.next(), false).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    let before = cached_repository(&fixture, &cache).latest().unwrap();
    assert!(before.previous_sha256.is_some());
    let path = fixture.repository.root.join("HEAD");
    let mut head: Head = fixture
        .repository
        .verified(&fs::read(&path).unwrap())
        .unwrap();
    head.previous_sha256 = None;
    fs::write(&path, fixture.repository.signed(head).unwrap()).unwrap();

    let after = cached_repository(&fixture, &cache).latest().unwrap();
    assert!(Arc::ptr_eq(&before.payload, &after.payload));
    assert!(after.previous_sha256.is_none());
}

#[test]
fn a_new_key_cannot_authenticate_a_cached_payload_through_a_resigned_head() {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    cached_repository(&fixture, &cache).latest().unwrap();
    let path = fixture.repository.root.join("HEAD");
    let head: Head = fixture
        .repository
        .verified(&fs::read(&path).unwrap())
        .unwrap();
    fs::write(
        fixture.repository.root.parent().unwrap().join("key"),
        [43; 32],
    )
    .unwrap();
    let changed_key = cached_repository(&fixture, &cache);
    // Fresh HEAD authentication and the signed-file digest both succeed. Only
    // the payload's old signing key differs, so an unpinned cache would bypass it.
    fs::write(&path, changed_key.signed(head).unwrap()).unwrap();
    assert!(changed_key.latest().is_err());
}

#[test]
fn a_replaced_snapshot_with_a_new_digest_requires_full_authentication() {
    let fixture = Fixture::new();
    fixture.repository.publish(&fixture.initial, true).unwrap();
    let cache = Arc::new(AuthenticatedPayloadCache::default());
    let before = cached_repository(&fixture, &cache).latest().unwrap();
    let head_path = fixture.repository.root.join("HEAD");
    let mut head: Head = fixture
        .repository
        .verified(&fs::read(&head_path).unwrap())
        .unwrap();
    let original_path = fixture
        .repository
        .root
        .join(format!("{}.json", before.identity()));
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(original_path).unwrap()).unwrap();
    value["payload"]["state"]["revision"] = serde_json::json!(99);
    let changed = serde_json::to_vec(&value).unwrap();
    head.sha256 = digest(&changed);
    fixture
        .repository
        .write_new(
            &fixture
                .repository
                .root
                .join(format!("{}.json", head.sha256)),
            &changed,
        )
        .unwrap();
    fs::write(&head_path, fixture.repository.signed(head).unwrap()).unwrap();
    assert!(cached_repository(&fixture, &cache).latest().is_err());
}
