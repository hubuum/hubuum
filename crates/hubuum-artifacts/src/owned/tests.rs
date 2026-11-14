use super::*;

#[test]
fn owned_error_conversion_preserves_the_original_typed_source() {
    use std::error::Error;
    use std::fmt;
    #[derive(Debug)]
    struct Cause(u8);
    impl fmt::Display for Cause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("private owned error evidence")
        }
    }
    impl Error for Cause {}
    let error = OwnedArtifactError::Unresolved(ArtifactError::Source(Box::new(Cause(7))));
    let primary = error
        .source()
        .unwrap()
        .downcast_ref::<ArtifactError>()
        .unwrap();
    assert_eq!(
        primary.source().unwrap().downcast_ref::<Cause>().unwrap().0,
        7
    );
    assert!(!format!("{error:?}").contains("private owned error evidence"));
    let ArtifactError::Source(source) = ArtifactError::from(error) else {
        panic!("owned conversion must retain the source");
    };
    assert_eq!(source.downcast::<Cause>().unwrap().0, 7);
}
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::{ArtifactId, ArtifactWriter, DestinationId, ManifestBuilder, verify};
use rstest::rstest;

// Complete byte-store ownership model, deliberately separate from FaultStore.
// A cloned model is a reopened durable state for deterministic fault tests.
#[derive(Clone, Default)]
struct Model {
    state: Arc<Mutex<ModelState>>,
}
struct ModelState {
    scope: u64,
    destinations: BTreeMap<DestinationId, String>,
    attempts: BTreeMap<ArtifactId, Attempt>,
    lose_reply: Option<Mutation>,
}
impl Default for ModelState {
    fn default() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self {
            scope: NEXT.fetch_add(1, Ordering::Relaxed),
            destinations: BTreeMap::new(),
            attempts: BTreeMap::new(),
            lose_reply: None,
        }
    }
}
struct Attempt {
    identity: ArtifactIdentity,
    limits: ArtifactLimits,
    owner: Option<ArtifactCreationIntent>,
    blocks: Vec<Vec<u8>>,
    manifest: Option<ArtifactManifest>,
    phase: OwnedArtifactPhase,
}
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
enum Mutation {
    Create,
    Append,
    Seal,
    Publish,
    Abort,
}
impl ModelState {
    fn committed(&mut self, operation: Mutation) -> ArtifactResult<()> {
        if self.lose_reply == Some(operation) {
            self.lose_reply = None;
            return Err(ArtifactError::Backend("committed, response lost".into()));
        }
        Ok(())
    }
    fn attempt(&self, identity: &ArtifactIdentity) -> ArtifactResult<&Attempt> {
        let attempt = self
            .attempts
            .get(&identity.artifact())
            .ok_or(ArtifactError::NotFound)?;
        if &attempt.identity != identity {
            return Err(owned_conflict());
        }
        Ok(attempt)
    }
    fn attempt_mut(&mut self, identity: &ArtifactIdentity) -> ArtifactResult<&mut Attempt> {
        self.attempt(identity)?;
        Ok(self
            .attempts
            .get_mut(&identity.artifact())
            .expect("checked above"))
    }
    fn stage(
        &mut self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
        owner: Option<&ArtifactCreationIntent>,
    ) -> ArtifactResult<bool> {
        if !self.destinations.contains_key(&identity.destination()) {
            return Err(ArtifactError::NotFound);
        }
        if let Ok(attempt) = self.attempt(identity) {
            if attempt.owner.as_ref() != owner || attempt.limits != limits {
                return Err(owned_conflict());
            }
            if owner.is_none() && attempt.phase != OwnedArtifactPhase::Staging {
                return Err(owned_conflict());
            }
            return Ok(false);
        }
        if self.attempts.contains_key(&identity.artifact())
            || self.attempts.values().any(|a| {
                a.identity.attempt() == identity.attempt()
                    || a.identity.version() == identity.version()
            })
        {
            return Err(owned_conflict());
        }
        self.attempts.insert(
            identity.artifact(),
            Attempt {
                identity: identity.clone(),
                limits,
                owner: owner.cloned(),
                blocks: Vec::new(),
                manifest: None,
                phase: OwnedArtifactPhase::Staging,
            },
        );
        self.committed(Mutation::Create)?;
        Ok(true)
    }
    fn mutate(
        &mut self,
        identity: &ArtifactIdentity,
        artifact: Option<&OwnedArtifact>,
        operation: Mutation,
        ordinal: u32,
        bytes: &[u8],
        manifest: Option<&ArtifactManifest>,
    ) -> ArtifactResult<Option<ArtifactReference>> {
        let scope = StoreScope::Test(self.scope);
        let attempt = self.attempt_mut(identity)?;
        authorize(attempt.owner.as_ref(), &scope, artifact)?;
        let mut reference = None;
        match operation {
            Mutation::Create => unreachable!(),
            Mutation::Append => {
                if attempt.phase != OwnedArtifactPhase::Staging {
                    return Err(owned_conflict());
                }
                if let Some(old) = attempt.blocks.get(ordinal as usize) {
                    if old != bytes {
                        return Err(owned_conflict());
                    }
                } else {
                    let size: usize = attempt.blocks.iter().map(Vec::len).sum();
                    if ordinal as usize != attempt.blocks.len()
                        || ordinal >= attempt.limits.blocks()
                        || bytes.is_empty()
                        || bytes.len() > attempt.limits.block_bytes() as usize
                        || size as u64 + bytes.len() as u64 > attempt.limits.artifact_bytes()
                        || attempt
                            .blocks
                            .last()
                            .is_some_and(|b| b.len() != attempt.limits.block_bytes() as usize)
                        || 117 + (ordinal + 1) * 36 > attempt.limits.manifest_bytes()
                    {
                        return Err(owned_conflict());
                    }
                    attempt.blocks.push(bytes.to_vec());
                }
            }
            Mutation::Seal => {
                let manifest = manifest.expect("seal manifest");
                if matches!(
                    attempt.phase,
                    OwnedArtifactPhase::Sealed | OwnedArtifactPhase::Published(_)
                ) {
                    if attempt.manifest.as_ref() != Some(manifest) {
                        return Err(owned_conflict());
                    }
                } else {
                    if attempt.phase != OwnedArtifactPhase::Staging {
                        return Err(owned_conflict());
                    }
                    ArtifactManifest::decode(&manifest.encode(), identity, attempt.limits)?;
                    if manifest.block_bytes() != attempt.limits.block_bytes()
                        || manifest.block_count() as usize != attempt.blocks.len()
                        || manifest.byte_size()
                            != attempt.blocks.iter().map(|b| b.len() as u64).sum::<u64>()
                    {
                        return Err(owned_conflict());
                    }
                    attempt.manifest = Some(manifest.clone());
                    attempt.phase = OwnedArtifactPhase::Sealed;
                }
            }
            Mutation::Publish => {
                let manifest = manifest.expect("verified manifest");
                if !matches!(
                    attempt.phase,
                    OwnedArtifactPhase::Sealed | OwnedArtifactPhase::Published(_)
                ) || attempt.manifest.as_ref() != Some(manifest)
                {
                    return Err(owned_conflict());
                }
                let published =
                    ArtifactReference::from_catalog(identity.clone(), manifest.manifest_digest());
                attempt.phase = OwnedArtifactPhase::Published(published.clone());
                reference = Some(published);
            }
            Mutation::Abort => {
                if matches!(attempt.phase, OwnedArtifactPhase::Published(_)) {
                    return Err(owned_conflict());
                }
                attempt.phase = OwnedArtifactPhase::Fenced;
            }
        }
        self.committed(operation)?;
        Ok(reference)
    }
}
#[async_trait]
impl ArtifactStore for Model {
    async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
        let mut state = self.state.lock().unwrap();
        for (id, name) in &state.destinations {
            if *id == destination.id() || name == destination.name() {
                return if *id == destination.id() && name == destination.name() {
                    Ok(())
                } else {
                    Err(owned_conflict())
                };
            }
        }
        state
            .destinations
            .insert(destination.id(), destination.name().into());
        Ok(())
    }
    async fn stage(
        &self,
        identity: &ArtifactIdentity,
        limits: ArtifactLimits,
    ) -> ArtifactResult<()> {
        self.state
            .lock()
            .unwrap()
            .stage(identity, limits, None)
            .map(|_| ())
    }
    async fn append(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        self.state
            .lock()
            .unwrap()
            .mutate(identity, None, Mutation::Append, ordinal, bytes, None)
            .map(|_| ())
    }
    async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
        self.state
            .lock()
            .unwrap()
            .mutate(
                manifest.identity(),
                None,
                Mutation::Seal,
                0,
                &[],
                Some(manifest),
            )
            .map(|_| ())
    }
    async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        let state = self.state.lock().unwrap();
        let attempt = state.attempt(identity)?;
        if !matches!(
            attempt.phase,
            OwnedArtifactPhase::Sealed | OwnedArtifactPhase::Published(_)
        ) {
            return Err(owned_conflict());
        }
        Ok(attempt
            .manifest
            .as_ref()
            .ok_or_else(owned_conflict)?
            .encode())
    }
    async fn published_manifest(&self, reference: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        let state = self.state.lock().unwrap();
        let attempt = state.attempt(reference.identity())?;
        if attempt.phase != OwnedArtifactPhase::Published(reference.clone()) {
            return Err(owned_conflict());
        }
        Ok(attempt
            .manifest
            .as_ref()
            .ok_or_else(owned_conflict)?
            .encode())
    }
    async fn block(
        &self,
        identity: &ArtifactIdentity,
        ordinal: u32,
    ) -> ArtifactResult<Option<Vec<u8>>> {
        let state = self.state.lock().unwrap();
        let attempt = state.attempt(identity)?;
        if !matches!(
            attempt.phase,
            OwnedArtifactPhase::Sealed | OwnedArtifactPhase::Published(_)
        ) {
            return Err(owned_conflict());
        }
        Ok(attempt.blocks.get(ordinal as usize).cloned())
    }
    async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .mutate(
                verified.manifest().identity(),
                None,
                Mutation::Publish,
                0,
                &[],
                Some(verified.manifest()),
            )?
            .expect("published reference"))
    }
    async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
        self.state
            .lock()
            .unwrap()
            .mutate(identity, None, Mutation::Abort, 0, &[], None)
            .map(|_| ())
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        let mut state = self.state.lock().unwrap();
        let mut removed = 0;
        for attempt in state
            .attempts
            .values_mut()
            .filter(|a| a.phase == OwnedArtifactPhase::Fenced)
        {
            while removed < 16 && !attempt.blocks.is_empty() {
                attempt.blocks.pop();
                removed += 1;
            }
        }
        Ok(removed)
    }
}
#[async_trait]
impl OwnedArtifactStore for Model {
    type Receipt = OwnedArtifact;
    async fn create_owned(
        &self,
        intent: &ArtifactCreationIntent,
    ) -> OwnedArtifactResult<OwnedArtifactAdmission> {
        let mut state = self.state.lock().unwrap();
        let created = state.stage(intent.identity(), intent.limits(), Some(intent))?;
        let phase = state.attempt(intent.identity())?.phase.clone();
        let artifact = OwnedArtifact::checked(intent.clone(), StoreScope::Test(state.scope));
        Ok(if created {
            OwnedArtifactAdmission::Created(artifact)
        } else {
            OwnedArtifactAdmission::ResumedSameOwner { artifact, phase }
        })
    }
    async fn resolve_owned(
        &self,
        intent: &ArtifactCreationIntent,
    ) -> OwnedArtifactResult<OwnedArtifactResolution> {
        let state = self.state.lock().unwrap();
        let attempt = match state.attempt(intent.identity()) {
            Ok(value) => value,
            Err(ArtifactError::NotFound) => return Ok(OwnedArtifactResolution::Pending),
            Err(error) => return Err(error.into()),
        };
        if attempt.owner.as_ref() != Some(intent) {
            return Err(owned_conflict().into());
        }
        Ok(OwnedArtifactResolution::Established {
            artifact: OwnedArtifact::checked(intent.clone(), StoreScope::Test(state.scope)),
            phase: attempt.phase.clone(),
        })
    }
    async fn append_owned(
        &self,
        artifact: &OwnedArtifact,
        ordinal: u32,
        bytes: &[u8],
    ) -> OwnedArtifactResult<()> {
        self.state
            .lock()
            .unwrap()
            .mutate(
                artifact.intent.identity(),
                Some(artifact),
                Mutation::Append,
                ordinal,
                bytes,
                None,
            )
            .map(|_| ())
            .map_err(Into::into)
    }
    async fn seal_owned(
        &self,
        artifact: &OwnedArtifact,
        manifest: &ArtifactManifest,
    ) -> OwnedArtifactResult<()> {
        self.state
            .lock()
            .unwrap()
            .mutate(
                manifest.identity(),
                Some(artifact),
                Mutation::Seal,
                0,
                &[],
                Some(manifest),
            )
            .map(|_| ())
            .map_err(Into::into)
    }
    async fn publish_owned(
        &self,
        artifact: &OwnedArtifact,
        verified: &VerifiedArtifact,
    ) -> OwnedArtifactResult<ArtifactReference> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .mutate(
                verified.manifest().identity(),
                Some(artifact),
                Mutation::Publish,
                0,
                &[],
                Some(verified.manifest()),
            )?
            .expect("published reference"))
    }
    async fn abort_owned(&self, artifact: &OwnedArtifact) -> OwnedArtifactResult<()> {
        self.state
            .lock()
            .unwrap()
            .mutate(
                artifact.intent.identity(),
                Some(artifact),
                Mutation::Abort,
                0,
                &[],
                None,
            )
            .map(|_| ())
            .map_err(Into::into)
    }
}
fn limits() -> ArtifactLimits {
    ArtifactLimits::builder()
        .block_bytes(4)
        .artifact_bytes(12)
        .blocks(3)
        .build()
        .unwrap()
}
fn intent(identity: ArtifactIdentity, owner: u8) -> ArtifactCreationIntent {
    ArtifactCreationIntent::new(identity, limits(), [owner; 16], [1; 16], [7; 32]).unwrap()
}
async fn fixture() -> (Model, ArtifactCreationIntent) {
    let store = Model::default();
    let destination = Destination::new(DestinationId::generate(), "owned-model").unwrap();
    store.register_destination(&destination).await.unwrap();
    (store, intent(ArtifactIdentity::new(destination.id()), 1))
}
fn receipt(admission: OwnedArtifactAdmission) -> OwnedArtifact {
    match admission {
        OwnedArtifactAdmission::Created(a)
        | OwnedArtifactAdmission::ResumedSameOwner { artifact: a, .. } => a,
    }
}
fn manifest(intent: &ArtifactCreationIntent) -> ArtifactManifest {
    let mut builder = ManifestBuilder::new(intent.identity().clone(), intent.limits());
    builder.push(b"abcd").unwrap();
    builder.finish().unwrap()
}

#[test]
fn intent_roundtrip_is_canonical_and_not_a_receipt() {
    let intent = intent(ArtifactIdentity::new(DestinationId::generate()), 1);
    let encoded = intent.encode().unwrap();
    assert!(encoded.len() < OWNED_INTENT_BYTES);
    assert_eq!(ArtifactCreationIntent::decode(&encoded).unwrap(), intent);
}
#[rstest]
#[case("protocol", serde_json::json!(2))]
#[case("operation", serde_json::json!(vec![0; 16]))]
#[case("incarnation", serde_json::json!(vec![0; 16]))]
#[case("unexpected", serde_json::json!(1))]
fn intent_rejects_invalid_fields(#[case] key: &str, #[case] value: serde_json::Value) {
    let intent = intent(ArtifactIdentity::new(DestinationId::generate()), 1);
    let mut wire = serde_json::to_value(intent).unwrap();
    wire[key] = value;
    assert!(serde_json::from_value::<ArtifactCreationIntent>(wire).is_err());
}
#[test]
fn noncanonical_and_oversized_intent_bytes_are_rejected() {
    let i = intent(ArtifactIdentity::new(DestinationId::generate()), 1);
    let mut bytes = i.encode().unwrap();
    bytes.push(b' ');
    assert!(ArtifactCreationIntent::decode(&bytes).is_err());
    assert!(ArtifactCreationIntent::decode(&vec![b' '; OWNED_INTENT_BYTES + 1]).is_err());
}
#[tokio::test]
async fn a_foreign_same_identity_producer_cannot_obtain_ownership() {
    let (store, original) = fixture().await;
    let a = receipt(store.create_owned(&original).await.unwrap());
    let other = intent(original.identity().clone(), 2);
    assert!(matches!(
        store.create_owned(&other).await,
        Err(OwnedArtifactError::Conflict)
    ));
    store.append_owned(&a, 0, b"abcd").await.unwrap();
}
#[rstest]
#[case(Mutation::Create)]
#[case(Mutation::Append)]
#[case(Mutation::Seal)]
#[case(Mutation::Publish)]
#[case(Mutation::Abort)]
#[tokio::test]
async fn committed_mutation_with_lost_reply_is_recoverable(#[case] lost: Mutation) {
    let (store, intent) = fixture().await;
    if lost == Mutation::Create {
        store.state.lock().unwrap().lose_reply = Some(lost);
        assert!(store.create_owned(&intent).await.is_err());
    }
    let a = receipt(store.clone().create_owned(&intent).await.unwrap());
    let m = manifest(&intent);
    if lost == Mutation::Append {
        store.state.lock().unwrap().lose_reply = Some(lost);
        assert!(store.append_owned(&a, 0, b"abcd").await.is_err());
    }
    store.append_owned(&a, 0, b"abcd").await.unwrap();
    if lost == Mutation::Seal {
        store.state.lock().unwrap().lose_reply = Some(lost);
        assert!(store.seal_owned(&a, &m).await.is_err());
    }
    store.seal_owned(&a, &m).await.unwrap();
    if lost == Mutation::Abort {
        store.state.lock().unwrap().lose_reply = Some(lost);
        assert!(store.abort_owned(&a).await.is_err());
        assert!(matches!(
            store.resolve_owned(&intent).await.unwrap(),
            OwnedArtifactResolution::Established {
                phase: OwnedArtifactPhase::Fenced,
                ..
            }
        ));
    } else {
        let verified = verify(&store, &m).await.unwrap();
        if lost == Mutation::Publish {
            store.state.lock().unwrap().lose_reply = Some(lost);
            assert!(store.publish_owned(&a, &verified).await.is_err());
        }
        let first = store.publish_owned(&a, &verified).await.unwrap();
        assert!(
            matches!(store.resolve_owned(&intent).await.unwrap(), OwnedArtifactResolution::Established { phase: OwnedArtifactPhase::Published(reference), .. } if reference == first)
        );
    }
}
#[tokio::test]
async fn missing_before_delayed_creation_is_still_pending() {
    let (store, intent) = fixture().await;
    assert!(matches!(
        store.resolve_owned(&intent).await.unwrap(),
        OwnedArtifactResolution::Pending
    ));
    // A delayed request still owns the right to try its conditional create.
    store.create_owned(&intent).await.unwrap();
    assert!(matches!(
        store.resolve_owned(&intent).await.unwrap(),
        OwnedArtifactResolution::Established { .. }
    ));
}
#[tokio::test]
async fn admitted_view_reuses_bounded_writer_and_never_legacy_mutators() {
    let (store, intent) = fixture().await;
    let a = receipt(store.create_owned(&intent).await.unwrap());
    let view = a.session(&store);
    let mut writer = ArtifactWriter::new(&view, intent.identity().clone(), limits())
        .await
        .unwrap();
    writer.write(b"abcd").await.unwrap();
    let reference = writer.finish().await.unwrap();
    assert_eq!(reference.identity(), intent.identity());
    assert!(view.cleanup().await.is_err());
}

#[rstest]
#[case(Mutation::Create)]
#[case(Mutation::Append)]
#[case(Mutation::Seal)]
#[case(Mutation::Publish)]
#[case(Mutation::Abort)]
#[tokio::test]
async fn legacy_mutators_cannot_bypass_owned_mode(#[case] operation: Mutation) {
    let (store, intent) = fixture().await;
    let a = receipt(store.create_owned(&intent).await.unwrap());
    let m = manifest(&intent);
    let result = match operation {
        Mutation::Create => store.stage(intent.identity(), limits()).await,
        Mutation::Append => store.append(intent.identity(), 0, b"abcd").await,
        Mutation::Seal => store.seal(&m).await,
        Mutation::Abort => store.abort(intent.identity()).await,
        Mutation::Publish => {
            store.append_owned(&a, 0, b"abcd").await.unwrap();
            store.seal_owned(&a, &m).await.unwrap();
            store
                .publish(&verify(&store, &m).await.unwrap())
                .await
                .map(|_| ())
        }
    };
    assert!(result.is_err());
}
#[tokio::test]
async fn legacy_staging_is_never_adopted() {
    let (store, intent) = fixture().await;
    store.stage(intent.identity(), limits()).await.unwrap();
    assert!(store.create_owned(&intent).await.is_err());
    store.append(intent.identity(), 0, b"abcd").await.unwrap();
}
#[tokio::test]
async fn abort_does_not_release_identity_ownership() {
    let (store, original) = fixture().await;
    let a = receipt(store.create_owned(&original).await.unwrap());
    store.abort_owned(&a).await.unwrap();
    store.cleanup().await.unwrap();
    assert!(
        store
            .create_owned(&intent(original.identity().clone(), 2))
            .await
            .is_err()
    );
}
