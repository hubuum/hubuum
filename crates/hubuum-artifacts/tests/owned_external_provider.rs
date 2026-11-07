//! Outside-crate receipt control. This adapter wraps genuine filesystem
//! admission, not DTO metadata. It does not qualify a DB/memory provider.
use hubuum_artifacts::{ArtifactCreationIntent, OwnedArtifact, OwnedArtifactReceipt};

// Intentionally neither Clone nor Serde. Only the provider can wrap a real
// native receipt; there is no constructor taking identity/intent metadata.
struct ExternalReceipt {
    native: OwnedArtifact,
}
impl OwnedArtifactReceipt for ExternalReceipt {
    fn intent(&self) -> &ArtifactCreationIntent {
        self.native.intent()
    }
    fn same_binding(&self, other: &Self) -> bool {
        self.native.same_binding(&other.native)
    }
}
#[test]
fn an_external_private_receipt_requires_no_clone_or_serde_contract() {
    fn contract<R: OwnedArtifactReceipt>() {}
    contract::<ExternalReceipt>();
}

#[cfg(unix)]
mod filesystem {
    use super::*;
    use async_trait::async_trait;
    use hubuum_artifacts::{
        ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReader, ArtifactReference,
        ArtifactResult, ArtifactStore, ArtifactWriter, Destination, DestinationId,
        FilesystemArtifactStore, ManifestBuilder, OwnedArtifactAdmission, OwnedArtifactPhase,
        OwnedArtifactResolution, OwnedArtifactResult, OwnedArtifactSession, OwnedArtifactStore,
        VerifiedArtifact, verify,
    };
    use rstest::rstest;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    struct ExternalStore {
        native: FilesystemArtifactStore,
    }
    #[async_trait]
    impl ArtifactStore for ExternalStore {
        async fn register_destination(&self, destination: &Destination) -> ArtifactResult<()> {
            self.native.register_destination(destination).await
        }
        async fn stage(
            &self,
            identity: &ArtifactIdentity,
            limits: ArtifactLimits,
        ) -> ArtifactResult<()> {
            self.native.stage(identity, limits).await
        }
        async fn append(
            &self,
            identity: &ArtifactIdentity,
            ordinal: u32,
            bytes: &[u8],
        ) -> ArtifactResult<()> {
            self.native.append(identity, ordinal, bytes).await
        }
        async fn seal(&self, manifest: &ArtifactManifest) -> ArtifactResult<()> {
            self.native.seal(manifest).await
        }
        async fn manifest(&self, identity: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
            self.native.manifest(identity).await
        }
        async fn published_manifest(
            &self,
            reference: &ArtifactReference,
        ) -> ArtifactResult<Vec<u8>> {
            self.native.published_manifest(reference).await
        }
        async fn block(
            &self,
            identity: &ArtifactIdentity,
            ordinal: u32,
        ) -> ArtifactResult<Option<Vec<u8>>> {
            self.native.block(identity, ordinal).await
        }
        async fn publish(&self, verified: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
            self.native.publish(verified).await
        }
        async fn abort(&self, identity: &ArtifactIdentity) -> ArtifactResult<()> {
            self.native.abort(identity).await
        }
        async fn cleanup(&self) -> ArtifactResult<u32> {
            self.native.cleanup().await
        }
    }
    #[async_trait]
    impl OwnedArtifactStore for ExternalStore {
        type Receipt = ExternalReceipt;
        async fn create_owned(
            &self,
            intent: &ArtifactCreationIntent,
        ) -> OwnedArtifactResult<OwnedArtifactAdmission<Self::Receipt>> {
            // This boundary can only wrap proof returned by real atomic native
            // admission. It never constructs proof from a success-shaped DTO.
            Ok(match self.native.create_owned(intent).await? {
                OwnedArtifactAdmission::Created(native) => {
                    OwnedArtifactAdmission::Created(ExternalReceipt { native })
                }
                OwnedArtifactAdmission::ResumedSameOwner {
                    artifact: native,
                    phase,
                } => OwnedArtifactAdmission::ResumedSameOwner {
                    artifact: ExternalReceipt { native },
                    phase,
                },
            })
        }
        async fn resolve_owned(
            &self,
            intent: &ArtifactCreationIntent,
        ) -> OwnedArtifactResult<OwnedArtifactResolution<Self::Receipt>> {
            Ok(match self.native.resolve_owned(intent).await? {
                OwnedArtifactResolution::Pending => OwnedArtifactResolution::Pending,
                OwnedArtifactResolution::Established {
                    artifact: native,
                    phase,
                } => OwnedArtifactResolution::Established {
                    artifact: ExternalReceipt { native },
                    phase,
                },
            })
        }
        async fn append_owned(
            &self,
            receipt: &Self::Receipt,
            ordinal: u32,
            bytes: &[u8],
        ) -> OwnedArtifactResult<()> {
            self.native
                .append_owned(&receipt.native, ordinal, bytes)
                .await
        }
        async fn seal_owned(
            &self,
            receipt: &Self::Receipt,
            manifest: &ArtifactManifest,
        ) -> OwnedArtifactResult<()> {
            self.native.seal_owned(&receipt.native, manifest).await
        }
        async fn publish_owned(
            &self,
            receipt: &Self::Receipt,
            verified: &VerifiedArtifact,
        ) -> OwnedArtifactResult<ArtifactReference> {
            self.native.publish_owned(&receipt.native, verified).await
        }
        async fn abort_owned(&self, receipt: &Self::Receipt) -> OwnedArtifactResult<()> {
            self.native.abort_owned(&receipt.native).await
        }
    }
    fn root() -> TempDir {
        tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap()
    }
    fn destination() -> Destination {
        Destination::new(DestinationId::generate(), "external-owned").unwrap()
    }
    async fn store(root: &TempDir, destination: &Destination) -> ExternalStore {
        let native = FilesystemArtifactStore::open(root.path().canonicalize().unwrap())
            .await
            .unwrap();
        native.register_destination(destination).await.unwrap();
        ExternalStore { native }
    }
    fn intent(destination: &Destination) -> ArtifactCreationIntent {
        ArtifactCreationIntent::new(
            ArtifactIdentity::new(destination.id()),
            limits(),
            [1; 16],
            [2; 16],
            [3; 32],
        )
        .unwrap()
    }
    fn limits() -> ArtifactLimits {
        ArtifactLimits::builder()
            .block_bytes(4)
            .artifact_bytes(8)
            .blocks(2)
            .build()
            .unwrap()
    }
    fn receipt(admission: OwnedArtifactAdmission<ExternalReceipt>) -> ExternalReceipt {
        match admission {
            OwnedArtifactAdmission::Created(receipt)
            | OwnedArtifactAdmission::ResumedSameOwner {
                artifact: receipt, ..
            } => receipt,
        }
    }
    fn manifest(intent: &ArtifactCreationIntent) -> ArtifactManifest {
        let mut builder = ManifestBuilder::new(intent.identity().clone(), intent.limits());
        builder.push(b"abcd").unwrap();
        builder.finish().unwrap()
    }

    #[tokio::test]
    async fn external_private_receipt_drives_the_existing_verified_writer() {
        let root = root();
        let destination = destination();
        let store = store(&root, &destination).await;
        let intent = intent(&destination);
        let receipt = receipt(store.create_owned(&intent).await.unwrap());
        let session = OwnedArtifactSession::new(&store, &receipt);
        let mut writer = ArtifactWriter::new(&session, intent.identity().clone(), intent.limits())
            .await
            .unwrap();
        writer.write(b"abcd").await.unwrap();
        let reference = writer.finish().await.unwrap();
        let mut reader = ArtifactReader::open(&store, &reference).await.unwrap();
        assert_eq!(reader.next_block().await.unwrap().unwrap(), b"abcd");
        assert!(session.cleanup().await.is_err());
    }
    #[tokio::test]
    async fn same_binding_ignores_progress_but_retains_the_actual_issuer() {
        let root = root();
        let destination = destination();
        let store = store(&root, &destination).await;
        let intent = intent(&destination);
        let receipt = receipt(store.create_owned(&intent).await.unwrap());
        store.append_owned(&receipt, 0, b"abcd").await.unwrap();
        store
            .seal_owned(&receipt, &manifest(&intent))
            .await
            .unwrap();
        let OwnedArtifactResolution::Established {
            artifact: recovered,
            phase: OwnedArtifactPhase::Sealed,
        } = store.resolve_owned(&intent).await.unwrap()
        else {
            panic!("expected actual sealed admission");
        };
        assert!(receipt.same_binding(&recovered));
        // same_binding is stable, but a writer still cannot resume sealed output.
        assert!(
            OwnedArtifactSession::new(&store, &receipt)
                .stage(intent.identity(), intent.limits())
                .await
                .is_err()
        );
    }
    #[derive(Clone, Copy)]
    enum Mutation {
        Stage,
        Append,
        Seal,
        Publish,
        Abort,
    }
    #[rstest]
    #[case(Mutation::Stage, false)]
    #[case(Mutation::Append, false)]
    #[case(Mutation::Seal, false)]
    #[case(Mutation::Publish, false)]
    #[case(Mutation::Abort, false)]
    #[case(Mutation::Stage, true)]
    #[case(Mutation::Append, true)]
    #[case(Mutation::Seal, true)]
    #[case(Mutation::Publish, true)]
    #[case(Mutation::Abort, true)]
    #[tokio::test]
    async fn external_receipt_cannot_mutate_legacy_or_foreign_store_binding(
        #[case] mutation: Mutation,
        #[case] target_owned: bool,
    ) {
        let first_root = root();
        let second_root = root();
        let destination = destination();
        let first = store(&first_root, &destination).await;
        let second = store(&second_root, &destination).await;
        let intent = intent(&destination);
        let foreign = receipt(first.create_owned(&intent).await.unwrap());
        let target = if target_owned {
            Some(receipt(second.create_owned(&intent).await.unwrap()))
        } else {
            second
                .stage(intent.identity(), intent.limits())
                .await
                .unwrap();
            None
        };
        let manifest = manifest(&intent);
        if matches!(mutation, Mutation::Seal | Mutation::Publish) {
            if let Some(target) = &target {
                second.append_owned(target, 0, b"abcd").await.unwrap();
            } else {
                second.append(intent.identity(), 0, b"abcd").await.unwrap();
            }
        }
        if matches!(mutation, Mutation::Publish) {
            if let Some(target) = &target {
                second.seal_owned(target, &manifest).await.unwrap();
            } else {
                second.seal(&manifest).await.unwrap();
            }
        }
        let result = match mutation {
            Mutation::Stage => {
                OwnedArtifactSession::new(&second, &foreign)
                    .stage(intent.identity(), intent.limits())
                    .await
            }
            Mutation::Append => second
                .append_owned(&foreign, 0, b"abcd")
                .await
                .map_err(Into::into),
            Mutation::Seal => second
                .seal_owned(&foreign, &manifest)
                .await
                .map_err(Into::into),
            Mutation::Publish => second
                .publish_owned(&foreign, &verify(&second, &manifest).await.unwrap())
                .await
                .map(|_| ())
                .map_err(Into::into),
            Mutation::Abort => second.abort_owned(&foreign).await.map_err(Into::into),
        };
        assert!(result.is_err());
        // The failed foreign mutation did not abort or modify the target. Its
        // real owner (or original legacy caller) can still complete normally.
        if let Some(target) = &target {
            if !matches!(mutation, Mutation::Seal | Mutation::Publish) {
                second.append_owned(target, 0, b"abcd").await.unwrap();
            }
            second.seal_owned(target, &manifest).await.unwrap();
            second
                .publish_owned(target, &verify(&second, &manifest).await.unwrap())
                .await
                .unwrap();
        } else {
            if !matches!(mutation, Mutation::Seal | Mutation::Publish) {
                second.append(intent.identity(), 0, b"abcd").await.unwrap();
            }
            second.seal(&manifest).await.unwrap();
            second
                .publish(&verify(&second, &manifest).await.unwrap())
                .await
                .unwrap();
        }
    }
}
