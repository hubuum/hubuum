# Release Supply-Chain Verification

Hubuum stable releases publish verifiable native archives and multi-platform
container images. Release evidence is tied to the tagged source commit and is
created only after the tagged commit has passed the blocking `main` workflow.

## Dependency policy

`deny.toml` is the committed Rust dependency policy. It checks RustSec
advisories, yanked packages, licenses, trusted registries, exact git revisions,
and duplicate versions of selected security-critical TLS and cryptography
crates. The same policy runs for relevant pull requests, `main`, release tags,
and the daily scheduled supply-chain workflow.

Run the policy locally with:

```bash
bash scripts/run-cargo-deny.sh cargo-deny.log
```

Supply-chain exceptions are deny-by-default. An advisory, license, or container
vulnerability exception must be present in both the tool-specific policy and
`.github/supply-chain-exceptions.json`. Each metadata entry requires a reason,
owner, and ISO 8601 expiry date. CI rejects expired or unmatched entries.

An exception has this shape:

```json
{
  "kind": "container-vulnerability",
  "id": "CVE-YYYY-NNNN",
  "reason": "Scanner false positive confirmed against the shipped package",
  "owner": "@hubuum/security",
  "expires": "2026-09-01"
}
```

## Immutable build inputs

GitHub Actions are pinned to full commit SHAs with their readable release name
in a comment. CI service images and release tooling containers are pinned by
OCI digest. `.github/supply-chain-tools.env` records exact versions and image
digests for cargo-deny, cargo-semver-checks, Diesel CLI, Syft, Trivy, and
cosign. Consumers read individual validated values rather than evaluating the
manifest as shell code.

Diesel CLI, cargo-deny, and cargo-semver-checks are built from
checksum-verified crates.io sources at exact versions. The semantic checker is
installed only when a workspace package is deliberately classified as public.
Syft and Trivy run from digest-pinned OCI images. The pinned cosign installer
verifies the downloaded executable before use. Workflows record the executable
or platform-image digest and tool version in their run summary.

Validate these controls locally with:

```bash
python3 scripts/check-supply-chain-policy.py
python3 scripts/test-supply-chain-policy.py
bash scripts/test-generate-container-evidence.sh
bash scripts/test-generate-release-sbom.sh
```

## Native release evidence

Every stable native archive has a neighboring CycloneDX 1.6 SBOM named
`<archive>.cdx.json`. The SBOM identifies the archive digest, source revision,
release tag, build target, exact Cargo feature selection, Cargo lockfile digest,
workspace crates, and target-filtered resolved Rust dependencies.

`release-checksums.txt` is the authoritative SHA-256 manifest for all native
archives and their SBOMs. It is signed keylessly with Sigstore and published
with its `release-checksums.sigstore.json` verification bundle. GitHub also
publishes SLSA build provenance for every digest in that manifest. The release
includes the provenance bundle, detailed build-input predicate, and its signed
attestation bundle.

Download and verify a release without maintainer secrets:

```bash
version=v0.0.9
gh release download "$version" --repo hubuum/hubuum --dir "hubuum-$version"
cd "hubuum-$version"
sha256sum --check release-checksums.txt

cosign verify-blob \
  --bundle release-checksums.sigstore.json \
  --certificate-identity "https://github.com/hubuum/hubuum/.github/workflows/ci.yml@refs/tags/$version" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  release-checksums.txt

gh attestation verify \
  "hubuum-linux-x86_64-all-features-$version.tar.gz" \
  --repo hubuum/hubuum
```

Choose the archive matching the operator platform in the final command. Inspect
its neighboring `.cdx.json` file before deployment when dependency inventory is
part of the local approval process.

## Container evidence and signatures

Each stable platform image receives a CycloneDX SBOM containing both Syft's
final Alpine package inventory and Cargo's resolved Rust dependency graph. The
SBOM and SLSA provenance are attached as signed GitHub attestations to the
platform-image digest. The final multi-architecture manifest is separately
attested and signed keylessly with cosign. Versioned tags that resolve to the
same manifest share that immutable signature.

Verify the final manifest and its GitHub provenance:

```bash
version=v0.0.9
image=ghcr.io/hubuum/hubuum-server
digest="$(docker buildx imagetools inspect \
  --format '{{json .Manifest}}' "$image:$version" | jq --raw-output '.digest')"

cosign verify \
  --certificate-identity "https://github.com/hubuum/hubuum/.github/workflows/ci.yml@refs/tags/$version" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  "$image@$digest"

gh attestation verify \
  "oci://$image@$digest" \
  --repo hubuum/hubuum
```

Platform SBOM attestations can be retrieved and verified with the platform
digest shown by `docker buildx imagetools inspect --raw`:

```bash
gh attestation verify \
  "oci://ghcr.io/hubuum/hubuum-server@sha256:PLATFORM_DIGEST" \
  --repo hubuum/hubuum \
  --predicate-type https://cyclonedx.org/bom \
  --format json
```

## Final-image vulnerability policy

The digest actually pushed for each AMD64 and ARM64 release platform is scanned
with a digest-pinned Trivy image. Both operating-system and application-library
packages are included. A fixed HIGH or CRITICAL vulnerability blocks the final
multi-architecture release manifest. Unfixed findings are retained in the full
JSON report but do not block publication because no deployable remediation is
available.

Syft generates the container SBOM and Trivy independently scans both the final
image and that SBOM. The merged SBOM, raw Syft inventory, Trivy image report,
fixed-finding policy report, Trivy SBOM report, scan status, and signed
attestation bundles are retained as workflow artifacts for 90 days. Signed OCI
attestations and container signatures remain attached to the immutable release
digest.

A scanner false positive must not be suppressed inline. Add the exact advisory
identifier to `.trivyignore` and matching time-bounded metadata to
`.github/supply-chain-exceptions.json`; CI enforces the pairing and expiry.

Mutable `main` artifacts are development builds. Main container images receive
SBOMs and final-image scans, but their moving tags are not represented as
stable signed releases.
