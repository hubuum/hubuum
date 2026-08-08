# Releasing Hubuum

This repository uses the CI workflow in
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) for both validation and publishing.

## What the workflows enforce

- `Cargo.toml` package version must match the release tag.
- `CHANGELOG.md` must contain a section for the release version.
- `docs/openapi.json` must be regenerated for the release version.
- A version bump in `Cargo.toml` must come with matching changelog and OpenAPI updates.
- The candidate OpenAPI contract must have no unaccepted breaks from the
  immediately preceding stable release.
- The candidate must pass the adjacent stable release upgrade and application
  rollback harness against an immutable release-image digest.
- Every Rust package must retain an explicit support classification; internal
  packages cannot be published, and any supported package must pass rustdoc,
  clean packaging, and semantic compatibility checks.

## Scripted release flow

### First release (`v0.0.1`)

The repository is already prepared at version `0.0.1`. Once the release changes are on
`main` and that commit has a successful CI run:

1. Check out the release commit on a clean local `main` branch.
2. Run `./scripts/check-release-readiness.sh v0.0.1`.
3. Run `./scripts/release.sh tag`.
4. Push the tag with `git push origin v0.0.1`.

Do not tag a different commit while CI is still running: the tag workflow requires the
exact tagged commit to have a successful `main` CI run.

### Later releases

Use the helper script in [`scripts/release.sh`](../scripts/release.sh):

1. Start from a clean local `main`.
2. Run `./scripts/release.sh prepare 0.0.2`.
3. Review the generated release branch `release/v0.0.2`, including the full
   `Cargo.lock` dependency refresh, polish `CHANGELOG.md` if needed, and commit it.
4. Open and merge that release branch.
5. Check out the merged `main` and run `./scripts/release.sh tag`.
6. Push `main` and the new tag.

The helper script:

- creates the `release/vX.Y.Z` branch from `main`
- updates `Cargo.toml`
- updates all Cargo dependencies to the newest versions allowed by the workspace
  manifests
- rolls the current `Unreleased` changelog notes into the new release section
- regenerates `docs/openapi.json`
- runs the existing release validation scripts before you commit or tag

Once the tag is pushed, the CI workflow will:

- verify the tagged commit already passed CI on `main`
- regenerate OpenAPI and compare it with the immediately preceding stable tag
- resolve the latest stable release, migrate its representative data under live
  API probes, exercise a mixed-version interval, and restore its application
  image against the migrated database before publishing
- verify that the tag, `Cargo.toml`, changelog, and OpenAPI versions match
- validate Rust API classifications and any supported crate compatibility
- publish GitHub release archives and SHA-256 checksums for Linux x86_64, Linux ARM64,
  Windows x86_64, and macOS ARM64
- use the matching changelog section as the GitHub Release notes
- publish AMD64 and ARM64 GHCR images for the release tag

## Rust package compatibility

The root server package and all current workspace crates are internal and set
`publish = false`. They are shipped as source inputs to the Hubuum binaries, not
as supported crates. The authoritative classification and promotion process is
documented in [Rust API Boundary](rust_api_boundary.md).

CI rejects missing classifications and any internal package that enables Cargo
publishing. A future `experimental-public` or `stable-public` package is
automatically checked with the pinned `cargo-semver-checks` version, rustdoc
warnings denied, all features, and Cargo's clean packaged-source build. Promote
a package only in a dedicated change containing its API policy, release owner,
versioning rules, and downstream migration or compatibility fixtures.

For an initial public release, CI records the absence of a crates.io baseline
and skips only semantic comparison; rustdoc and clean packaging remain
mandatory. Once the first release exists, the semantic compatibility check is
mandatory, and registry lookup errors fail rather than bypass the check.

## OpenAPI compatibility gate

The `OpenAPI contract` job treats two independent failures as release
blockers. The generated document must exactly match `docs/openapi.json`, and
the generated candidate must be compatible with the latest stable release. A
tag build excludes its own tag while resolving the baseline, so it compares
with the immediately preceding stable release. If the repository has no stable
release yet, the structured report records an explicit skipped baseline rather
than silently choosing another source.

The job installs checksum-pinned `oasdiff`, records its version and binary
digest, and publishes one `openapi-contract` artifact containing:

- the generated OpenAPI document and exact drift diff;
- the baseline document, tag, source URL, and SHA-256 digest;
- raw structural and classified changes;
- `compatibility.json`; and
- `summary.md`, grouped into additive, behavioral, and breaking changes.

Breaking findings fail the job unless each fingerprint is listed in
`.github/openapi-breaking-exceptions.json`. An exception must name the exact
baseline, have a unique stable identifier and future expiry date, explain the
decision, provide client migration guidance, and point to matching text in the
`[Unreleased]` changelog.
Exceptions are never wildcards: every fingerprint must still be present, and
unused fingerprints fail while their baseline is current. An exception for an
older baseline becomes inactive automatically when a new stable release is
published.

During normal development the policy validates `[Unreleased]`. After
`release.sh prepare` moves those notes into the new version section, release-PR
and tag checks also validate that exact candidate-version section. This keeps
the documented approval attached to the release without weakening ordinary PR
checks or searching unrelated historical notes.

For an intentional pre-1.0 break:

1. Add a clearly marked breaking entry and migration steps to `[Unreleased]`.
2. Run the compatibility check to obtain the exact finding fingerprints.
3. Add the smallest coherent exception group with those fingerprints, the
   current stable baseline, rationale, migration text, changelog marker, and a
   review expiry.
4. Review the uploaded Markdown and JSON reports before merging.

Run the policy fixtures locally with:

```bash
oasdiff_bin="$(scripts/install-oasdiff.sh /tmp/hubuum-oasdiff)"
OASDIFF_BIN="$oasdiff_bin" scripts/test-openapi-compatibility.sh
```

## Certified Upgrade And Rollback Window

CI certifies exactly one adjacent application transition: the latest stable
release (`N-1`) to the candidate (`N`). It resolves `N-1` through the GitHub
release API, pulls the release image, records its immutable digest, and tests it
with the candidate against one PostgreSQL database. The report records the
candidate SHA, migration set and duration, maximum observed API latency and
outage, the terminal test phase, and failure logs.

The certified sequence drains old workers, keeps the old API under ordinary
read probes while candidate migrations run, starts both API versions for
cross-version reads and writes, completes the candidate rollout, and then
restarts the `N-1` API against the migrated schema. That last step is an
application rollback only. Hubuum does not automatically downgrade the
database, and releases older than `N-1` are outside this compatibility promise.

Backup, restore, and import document formats may change at a release boundary.
Quiesce those operations while versions overlap and use the document format
accepted by the application version that will process it. A successful API
rollback does not make a newer backup or import document readable by `N-1`.

## Native archives

Linux AMD64 and ARM64 archives are exported from the same Alpine builder used by the production
container. Both executables are stripped, statically linked musl binaries. CI rejects the archive
if either binary declares a dynamic runtime dependency, so users do not need system copies of
glibc, libpq, or OpenSSL.

macOS and Windows use their native Rust targets. Their builds enable embedded migrations, bundled
libpq, and vendored OpenSSL, so users do not need Homebrew, PostgreSQL client libraries, or OpenSSL
packages. They retain the standard operating-system libraries expected by native executables.

Both tagged releases and `main-latest` use these platform contracts. Every archive includes
`hubuum-server`, `hubuum-admin`, and the embedded migration runner exposed through
`hubuum-admin --migrate`.

## Container images

The CI workflow publishes one Alpine-based container image with both the `rustls` and OpenSSL TLS
backends:

- Default tags like `ghcr.io/hubuum/hubuum-server:v0.0.1` and `:main` are the full image.
  It can also run plain HTTP when no TLS certificate and key are configured.

The full image also gets explicit aliases ending in `-full`.

The image runs pending embedded Diesel migrations during startup unless
`HUBUUM_SKIP_MIGRATIONS` is enabled. The image does not need the standalone Diesel CLI or `psql`;
operators can run migrations explicitly with `hubuum-admin --migrate`.

Publishing from `main` happens in the same workflow run and depends directly on the CI jobs passing.
Documentation-only and repository-metadata pushes do not rebuild or replace
`main-latest` archives or container images. The existing artifacts remain valid
because their binary inputs are unchanged. Changes to Rust sources, embedded
documentation, migrations, manifests, container inputs, or the publication
workflow still run the complete validation and publishing path.
