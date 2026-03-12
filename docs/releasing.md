# Releasing Hubuum

This repository uses the CI workflow in
[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) for both validation and publishing.

## What the workflows enforce

- `Cargo.toml` package version must match the release tag.
- `CHANGELOG.md` must contain a section for the release version.
- `docs/openapi.json` must be regenerated for the release version.
- A version bump in `Cargo.toml` must come with matching changelog and OpenAPI updates.

## Scripted release flow

Use the helper script in [`scripts/release.sh`](../scripts/release.sh):

1. Start from a clean local `main`.
2. Run `./scripts/release.sh prepare 0.0.2`.
3. Review the generated release branch `release/v0.0.2`, polish `CHANGELOG.md` if needed, and commit it.
4. Open and merge that release branch.
5. Check out the merged `main` and run `./scripts/release.sh tag`.
6. Push `main` and the new tag.

The helper script:

- creates the `release/vX.Y.Z` branch from `main`
- updates `Cargo.toml`
- rolls the current `Unreleased` changelog notes into the new release section
- regenerates `docs/openapi.json`
- runs the existing release validation scripts before you commit or tag

Once the tag is pushed, the CI workflow will:

- verify the tagged commit already passed CI on `main`
- publish GitHub release archives for Linux x86_64 and aarch64
- publish multi-arch GHCR images for the release tag

## Container images

The CI workflow publishes two container image variants:

- Default tags like `ghcr.io/hubuum/hubuum-server:v0.0.1` and `:main` are the full image.
  This image includes both `rustls` and `openssl`, and it can also run plain HTTP when no TLS
  certificate and key are configured.
- Tags ending in `-rustls-only` are the slimmer image that only includes the `rustls` backend.

The full image also gets explicit aliases ending in `-full`.

Publishing from `main` happens in the same workflow run and depends directly on the CI jobs passing.
