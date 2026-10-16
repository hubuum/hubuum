# Temporary Swagger UI build patch

This directory contains the published `utoipa-swagger-ui` 10.0.1 source and
its MIT and Apache-2.0 licenses. It is a third-party dependency, excluded from
the Hubuum workspace and patched through the root Cargo manifest.

All zip 8.x versions were yanked on 2026-10-16, leaving upstream's `zip = "8.6"`
build dependency without a supported release. The patch follows
[upstream PR #1645](https://github.com/juhaku/utoipa/pull/1645):

- Require zip 9.0.4, which preserves the dependency's Rust 1.88 minimum.
- Propagate the now-fallible `ZipFile::name()` result in `build.rs`.
- Rename the original README to `README.upstream.txt` and point the normalized
  package manifest at it; this file records the local patch provenance.

Runtime source and vendored Swagger UI assets are unchanged. The dependency
and license checks remain enabled without exceptions. See
[upstream issue #1644](https://github.com/juhaku/utoipa/issues/1644).

Remove this directory, the root patch/exclusion, and its Docker dependency-stage
copy when a published Swagger UI release includes the compatible zip update.
