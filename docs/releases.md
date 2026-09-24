# Releases and compatibility

The documentation site opens the **latest published stable release**. Every
edition has its own search index and stable path:

| URL below `https://hubuum.github.io/hubuum/` | Content |
| --- | --- |
| `/` | Redirects to the latest published release's documentation |
| `/vX.Y.Z/` | An immutable snapshot of the documentation from that release tag |
| `/main/` | Development documentation; select it explicitly for unreleased changes |

Use the **Version** menu to switch editions. Switching opens the selected
edition's home page, since an older release may not have the page you are on.
Older snapshots remain available when a new release is published. A missing
version can be [published on demand](contributing/documentation.md#publish-an-older-release).

## Find documentation for your version

1. Identify the server version with `hubuum-server --version` or the running
   instance's OpenAPI `info.version`.
2. Choose the matching version in the menu or open its `/vX.Y.Z/` path directly.
3. Read that version's [release notes](https://github.com/hubuum/hubuum/releases)
   and changelog, available from its release tag.
4. Check the [CLI and client compatibility records](integrations/clients.md#choose-compatible-versions)
   before upgrading a companion application.

Releases that predate this website can still be published: their original
`docs/` content is rendered with the current website tooling and a generated
entry page. New tutorials and API behavior are not copied into older editions.
The source commit is recorded in each edition's `build.json` and the site's
`versions.json`. Published release snapshots are retained without rebuilding
them in place; corrections belong in the next release or development edition.

## Upgrade deliberately

Hubuum is under active development before `1.0`. Pin server and frontend images
or native archives to explicit releases, and install matching server,
administrator, and template-worker binaries.

Review breaking API, storage SDK, configuration, and backup-format changes in
the target release. Follow [deployment sequencing](distributed_deployment.md)
and [backup and restore](backup-restore.md) guidance. A client using only normal
object reads can have different upgrade needs from an administrator automating
credential management or restores.

Maintainers can find the publishing and compatibility gates in
[releasing Hubuum](releasing.md), [operational contracts](operational_contracts.md),
and the [Rust API boundary](rust_api_boundary.md).
