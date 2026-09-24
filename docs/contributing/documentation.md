# Maintain the documentation

The public documentation is built with [Zensical](https://zensical.org/) from
`docs/` in this repository. `zensical.toml` defines audience-based navigation,
the canonical URL, and theme settings. Existing document paths stay stable;
navigation labels can change without moving files.

## Preview and validate locally

Requirements: Python 3.11 or newer, Bash, and a running Docker engine.
The scripts use only the Python standard library. Zensical runs in its official
container, pinned by version and digest in `.github/docs-tools.env`; there is
no Python package installation or virtual environment to maintain here.

From the repository root:

```sh
bash scripts/docs.sh serve
```

Open `http://127.0.0.1:8000`. The preview rebuilds as you edit documentation.
Stop it with Ctrl-C. Restart it after changing `zensical.toml`, since the preview
uses a prepared edition-specific configuration. Run the same validation as the publishing build with:

```sh
python3 scripts/test-check-docs.py
bash scripts/docs.sh build
npx markdownlint-cli2 --config .markdownlint.json "**/*.md" "!target"
```

The build checks that every Markdown page has exactly one navigation entry,
uses Zensical's strict internal-link and anchor validation, and checks the
generated HTML links and assets against the GitHub Pages `/hubuum/` prefix.
The output is one edition in `target/docs-site/`; generated output and caches are ignored by
Git. A local preview shows that edition; the version menu is populated when the
edition is assembled with the published archive.

To build the original documentation from an older tag:

```sh
git fetch origin --tags
bash scripts/docs.sh build v0.0.16
```

The renderer stages the tagged `docs/` content, filters navigation to pages that
exist in that release, and adds any older pages to a release-reference section.
For tags without a home page, it generates a short release entry point. It never
copies current tutorials into an older release. Source links are pinned to the
tag's resolved commit. The ordinary `serve` command previews the working tree.

## Write for a reader's task

| Section | Reader's question | Content to place here |
| --- | --- | --- |
| Overview | Is Hubuum relevant to us? | Concepts, ecosystem, maturity, compatibility |
| Get started | How do I reach a first success? | Short, ordered tutorials with prerequisites and expected results |
| User guide | How do I work with my data? | Modeling, permissions, queries, data workflows |
| Administration | How do I run and recover it? | Deployment, configuration, identity, monitoring, runbooks |
| API & integrations | How do I connect another system? | HTTP contracts, client entry points, compatibility |
| Contributing | How do I change Hubuum correctly? | Development, architecture, verification, release policy |

Keep tutorials distinct from exhaustive references. Explain prerequisites,
required permissions, expected results, and failure recovery. Reuse a canonical
reference with a relative link instead of copying its configuration tables into
another guide. The same page can be linked from several audience landing pages
while having one canonical navigation entry.

Use descriptive link labels, one H1 per page, a language on every code fence,
and consistent Markdown table separators. Prefer normal Markdown; the home page
uses a small HTML wrapper for Zensical's accessible card layout. The stylesheet
adds only typography, color, and card treatment, and system fonts avoid a
third-party font request.

When linking within `docs/`, use relative `.md` paths. For code or assets outside
`docs/`, use an explicit GitHub `blob/main/` or `tree/main/` URL. Those files are
not part of the static site. Keep generated references generated: use the
[OpenAPI](../integrations/api.md), [inventory](../generated/project_inventory.md),
and [operational-contract](../operational_contracts.md) workflows.

## GitHub Pages publishing

The intended public URL is **<https://hubuum.github.io/hubuum/>**. The root opens
the latest published stable release, with immutable releases at `/vX.Y.Z/` and
an explicitly selected development edition at `/main/`. A version menu and banner
identify the edition on every page. Search is scoped to the selected edition.

The `Documentation` workflow runs on pull requests, pushes to `main`, published
releases, successful release-tag CI runs, and manual dispatch. The CI-completion
trigger covers releases created with `GITHUB_TOKEN`, which do not trigger another
release workflow. It accepts only successful tag pushes from this repository;
pull-request CI runs cannot enter the publishing path.
It uses the shared change classifier for ordinary
changes. Release events and manual runs always build. It always resolves the
**Documentation check** job, including when no build is needed, so that check can
be required by branch protection.

Pull requests build development and latest-release editions and retain a
`documentation-site` artifact for 14 days.
Download and extract it, then run `python3 -m http.server 8000` in its directory
to review it. PRs have read-only repository permission and cannot publish Pages.
After merge or a stable release publication, a separate serialized deployment
job combines validated editions with the retained archive on the `gh-pages`
branch, then uploads and deploys a Pages artifact. The archive is generated
output, stored under `site/` on that branch; source documentation remains on
`main` and release tags. Deployment needs `contents: write` for the archive,
`pages: write`, and `id-token: write`. No personal access token is needed.

Release directories are append-only. Rebuilding the same source commit retains
the existing snapshot; a moved tag with a different commit is rejected. Updating
`main` never modifies a release directory. The default is the highest published
stable version in the archive, so backfilling an older tag does not move it
backward. Prereleases do not publish or become the default.

### One-time repository setup

A repository administrator must select **Settings → Pages → Build and deployment
→ Source → GitHub Actions**. Configure the `github-pages` environment to accept
deployments from `main` and release tags matching `v*`. Add **Documentation check** to the repository's
required checks if documentation builds should block merging.

After merging the setup, run the `Documentation` workflow on `main` if necessary
to publish or retry the first deployment. The first publication includes the
latest released tag even when that tag predates the website. Build validation
works before Pages is enabled; publishing requires that repository setting.

### Publish an older release

Open **Actions → Documentation → Run workflow**, select the `main` branch, and
enter an existing stable release tag such as `v0.0.15` in **version**. The workflow
requires a published, non-draft, non-prerelease GitHub release for that tag.
It builds the original tagged documentation with the current pinned renderer,
retains all previously published versions, and adds the new version to the menu.
Leave the input empty to refresh development and ensure the latest release is
present. The selected root remains the latest release.

The archive lives independently of Actions artifact retention. Do not delete or
force-push `gh-pages`: it retains the published release snapshots. A failed Pages
deployment can be retried using a manual run; the archived snapshots remain
unchanged.

For a custom domain, configure it in GitHub Pages and update `site_url` in
`zensical.toml` to the actual canonical URL, then rebuild. The same static
`target/docs-site/` output can be uploaded to another static host. See
[GitHub's Pages workflow documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
for the hosting requirements.

## Connecting companion projects

The initial site is a curated hub. It links to the real documentation maintained
in the Rust client, Python client, CLI, and frontend repositories. It does not
fetch another repository's moving default branch during a documentation build.

For each companion project, maintain these entry points in
[the ecosystem page](../ecosystem.md) and [the interface guide](../integrations/clients.md):

1. Repository and stable documentation home.
2. Installation and first-use guide.
3. Command, UI, or language API reference.
4. Release notes and tested server compatibility, including evidence where available.
5. A backlink to the shared server concepts, API contracts, and operations guides.

A companion can publish its own Zensical site from its own repository, reusing
the section names and visual theme. Replace the links here once that site is
published. This keeps independent releases independent and avoids copying
examples into several repositories.

If unified cross-project search becomes a requirement, introduce a reviewed
manifest of pinned companion revisions and an explicit import step, preserving
source/edit links and licenses. Do not silently aggregate `main` branches or
claim compatibility based only on the fact that documentation builds together.

## Changing the site tooling

Update the Zensical version and multi-architecture digest together, then build
the site and check navigation, search, both color schemes, and narrow-screen
layout. GitHub Actions are pinned to immutable commit SHAs.

When adding build inputs, update `scripts/classify-ci-changes.sh` and its
regression tests so the documentation job runs for them. Do not suppress link
validation to make a renamed heading or moved page pass; update the references.
