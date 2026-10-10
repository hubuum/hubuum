# Maintain the documentation

The public documentation is built with [Zensical](https://zensical.org/) from
`docs/` in this repository. `zensical.toml` defines audience-based navigation,
the canonical URL, and theme settings. Existing document paths stay stable;
navigation labels can change without moving files.

## Preview and validate locally

Requirements: Python 3.11 or newer, Git, Bash, and a running Docker engine.
The scripts use only the Python standard library. `.github/docs-tools.env` pins
the shared tooling commit from [hubuum/.github](https://github.com/hubuum/.github).
That revision owns the digest-pinned official Zensical container, theme, checks,
and reusable workflows. There is no Python package installation or virtual
environment to maintain in this repository.

From the repository root:

```sh
bash scripts/docs.sh serve
```

Open `http://127.0.0.1:8000`. Stop the preview with Ctrl-C and rebuild after editing
source files or configuration. Run the same validation as the publishing build with:

```sh
bash scripts/docs.sh check
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

Keep tutorials short and ordered, with prerequisites and expected results. Put
configuration tables in references, and implementation/test details under
Contributing. Use [Configuration reference](../quick_start.md) as the label for
that existing path. Follow the shared
[content policy](https://github.com/hubuum/.github/blob/main/docs-tooling/README.md#content-policy)
for terminology, cross-project links, examples, and stable anchors.

When linking within `docs/`, use relative `.md` paths. The operator package and
runbooks in `observability/` are imported by `tool.hubuum_docs.source_files` in
`zensical.toml`; each imported page also needs one navigation entry. Use explicit
GitHub URLs for other repository assets. Keep OpenAPI, inventory, and operational
references generated from their canonical sources.

## Shared example dataset

Use the [Atlas example](../getting-started/example-dataset.md) for tutorials,
client walkthroughs, screenshots, and product diagrams. Introduce the class
before the object: Service/Atlas, Server/web-01, Location/Oslo, and
Context/Research notes are the central examples. Keep class names and object
names case-sensitive. Numeric IDs and revisions are runtime values, not fixture
identities.

The canonical recipe is `docs/assets/atlas/atlas.import.json`. The neighboring
backup is produced by the real server, never assembled by hand. Its manifest
records checksums, sizes, counts, and the producing server version. Both files
are published within each documentation edition, so release snapshots keep
their original dataset. Client repositories should link to a matching server
edition and reuse its files rather than maintaining independent seed data.

When showing complete object data in Markdown, put an `atlas-data` comment
immediately before the JSON fence, for example `<!-- atlas-data: object:Atlas -->`.
The offline check compares marked examples with the canonical import. Mark
partial payloads and tutorial additions explicitly; do not label them as the
unchanged baseline. Specialized contract examples may use placeholders or
additional resources where Atlas does not cover the behavior.

Run from the repository root with Python 3.11+ and the production image built
as described in [development](../development.md):

```sh
python3 tests/python/run.py unit tooling.test_atlas
python3 tests/python/run.py integration atlas check
python3 tests/python/run.py integration atlas verify --image hubuum-server:verify
```

After changing the import, regenerate the backup and manifest:

```sh
python3 tests/python/run.py integration atlas generate --image hubuum-server:verify
```

The generator and verifier reuse `tests/python/integration/corpus.py`'s isolated Docker
harness. They create their own network, databases, and temporary credentials;
they do not accept an existing database URL. Generation publishes files only
after import, application scenarios, and two restore rounds succeed. No
third-party Python packages are required. For a regeneration check that leaves
committed files untouched, add `--directory target/generated-atlas-corpus`.

CI checks metadata and documentation drift, imports and restores the committed
corpus against the candidate production server, and regenerates into a temporary
output directory. Changes to the dataset, its guide, or its tooling select the
required checks. The larger [functional corpus](https://github.com/hubuum/hubuum/blob/main/test-corpora/README.md)
continues to cover scale and specialized edge cases.

## GitHub Pages publishing

The server publishes at <https://hubuum.github.io/hubuum/>. The root selects the
latest stable release; `/vX.Y.Z/` retains that release and `/main/` describes
development. Follow the shared
[publishing and public-release verification policy](https://github.com/hubuum/.github/blob/main/docs-tooling/README.md#ci-and-publishing).

### One-time repository setup

Use the shared [Pages setup script](https://github.com/hubuum/.github/blob/main/scripts/setup-pages.sh).
The source is GitHub Actions, the deployment environment accepts `main` and
release tags, and **Documentation check** is the required validation job.

### Publish an older release

Run **Actions → Documentation → Run workflow** on `main` with the published
stable tag in **version**. See the shared policy for immutable snapshots,
backfills, and deployment recovery. Verify the public site after publication.

## Connecting companion projects

### Organization landing page and project sites

The [ecosystem site](https://hubuum.github.io/) comes from `hubuum/hubuum.github.io`.
This repository publishes the server site; companions publish their own sites.
The GitHub organization profile in `.github` is separate from the Pages home.
See the shared [repository ownership table](https://github.com/hubuum/.github/blob/main/docs-tooling/README.md#repository-ownership).

### Content ownership and shared navigation

The server owns concepts, HTTP contracts, data fixtures, and operations.
Companions own their installation, examples, API/command/UI references, and
compatibility evidence. Link directly to the relevant guide in a matching
released edition; never infer compatibility from equal version numbers.

## Changing the site tooling

Follow the shared [validation and adoption procedure](https://github.com/hubuum/.github/blob/main/docs-tooling/README.md#validate-and-update).
Keep `.github/docs-tools.env` and both reusable-workflow pins at the same SHA.
When adding build inputs, check `scripts/classify-ci-changes.sh` and its tests.
