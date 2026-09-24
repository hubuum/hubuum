# Documentation review and roadmap

This review establishes the first website from the existing reference library.
It covers navigation and local-link integrity across `docs/`, with focused
content checks of onboarding, configuration, authentication, and ecosystem
entry points. It is not a claim that every historical command example has been
executed against every supported deployment.

## Findings and changes

| Finding | Action in the initial site |
| --- | --- |
| The README was the main index for a large, mostly flat reference library. | Added six audience-oriented sections with guided landing pages and explicit navigation coverage for every document. |
| “Quick start” began with hundreds of lines of configuration reference. | Added an ordered first-server tutorial; relabeled the existing page as configuration reference while retaining its path. |
| The configuration page's Compose fragment had an undefined database service and no migration step. | Replaced it with links to the maintained complete deployment and development configurations. |
| A new user had no short explanation of collections, classes, objects, and tasks. | Added concepts, first-request examples, and user reading paths. |
| Some source links in task internals still referenced old test locations. | Repaired moved test links and made repository links usable from the published site. |
| The collection guide said token scopes only narrowed permission types. | Corrected it to include resource identity boundaries, consistent with the token model and permission guide. |
| Operations content was scattered among configuration, metrics, deployment, and recovery references. | Added an administrator entry point, symptom-based troubleshooting, and links to existing dashboards and runbooks. |
| The clients and frontend release independently; an older Python repository is archived. | Linked the active companion repositories and their compatibility records, distinguished the CLI from `hubuum-admin`, and identified the archived project. |
| A website could make development behavior look like a released contract. | Made the latest release the default, retained immutable `/vX.Y.Z/` snapshots, and made development an explicit version-menu choice. |
| New pages could become invisible as the library grows. | Added a navigation coverage check, strict source-link validation, rendered-link checks, and a required-check-compatible PR workflow. |

## Placement of the existing library

The navigation in `zensical.toml` is the maintained page inventory. A validator
compares it with every `docs/**/*.md` file, so this review does not introduce a
second list that can drift.

| Existing material | Canonical home |
| --- | --- |
| Collections, permissions, schemas, computed fields, history | User guide → Model & protect data |
| Queries, search, name addressing, relationships, patches, revisions | User guide → Find & change data |
| Imports, exports, templates, tasks, remote targets, events | User guide → Automate workflows |
| Deployment, configuration, database roles, recovery, artifact verification | Administration → Install & upgrade |
| Authentication, external identities, approvals, secrets, throttling, Treetop | Administration → Identity & security |
| Metrics, logs, tracing, limits, performance tuning, worker operation | Administration → Monitor & troubleshoot |
| Query support and integration evidence | API & integrations |
| Storage and task internals, SDK contracts, Rust policies, benchmarks, releases | Contributing |

The operator package remains in `observability/`, alongside its executable alert
fixtures and dashboards, and is linked from administration. `test-corpora/`
retains its dataset documentation. Root release notes and security policy remain
canonical repository documents. Generated JSON specifications, fixture files,
and measurements remain available as static assets or explicit repository links.

## Next editorial work

These are proposed follow-ups, not capabilities implemented by the initial site.

| Priority | Work | Completion criterion |
| --- | --- | --- |
| 1 | Add a complete inventory walkthrough spanning the frontend, CLI, and one client. | A tested scenario with permissions, sample data, expected results, and cleanup, owned jointly by the affected projects. |
| 1 | Review each operational example against a pinned released deployment. | Reproducible evidence for install, upgrade, backup, and restore examples; remove redundant snippets. |
| 2 | Separate long mixed-purpose references where it helps readers. | Keep stable links or redirects while distinguishing tutorials, how-to guides, reference, and design rationale. |
| 2 | Publish companion documentation sites. | Stable documentation homes with backlinks, shared concepts, and release compatibility records. |
| 3 | Consider unified ecosystem search. | Pinned cross-repository inputs, preserved ownership/edit links, reproducible builds, and an agreed compatibility policy. |

For each future PR, review audience placement, local links, version applicability,
and whether a behavior change needs a changelog entry. See the
[documentation workflow](documentation.md) for build and publishing instructions.
