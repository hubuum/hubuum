# Contributing

## Authentication project delivery

Project [#6](https://github.com/orgs/hubuum/projects/6) uses the dedicated
`project/auth-support` integration branch in this repository. Target authentication
implementation and supporting project-only PRs there; unrelated work targets `main`.
Keep PRs draft during implementation and review. Merge `main` into the integration
branch to synchronize it; never rebase shared history. Promotion requires the
explicitly reviewed milestone integration PR described by the project.

Before marking an implementation issue Ready, starting implementation, or merging:

- Link the implementing issue and inspect its native **blocked by** relationships,
  including blockers in other repositories.
- Require every strict blocker to be closed with reason **completed**, and inspect
  its acceptance evidence. A **not planned** closure does not deliver a dependency.
- Pin accepted contracts and candidate artifacts by commit, workflow run, artifact
  ID and digest (container images by OCI digest). Record downstream acceptance;
  successful CI or issue closure alone does not establish contract acceptance.
- Require the selected correctness, integration and affected container checks to
  pass. Mark the PR ready for review for final validation before merging.

There is **no automated native-blocker merge check** configured for this project.
These prerequisite checks remain a required reviewer/merge gate. Native GitHub
relationships display ordering but do not enforce merges. Planning, contract review
and independent fixture preparation may proceed before implementation readiness.

Project CI retains immutable Actions candidate artifacts, with source/tested commit
identities and payload checksums. Use artifact IDs and digests, not names alone.
Candidates are accepted only after the complete relevant CI run passes and a reviewer
records acceptance. Download required artifacts before retention expires; retain the
IDs, digests and evidence in the implementing issue. Expired candidates must be
rebuilt and requalified. No project push publishes stable packages, `main` images or
the public production documentation site. This setup gate is
[server #572](https://github.com/hubuum/hubuum/issues/572), independent of runtime
configuration #568.
