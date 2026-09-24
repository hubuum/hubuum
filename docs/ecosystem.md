# The Hubuum ecosystem

The server owns the data model, permissions, and HTTP contracts. Companion
projects provide interfaces for people and applications. Choose the interface
that fits your workflow; all of them connect to a Hubuum server.

| Component | Use it for | Documentation home |
| --- | --- | --- |
| [Hubuum server](https://github.com/hubuum/hubuum) | Data storage, API, authorization, background work, and administration | This site |
| [Web frontend](https://github.com/hubuum/hubuum-frontend) | Interactive inventory and administration in a browser | [Frontend README](https://github.com/hubuum/hubuum-frontend#readme) |
| [CLI](https://github.com/hubuum/hubuum-cli) | Interactive terminal work, scripts, and shell automation | [CLI guide](https://github.com/hubuum/hubuum-cli#usage) |
| [Rust client](https://github.com/hubuum/hubuum-client-rust) | Typed asynchronous and blocking Rust applications | [Rust client guides](https://github.com/hubuum/hubuum-client-rust#more-documentation) |
| [Python client](https://github.com/hubuum/hubuum-client-python) | Typed synchronous and asynchronous Python applications | [Python client guides](https://github.com/hubuum/hubuum-client-python/blob/main/docs/index.md) |

The archived [`hubuum-python`](https://github.com/hubuum/hubuum-python) repository
is an earlier implementation. The current Python client is
**`hubuum-client-python`**.

## One entry point, clear ownership

The proposed organization landing page at `https://hubuum.github.io/` belongs in
the separate `hubuum/hubuum.github.io` repository. It introduces the ecosystem
and directs readers to each project. The `.github` repository supplies the GitHub
organization profile, which can link to that landing page. See the
[site ownership plan](contributing/documentation.md#organization-landing-page-and-project-sites).

This server site at `/hubuum/` owns shared concepts, server operation, API behavior, and cross-project
workflows. Companion repositories own their installation instructions,
language-specific examples, command/UI reference, and release history.

Start with [clients and interfaces](integrations/clients.md) for practical entry
points and compatibility links. Client and server version numbers do not match
automatically; consult each project's declared server targets.

Maintainers adding another client or interface should follow the
[ecosystem documentation contract](contributing/documentation.md#connecting-companion-projects).
It allows each project to publish its own detailed documentation while sharing
navigation and an organization-wide entry point.
