# Security Policy

## Supported releases

The latest stable Hubuum release is supported with security fixes. Older
release lines and moving `main` builds are unsupported unless a release notice
explicitly states otherwise. Operators should upgrade to the latest stable
release before requesting a backport.

## Private vulnerability reporting

Report suspected vulnerabilities through the repository's
[private vulnerability reporting form](https://github.com/hubuum/hubuum/security/advisories/new).
Do not open a public issue for an unpatched vulnerability, exposed credential,
or supply-chain compromise.

Include the affected version or commit, impact, reproduction details, and any
known mitigations. For release-integrity concerns, also include the artifact
name or container digest and the verification command that failed.

Maintainers aim to acknowledge a complete report within three business days.
Triage, remediation, release timing, and coordinated disclosure are handled
privately with the reporter. Please allow a reasonable remediation window
before public disclosure unless active exploitation requires a faster response.

## Release verification

Archive checksums and signatures, provenance attestations, SBOM retrieval,
container signatures, and final-image scan policy are documented in
[Release Supply-Chain Verification](docs/supply-chain-security.md).

The same private reporting channel should be used for compromised release
credentials, suspicious GitHub Actions behavior, incorrect SBOM or provenance
claims, and vulnerable build or publication dependencies.
