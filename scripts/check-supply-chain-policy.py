#!/usr/bin/env python3
"""Validate immutable workflow inputs and exception governance."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = ROOT / ".github" / "workflows"
SHA_REF = re.compile(r"^[0-9a-f]{40}$")
DIGEST_REF = re.compile(r"@sha256:[0-9a-f]{64}$")
MUTABLE_INSTALLER = re.compile(
    r"(?i)(?:curl|wget)[^\n]*\|\s*(?:ba)?sh|irm[^\n]*\|\s*iex|/latest/download/"
)


def fail(message: str) -> None:
    print(f"supply-chain policy error: {message}", file=sys.stderr)
    raise SystemExit(1)


def workflow_files() -> list[Path]:
    return sorted((*WORKFLOW_DIR.glob("*.yml"), *WORKFLOW_DIR.glob("*.yaml")))


def check_workflows() -> None:
    for workflow in workflow_files():
        text = workflow.read_text(encoding="utf-8")
        if MUTABLE_INSTALLER.search(text):
            fail(f"{workflow.relative_to(ROOT)} executes a mutable installer")

        for line_number, line in enumerate(text.splitlines(), start=1):
            uses_match = re.search(r"\buses:\s*([^\s#]+)(?:\s+#\s*(\S+))?", line)
            if uses_match:
                value, version_comment = uses_match.groups()
                if value.startswith("./"):
                    continue
                if "@" not in value:
                    fail(f"{workflow.relative_to(ROOT)}:{line_number} has no action ref")
                ref = value.rsplit("@", 1)[1]
                if not SHA_REF.fullmatch(ref):
                    fail(
                        f"{workflow.relative_to(ROOT)}:{line_number} action is not pinned "
                        "to a full commit SHA"
                    )
                if not version_comment:
                    fail(
                        f"{workflow.relative_to(ROOT)}:{line_number} action pin has no "
                        "human-readable version comment"
                    )

            image_match = re.match(r"\s+image:\s*([^\s#]+)", line)
            if image_match and not DIGEST_REF.search(image_match.group(1)):
                fail(
                    f"{workflow.relative_to(ROOT)}:{line_number} service image is not "
                    "pinned by SHA-256 digest"
                )


def walk_tables(value: object):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_tables(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_tables(child)


def check_git_dependencies() -> None:
    for manifest in sorted(ROOT.glob("**/Cargo.toml")):
        if "target" in manifest.parts:
            continue
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        for table in walk_tables(data):
            git = table.get("git")
            if not isinstance(git, str):
                continue
            revision = table.get("rev")
            if not isinstance(revision, str) or not SHA_REF.fullmatch(revision):
                fail(
                    f"{manifest.relative_to(ROOT)} git dependency {git} lacks an "
                    "exact 40-character revision"
                )


def configured_exceptions() -> set[tuple[str, str]]:
    policy = tomllib.loads((ROOT / "deny.toml").read_text(encoding="utf-8"))
    configured: set[tuple[str, str]] = set()

    for entry in policy.get("advisories", {}).get("ignore", []):
        identifier = entry if isinstance(entry, str) else entry.get("id") or entry.get("crate")
        if identifier:
            configured.add(("advisory", identifier))

    for entry in policy.get("licenses", {}).get("exceptions", []):
        identifier = entry.get("crate") or entry.get("name")
        if identifier:
            configured.add(("license", identifier))

    trivy_ignore = ROOT / ".trivyignore"
    for line in trivy_ignore.read_text(encoding="utf-8").splitlines():
        identifier = line.strip()
        if identifier and not identifier.startswith("#"):
            configured.add(("container-vulnerability", identifier))

    return configured


def check_exceptions() -> None:
    path = ROOT / ".github" / "supply-chain-exceptions.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != 1 or not isinstance(data.get("exceptions"), list):
        fail(f"{path.relative_to(ROOT)} must use schema_version 1 and an exceptions array")

    today = dt.date.today()
    documented: set[tuple[str, str]] = set()
    for index, exception in enumerate(data["exceptions"]):
        if not isinstance(exception, dict):
            fail(f"exception {index} must be an object")
        missing = [
            field
            for field in ("kind", "id", "reason", "owner", "expires")
            if not isinstance(exception.get(field), str) or not exception[field].strip()
        ]
        if missing:
            fail(f"exception {index} is missing: {', '.join(missing)}")
        if exception["kind"] not in {
            "advisory",
            "container-vulnerability",
            "license",
        }:
            fail(f"exception {index} has unsupported kind {exception['kind']!r}")
        try:
            expires = dt.date.fromisoformat(exception["expires"])
        except ValueError:
            fail(f"exception {index} expires must be an ISO date")
        if expires < today:
            fail(f"exception {exception['id']} expired on {expires}")
        documented.add((exception["kind"], exception["id"]))

    configured = configured_exceptions()
    if configured != documented:
        fail(
            "deny.toml exceptions and .github/supply-chain-exceptions.json differ: "
            f"configured={sorted(configured)!r}, documented={sorted(documented)!r}"
        )


def check_tool_manifest() -> None:
    values: dict[str, str] = {}
    path = ROOT / ".github" / "supply-chain-tools.env"
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or not value:
            fail(f"invalid tool manifest line: {line!r}")
        values[key] = value

    required = {
        "CARGO_DENY_VERSION",
        "DIESEL_CLI_VERSION",
        "SYFT_IMAGE",
        "TRIVY_IMAGE",
        "COSIGN_VERSION",
    }
    if values.keys() != required:
        fail(f"tool manifest keys must be exactly {sorted(required)!r}")
    for key in ("SYFT_IMAGE", "TRIVY_IMAGE"):
        if not DIGEST_REF.search(values[key]):
            fail(f"{key} must include a full SHA-256 OCI digest")
    for key in ("CARGO_DENY_VERSION", "DIESEL_CLI_VERSION"):
        if not re.fullmatch(r"\d+\.\d+\.\d+", values[key]):
            fail(f"{key} must be an exact semantic version")
    if not re.fullmatch(r"v\d+\.\d+\.\d+", values["COSIGN_VERSION"]):
        fail("COSIGN_VERSION must be an exact v-prefixed semantic version")
    ci_workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
    cosign_input = f"cosign-release: {values['COSIGN_VERSION']}"
    if cosign_input not in ci_workflow:
        fail("ci.yml cosign-release does not match the pinned tool manifest")


def write_workflow_summary() -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    action_pins: set[str] = set()
    service_images: set[str] = set()
    for workflow in workflow_files():
        for line in workflow.read_text(encoding="utf-8").splitlines():
            uses_match = re.search(r"\buses:\s*([^\s#]+)(?:\s+#\s*(\S+))?", line)
            if uses_match and not uses_match.group(1).startswith("./"):
                action_pins.add(
                    f"{uses_match.group(1)} ({uses_match.group(2) or 'unlabeled'})"
                )
            image_match = re.match(r"\s+image:\s*([^\s#]+)", line)
            if image_match:
                service_images.add(image_match.group(1))

    tools = []
    for line in (ROOT / ".github" / "supply-chain-tools.env").read_text(
        encoding="utf-8"
    ).splitlines():
        if line and not line.startswith("#"):
            tools.append(line)

    with Path(summary_path).open("a", encoding="utf-8") as summary:
        summary.write("### Immutable supply-chain inputs\n\n")
        for pin in sorted(action_pins):
            summary.write(f"- Action: `{pin}`\n")
        for image in sorted(service_images):
            summary.write(f"- Service image: `{image}`\n")
        for tool in tools:
            summary.write(f"- Tool: `{tool}`\n")


def main() -> None:
    check_workflows()
    check_git_dependencies()
    check_exceptions()
    check_tool_manifest()
    write_workflow_summary()
    print("Supply-chain policy checks passed.")


if __name__ == "__main__":
    main()
