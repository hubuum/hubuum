#!/usr/bin/env python3
"""Validate immutable workflow inputs and exception governance."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import tomllib
from pathlib import Path
from typing import NoReturn


ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = ROOT / ".github" / "workflows"
SHA_REF = re.compile(r"^[0-9a-f]{40}$")
OCI_DIGEST_REF = re.compile(
    r"^[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9._-]*)?@sha256:[0-9a-f]{64}$"
)
MUTABLE_INSTALLER = re.compile(
    r"(?i)(?:curl|wget)[^\n]*\|\s*(?:ba)?sh|irm[^\n]*\|\s*iex|/latest/download/"
)
REQUIRED_TOOL_KEYS = {
    "CARGO_DENY_VERSION",
    "DIESEL_CLI_VERSION",
    "SYFT_IMAGE",
    "TRIVY_IMAGE",
    "COSIGN_VERSION",
}


def fail(message: str) -> NoReturn:
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
            if image_match and not OCI_DIGEST_REF.fullmatch(image_match.group(1)):
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


def add_exception(
    exceptions: set[tuple[str, str]], kind: str, identifier: object, source: str
) -> None:
    if not isinstance(identifier, str) or not identifier.strip():
        fail(f"{source} has an invalid {kind} exception identifier")
    exception = (kind, identifier)
    if exception in exceptions:
        fail(f"{source} configures duplicate exception {exception!r}")
    exceptions.add(exception)


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
        if isinstance(entry, str):
            identifier = entry
        elif isinstance(entry, dict):
            identifier = entry.get("id") or entry.get("crate")
        else:
            identifier = None
        add_exception(configured, "advisory", identifier, "deny.toml")

    for entry in policy.get("licenses", {}).get("exceptions", []):
        identifier = (
            entry.get("crate") or entry.get("name")
            if isinstance(entry, dict)
            else None
        )
        add_exception(configured, "license", identifier, "deny.toml")

    trivy_ignore = ROOT / ".trivyignore"
    for line in trivy_ignore.read_text(encoding="utf-8").splitlines():
        identifier = line.strip()
        if identifier and not identifier.startswith("#"):
            add_exception(
                configured, "container-vulnerability", identifier, ".trivyignore"
            )

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
        identity = (exception["kind"], exception["id"])
        if identity in documented:
            fail(f"exception {index} duplicates {identity!r}")
        documented.add(identity)

    configured = configured_exceptions()
    if configured != documented:
        fail(
            "deny.toml exceptions and .github/supply-chain-exceptions.json differ: "
            f"configured={sorted(configured)!r}, documented={sorted(documented)!r}"
        )


def parse_tool_manifest(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line or line.startswith("#"):
            continue
        if line != line.strip():
            fail(f"{path.name}:{line_number} has leading or trailing whitespace")
        key, separator, value = line.partition("=")
        if (
            not separator
            or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key)
            or not value
        ):
            fail(f"{path.name}:{line_number} is not a valid KEY=value entry")
        if key in values:
            fail(f"{path.name}:{line_number} duplicates {key}")
        values[key] = value
    return values


def validate_tool_values(values: dict[str, str]) -> None:
    if values.keys() != REQUIRED_TOOL_KEYS:
        fail(f"tool manifest keys must be exactly {sorted(REQUIRED_TOOL_KEYS)!r}")
    for key in ("SYFT_IMAGE", "TRIVY_IMAGE"):
        if not OCI_DIGEST_REF.fullmatch(values[key]):
            fail(f"{key} must be a literal OCI reference with a full SHA-256 digest")
    for key in ("CARGO_DENY_VERSION", "DIESEL_CLI_VERSION"):
        if not re.fullmatch(r"\d+\.\d+\.\d+", values[key]):
            fail(f"{key} must be an exact semantic version")
    if not re.fullmatch(r"v\d+\.\d+\.\d+", values["COSIGN_VERSION"]):
        fail("COSIGN_VERSION must be an exact v-prefixed semantic version")


def check_tool_manifest() -> None:
    path = ROOT / ".github" / "supply-chain-tools.env"
    values = parse_tool_manifest(path)
    validate_tool_values(values)
    ci_workflow = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
    cosign_versions = re.findall(r"\bcosign-release:\s*([^\s#]+)", ci_workflow)
    if not cosign_versions:
        fail("ci.yml does not configure cosign-release")
    if set(cosign_versions) != {values["COSIGN_VERSION"]}:
        fail("every ci.yml cosign-release must match the pinned tool manifest")


def print_tool_value(key: str) -> None:
    values = parse_tool_manifest(ROOT / ".github" / "supply-chain-tools.env")
    validate_tool_values(values)
    print(values[key])


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

    tool_values = parse_tool_manifest(ROOT / ".github" / "supply-chain-tools.env")

    with Path(summary_path).open("a", encoding="utf-8") as summary:
        summary.write("### Immutable supply-chain inputs\n\n")
        for pin in sorted(action_pins):
            summary.write(f"- Action: `{pin}`\n")
        for image in sorted(service_images):
            summary.write(f"- Service image: `{image}`\n")
        for key in sorted(tool_values):
            summary.write(f"- Tool: `{key}={tool_values[key]}`\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tool-value", choices=sorted(REQUIRED_TOOL_KEYS))
    args = parser.parse_args()
    if args.tool_value:
        print_tool_value(args.tool_value)
        return

    check_workflows()
    check_git_dependencies()
    check_exceptions()
    check_tool_manifest()
    write_workflow_summary()
    print("Supply-chain policy checks passed.")


if __name__ == "__main__":
    main()
