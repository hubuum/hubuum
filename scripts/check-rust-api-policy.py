#!/usr/bin/env python3
"""Validate workspace Rust API support and publishing classifications."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import NoReturn


DEFAULT_ROOT = Path(__file__).resolve().parent.parent
INTERNAL_STATUSES = {"internal-application", "workspace-internal"}
PUBLIC_STATUSES = {"experimental-public", "stable-public"}
VALID_STATUSES = INTERNAL_STATUSES | PUBLIC_STATUSES


@dataclass(frozen=True)
class PackagePolicy:
    name: str
    manifest: str
    rust_api: str
    publish: bool
    policy_document: str | None


def fail(message: str) -> NoReturn:
    print(f"Rust API policy error: {message}", file=sys.stderr)
    raise SystemExit(1)


def read_manifest(path: Path) -> dict[str, object]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"cannot read {path}: {error}")


def workspace_manifests(root: Path) -> list[Path]:
    root_manifest = root / "Cargo.toml"
    data = read_manifest(root_manifest)
    workspace = data.get("workspace")
    if not isinstance(workspace, dict):
        fail("root Cargo.toml must declare [workspace]")
    members = workspace.get("members")
    if not isinstance(members, list) or not all(
        isinstance(member, str) for member in members
    ):
        fail("workspace.members must be an array of package paths")
    return [root_manifest, *(root / member / "Cargo.toml" for member in members)]


def package_policy(root: Path, manifest: Path) -> PackagePolicy:
    data = read_manifest(manifest)
    package = data.get("package")
    if not isinstance(package, dict):
        fail(f"{manifest.relative_to(root)} must declare [package]")
    name = package.get("name")
    if not isinstance(name, str) or not name:
        fail(f"{manifest.relative_to(root)} has no package name")
    metadata = package.get("metadata")
    hubuum = metadata.get("hubuum") if isinstance(metadata, dict) else None
    rust_api = hubuum.get("rust-api") if isinstance(hubuum, dict) else None
    if rust_api not in VALID_STATUSES:
        fail(
            f"{name} must set package.metadata.hubuum.rust-api to one of "
            f"{sorted(VALID_STATUSES)!r}"
        )

    publish_value = package.get("publish", True)
    publish = publish_value is not False
    policy_document = (
        hubuum.get("policy-document") if isinstance(hubuum, dict) else None
    )
    if rust_api in INTERNAL_STATUSES:
        if publish:
            fail(f"internal package {name} must set publish = false")
        if policy_document is not None:
            fail(f"internal package {name} must not declare policy-document")
    else:
        if not publish:
            fail(f"public package {name} cannot set publish = false")
        if not isinstance(policy_document, str) or not policy_document.strip():
            fail(f"public package {name} must declare a policy-document")

    return PackagePolicy(
        name=name,
        manifest=str(manifest.relative_to(root)),
        rust_api=rust_api,
        publish=publish,
        policy_document=policy_document,
    )


def policies(root: Path) -> list[PackagePolicy]:
    result = [package_policy(root, manifest) for manifest in workspace_manifests(root)]
    if result[0].rust_api != "internal-application":
        fail("the root hubuum package must remain classified as internal-application")
    names = [policy.name for policy in result]
    if len(names) != len(set(names)):
        fail("workspace package names must be unique")
    return sorted(result, key=lambda policy: policy.name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true")
    output.add_argument("--supported-packages", action="store_true")
    arguments = parser.parse_args()

    result = policies(arguments.root.resolve())
    if arguments.supported_packages:
        for policy in result:
            if policy.rust_api in PUBLIC_STATUSES:
                print(policy.name)
    elif arguments.json:
        print(json.dumps([asdict(policy) for policy in result], indent=2))
    else:
        counts = {
            status: sum(policy.rust_api == status for policy in result)
            for status in sorted(VALID_STATUSES)
        }
        print(f"Rust API policy passed for {len(result)} package(s): {counts}")


if __name__ == "__main__":
    main()
