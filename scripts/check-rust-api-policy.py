#!/usr/bin/env python3
"""Validate workspace Rust API support and publishing classifications."""

from __future__ import annotations

import argparse
import json
import subprocess
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


@dataclass(frozen=True)
class CargoPublishPolicy:
    enabled: bool
    crates_io_enabled: bool


class PolicyError(RuntimeError):
    """Raised when the workspace violates the Rust API policy."""


def fail(message: str) -> NoReturn:
    raise PolicyError(message)


def read_manifest(path: Path) -> dict[str, object]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"cannot read {path}: {error}")


def cargo_workspace_metadata(root: Path) -> dict[str, object]:
    root_manifest = (root / "Cargo.toml").resolve()
    data = read_manifest(root_manifest)
    workspace = data.get("workspace")
    if not isinstance(workspace, dict):
        fail("root Cargo.toml must declare [workspace]")

    command = [
        "cargo",
        "metadata",
        "--format-version",
        "1",
        "--no-deps",
        "--manifest-path",
        str(root_manifest),
    ]
    if (root / "Cargo.lock").is_file():
        command.append("--locked")
    try:
        result = subprocess.run(
            command,
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        fail(f"cannot execute cargo metadata: {error}")
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        fail(f"cargo metadata failed: {detail}")
    try:
        metadata = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"cargo metadata returned invalid JSON: {error}")
    if not isinstance(metadata, dict):
        fail("cargo metadata must return a JSON object")
    return metadata


def workspace_manifests(root: Path) -> list[Path]:
    root_manifest = (root / "Cargo.toml").resolve()
    metadata = cargo_workspace_metadata(root)
    workspace_members = metadata.get("workspace_members")
    packages = metadata.get("packages")
    if not isinstance(workspace_members, list) or not all(
        isinstance(member, str) for member in workspace_members
    ):
        fail("cargo metadata returned invalid workspace_members")
    if not isinstance(packages, list):
        fail("cargo metadata returned invalid packages")

    manifests_by_id: dict[str, Path] = {}
    for package in packages:
        if not isinstance(package, dict):
            fail("cargo metadata returned an invalid package")
        package_id = package.get("id")
        manifest_path = package.get("manifest_path")
        if not isinstance(package_id, str) or not isinstance(manifest_path, str):
            fail("cargo metadata package is missing its id or manifest_path")
        manifest = Path(manifest_path).resolve()
        if not manifest.is_relative_to(root):
            fail(f"workspace manifest must remain inside the repository: {manifest}")
        manifests_by_id[package_id] = manifest

    manifests: list[Path] = []
    for member in workspace_members:
        manifest = manifests_by_id.get(member)
        if manifest is None:
            fail(f"cargo metadata omitted workspace member {member}")
        manifests.append(manifest)
    if root_manifest not in manifests:
        fail("the root Cargo.toml package must be a Cargo workspace member")
    if len(manifests) != len(set(manifests)):
        fail("cargo metadata returned duplicate workspace manifests")

    remaining = sorted(
        (manifest for manifest in manifests if manifest != root_manifest),
        key=lambda manifest: str(manifest.relative_to(root)),
    )
    return [root_manifest, *remaining]


def cargo_publish_policy(name: str, publish_value: object) -> CargoPublishPolicy:
    if isinstance(publish_value, bool):
        return CargoPublishPolicy(
            enabled=publish_value,
            crates_io_enabled=publish_value,
        )
    if isinstance(publish_value, list):
        if not all(
            isinstance(registry, str) and registry for registry in publish_value
        ):
            fail(f"package {name} has an invalid Cargo publish setting")
        registries = set(publish_value)
        return CargoPublishPolicy(
            enabled=bool(registries),
            crates_io_enabled="crates-io" in registries,
        )
    fail(f"package {name} has an invalid Cargo publish setting")


def package_manifest(
    root: Path, manifest: Path
) -> tuple[str, dict[str, object], dict[str, object] | None]:
    data = read_manifest(manifest)
    package = data.get("package")
    if not isinstance(package, dict):
        fail(f"{manifest.relative_to(root)} must declare [package]")
    name = package.get("name")
    if not isinstance(name, str) or not name:
        fail(f"{manifest.relative_to(root)} has no package name")
    metadata = package.get("metadata")
    hubuum = metadata.get("hubuum") if isinstance(metadata, dict) else None
    if not isinstance(hubuum, dict):
        hubuum = None
    return name, package, hubuum


def policy_document_path(
    root: Path, name: str, declared: object
) -> tuple[Path, str]:
    if not isinstance(declared, str) or not declared.strip():
        fail(f"public package {name} must declare a policy-document")
    declared_path = Path(declared)
    if declared_path.is_absolute():
        fail(f"public package {name} policy-document must be repository-relative")
    policy_path = (root / declared_path).resolve()
    if not policy_path.is_relative_to(root):
        fail(f"public package {name} policy-document must remain inside the repository")
    return policy_path, policy_path.relative_to(root).as_posix()


def public_policy_document(root: Path, name: str, declared: object) -> str:
    policy_path, relative_path = policy_document_path(root, name, declared)
    if not policy_path.is_file():
        fail(
            f"public package {name} policy-document does not exist as a file: "
            f"{relative_path}"
        )
    try:
        with policy_path.open("rb") as policy_file:
            policy_file.read(1)
    except OSError as error:
        fail(f"public package {name} policy-document is not readable: {error}")
    return relative_path


def declared_policy_documents(root: Path) -> list[str]:
    documents: set[str] = set()
    for manifest in workspace_manifests(root):
        name, _, hubuum = package_manifest(root, manifest)
        if hubuum is None or "policy-document" not in hubuum:
            continue
        _, relative_path = policy_document_path(
            root,
            name,
            hubuum["policy-document"],
        )
        documents.add(relative_path)
    return sorted(documents)


def package_policy(root: Path, manifest: Path) -> PackagePolicy:
    name, package, hubuum = package_manifest(root, manifest)
    rust_api = hubuum.get("rust-api") if hubuum is not None else None
    if rust_api not in VALID_STATUSES:
        fail(
            f"{name} must set package.metadata.hubuum.rust-api to one of "
            f"{sorted(VALID_STATUSES)!r}"
        )

    publish = cargo_publish_policy(name, package.get("publish", True))
    policy_document = hubuum.get("policy-document") if hubuum is not None else None
    if rust_api in INTERNAL_STATUSES:
        if publish.enabled:
            fail(f"internal package {name} must disable publishing")
        if policy_document is not None:
            fail(f"internal package {name} must not declare policy-document")
    else:
        if not publish.enabled:
            fail(
                f"public package {name} must enable publishing with publish = true "
                'or a registry list containing "crates-io"'
            )
        if not publish.crates_io_enabled:
            fail(
                f"public package {name} must allow crates.io publishing with "
                'publish = true or a registry list containing "crates-io"'
            )
        policy_document = public_policy_document(
            root,
            name,
            policy_document,
        )

    return PackagePolicy(
        name=name,
        manifest=str(manifest.relative_to(root)),
        rust_api=rust_api,
        publish=publish.enabled,
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    output = parser.add_mutually_exclusive_group()
    output.add_argument("--json", action="store_true")
    output.add_argument("--supported-packages", action="store_true")
    output.add_argument("--declared-policy-documents", action="store_true")
    arguments = parser.parse_args()

    try:
        if arguments.declared_policy_documents:
            for document in declared_policy_documents(arguments.root.resolve()):
                print(document)
            return 0
        result = policies(arguments.root.resolve())
    except PolicyError as error:
        print(f"Rust API policy error: {error}", file=sys.stderr)
        return 1
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
