#!/usr/bin/env python3
"""Create a CycloneDX SBOM tying Cargo dependencies to a release subject."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import subprocess
import urllib.parse
from pathlib import Path
from typing import Any


SHA256_DIGEST = re.compile(r"^[0-9a-f]{64}$")
SOURCE_REVISION = re.compile(r"^[0-9a-f]{40}$")
SOURCE_TAG = re.compile(r"^(?:main|v\d+\.\d+\.\d+)$")
TARGET = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
CARGO_FEATURES = re.compile(r"^[A-Za-z0-9_/-]+(?:,[A-Za-z0-9_/-]+)*$")
ROOT = Path(__file__).resolve().parent.parent


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cargo_metadata(
    path: Path | None,
    target: str,
    features: str | None,
    no_default_features: bool,
) -> dict[str, Any]:
    if path is not None:
        return json.loads(path.read_text(encoding="utf-8"))
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--format-version",
        "1",
        "--filter-platform",
        target,
    ]
    if no_default_features:
        command.append("--no-default-features")
    if features:
        command.extend(("--features", features))
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        cwd=ROOT,
        text=True,
    )
    return json.loads(result.stdout)


def package_ref(package: dict[str, Any]) -> str:
    name = urllib.parse.quote(package["name"], safe="")
    version = urllib.parse.quote(package["version"], safe="")
    source = package.get("source") or "workspace"
    source_hash = hashlib.sha256(source.encode()).hexdigest()[:16]
    return f"pkg:cargo/{name}@{version}?source={source_hash}"


def package_component(package: dict[str, Any]) -> dict[str, Any]:
    component: dict[str, Any] = {
        "type": "library",
        "bom-ref": package_ref(package),
        "name": package["name"],
        "version": package["version"],
        "purl": (
            "pkg:cargo/"
            f"{urllib.parse.quote(package['name'], safe='')}@"
            f"{urllib.parse.quote(package['version'], safe='')}"
        ),
        "properties": [
            {"name": "cargo:package_id", "value": package["id"]},
            {"name": "cargo:source", "value": package.get("source") or "workspace"},
        ],
    }
    if package.get("license"):
        component["licenses"] = [{"expression": package["license"]}]
    if package.get("checksum"):
        component["hashes"] = [
            {"alg": "SHA-256", "content": package["checksum"]}
        ]

    references = []
    for reference_type, field in (
        ("vcs", "repository"),
        ("website", "homepage"),
        ("documentation", "documentation"),
    ):
        if package.get(field):
            references.append({"type": reference_type, "url": package[field]})
    if references:
        component["externalReferences"] = references
    return component


def cargo_graph(metadata: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    packages = {package["id"]: package for package in metadata["packages"]}
    resolve = metadata.get("resolve") or {"nodes": []}
    resolved_ids = {node["id"] for node in resolve["nodes"]}
    components = [package_component(packages[package_id]) for package_id in resolved_ids]
    refs = {package_id: package_ref(packages[package_id]) for package_id in resolved_ids}

    dependencies = []
    for node in resolve["nodes"]:
        if node["id"] not in refs:
            continue
        depends_on = sorted(
            {
                refs[dependency["pkg"]]
                for dependency in node.get("deps", [])
                if dependency["pkg"] in refs
            }
        )
        dependencies.append({"ref": refs[node["id"]], "dependsOn": depends_on})

    workspace_refs = sorted(
        refs[package_id]
        for package_id in metadata.get("workspace_members", [])
        if package_id in refs
    )
    return components, dependencies, workspace_refs


def merge_base_sbom(
    base_path: Path | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    if base_path is None:
        return [], [], []
    base = json.loads(base_path.read_text(encoding="utf-8"))
    if base.get("bomFormat") != "CycloneDX":
        raise SystemExit("base SBOM is not CycloneDX")

    metadata = base.get("metadata", {})
    if not isinstance(metadata, dict):
        raise SystemExit("base SBOM metadata must be an object")
    root_component = metadata.get("component", {})
    if not isinstance(root_component, dict):
        raise SystemExit("base SBOM metadata component must be an object")
    old_root = root_component.get("bom-ref")
    if old_root is not None and (not isinstance(old_root, str) or not old_root):
        raise SystemExit("base SBOM root bom-ref must be a non-empty string")

    components = base.get("components", [])
    if not isinstance(components, list) or not all(
        isinstance(component, dict) for component in components
    ):
        raise SystemExit("base SBOM components must be an array of objects")
    base_dependencies = base.get("dependencies", [])
    if not isinstance(base_dependencies, list) or not all(
        isinstance(dependency, dict) for dependency in base_dependencies
    ):
        raise SystemExit("base SBOM dependencies must be an array of objects")

    dependencies = []
    root_dependencies: list[str] = []
    for dependency in base_dependencies:
        if old_root and dependency.get("ref") == old_root:
            depends_on = dependency.get("dependsOn", [])
            if not isinstance(depends_on, list) or not all(
                isinstance(reference, str) and reference for reference in depends_on
            ):
                raise SystemExit("base SBOM root dependency has invalid dependsOn")
            root_dependencies.extend(depends_on)
        else:
            dependencies.append(dependency)
    return components, dependencies, root_dependencies


def merge_components(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    referenced: dict[str, dict[str, Any]] = {}
    unreferenced: list[dict[str, Any]] = []
    for component in items:
        reference = component.get("bom-ref")
        if reference is None:
            unreferenced.append(component)
            continue
        if not isinstance(reference, str) or not reference:
            raise SystemExit("SBOM component bom-ref must be a non-empty string")
        existing = referenced.get(reference)
        if existing is not None and existing != component:
            raise SystemExit(f"conflicting SBOM components use bom-ref {reference!r}")
        referenced[reference] = component
    return [referenced[key] for key in sorted(referenced)] + unreferenced


def merge_dependencies(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for dependency in items:
        reference = dependency.get("ref")
        depends_on = dependency.get("dependsOn", [])
        if not isinstance(reference, str) or not reference:
            raise SystemExit("SBOM dependency ref must be a non-empty string")
        if not isinstance(depends_on, list) or not all(
            isinstance(item, str) and item for item in depends_on
        ):
            raise SystemExit(f"SBOM dependency {reference!r} has invalid dependsOn")

        existing = merged.get(reference)
        if existing is None:
            merged[reference] = {
                **dependency,
                "dependsOn": sorted(set(depends_on)),
            }
            continue

        for key, value in dependency.items():
            if key not in {"ref", "dependsOn"} and existing.get(key) != value:
                raise SystemExit(
                    f"conflicting SBOM dependency metadata uses ref {reference!r}"
                )
        existing["dependsOn"] = sorted(set(existing["dependsOn"] + depends_on))

    return [merged[key] for key in sorted(merged)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--base-sbom", type=Path)
    parser.add_argument("--cargo-features")
    parser.add_argument("--cargo-target", required=True)
    parser.add_argument("--metadata-json", type=Path)
    parser.add_argument("--no-default-features", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--subject-name")
    parser.add_argument("--subject-digest")
    parser.add_argument("--subject-type", choices=("file", "container"), default="file")
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--source-tag", required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    if args.artifact is None and (not args.subject_name or not args.subject_digest):
        parser.error("provide --artifact or both --subject-name and --subject-digest")
    if args.artifact is not None and args.subject_digest is not None:
        parser.error("--subject-digest cannot be combined with --artifact")
    if args.subject_name is not None and not args.subject_name.strip():
        parser.error("--subject-name cannot be blank")
    if not SOURCE_REVISION.fullmatch(args.source_revision):
        parser.error("--source-revision must be a full lowercase commit SHA")
    if not SOURCE_TAG.fullmatch(args.source_tag):
        parser.error("--source-tag must be main or a stable v-prefixed version")
    if not TARGET.fullmatch(args.target):
        parser.error("--target contains unsupported characters")
    if not TARGET.fullmatch(args.cargo_target):
        parser.error("--cargo-target contains unsupported characters")
    if args.cargo_features and not CARGO_FEATURES.fullmatch(args.cargo_features):
        parser.error("--cargo-features must be a comma-separated feature list")
    return args


def main() -> None:
    args = parse_args()
    metadata = cargo_metadata(
        args.metadata_json,
        args.cargo_target,
        args.cargo_features,
        args.no_default_features,
    )
    cargo_components, cargo_dependencies, workspace_refs = cargo_graph(metadata)
    base_components, base_dependencies, base_root_dependencies = merge_base_sbom(
        args.base_sbom
    )

    if args.artifact is not None:
        subject_name = args.subject_name or args.artifact.name
        digest = sha256(args.artifact)
    else:
        subject_name = args.subject_name
        algorithm, separator, digest = args.subject_digest.partition(":")
        if (
            separator != ":"
            or algorithm != "sha256"
            or not SHA256_DIGEST.fullmatch(digest)
        ):
            raise SystemExit("--subject-digest must be sha256:<64 lowercase hex characters>")

    lock_digest = sha256(ROOT / "Cargo.lock")
    root_ref = f"urn:hubuum:release:{digest}"
    root_component = {
        "type": args.subject_type,
        "bom-ref": root_ref,
        "name": subject_name,
        "version": args.source_tag,
        "hashes": [{"alg": "SHA-256", "content": digest}],
        "properties": [
            {"name": "hubuum:source_revision", "value": args.source_revision},
            {"name": "hubuum:release_tag", "value": args.source_tag},
            {"name": "hubuum:target", "value": args.target},
            {"name": "hubuum:cargo_target", "value": args.cargo_target},
            {
                "name": "hubuum:cargo_features",
                "value": args.cargo_features or "",
            },
            {
                "name": "hubuum:cargo_default_features",
                "value": str(not args.no_default_features).lower(),
            },
            {"name": "hubuum:cargo_lock_sha256", "value": lock_digest},
        ],
    }

    components = merge_components(base_components + cargo_components)
    dependencies = merge_dependencies(base_dependencies + cargo_dependencies)
    root_dependencies = sorted(set(base_root_dependencies + workspace_refs))
    dependencies.append({"ref": root_ref, "dependsOn": root_dependencies})
    dependencies = merge_dependencies(dependencies)

    document = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "version": 1,
        "metadata": {
            "timestamp": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
            "tools": {
                "components": [
                    {
                        "type": "application",
                        "name": "hubuum-release-sbom-generator",
                        "version": "1",
                    }
                ]
            },
            "component": root_component,
            "properties": [
                {
                    "name": "hubuum:base_sbom_merged",
                    "value": str(args.base_sbom is not None).lower(),
                }
            ],
        },
        "components": components,
        "dependencies": dependencies,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
