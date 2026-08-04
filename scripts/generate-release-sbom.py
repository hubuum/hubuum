#!/usr/bin/env python3
"""Create a CycloneDX SBOM tying Cargo dependencies to a release subject."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import urllib.parse
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cargo_metadata(path: Path | None) -> dict[str, Any]:
    if path is not None:
        return json.loads(path.read_text(encoding="utf-8"))
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--all-features",
            "--format-version",
            "1",
        ],
        check=True,
        capture_output=True,
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

    old_root = base.get("metadata", {}).get("component", {}).get("bom-ref")
    components = list(base.get("components", []))
    dependencies = []
    root_dependencies: list[str] = []
    for dependency in base.get("dependencies", []):
        if old_root and dependency.get("ref") == old_root:
            root_dependencies.extend(dependency.get("dependsOn", []))
        else:
            dependencies.append(dependency)
    return components, dependencies, root_dependencies


def unique_by_ref(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for item in items:
        reference = item.get("bom-ref") or item.get("ref")
        if reference:
            unique[reference] = item
    return [unique[key] for key in sorted(unique)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact", type=Path)
    parser.add_argument("--base-sbom", type=Path)
    parser.add_argument("--metadata-json", type=Path)
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
    return args


def main() -> None:
    args = parse_args()
    metadata = cargo_metadata(args.metadata_json)
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
        if separator != ":" or algorithm.lower() != "sha256" or len(digest) != 64:
            raise SystemExit("--subject-digest must be sha256:<64 lowercase hex characters>")
        int(digest, 16)

    lock_digest = sha256(Path("Cargo.lock"))
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
            {"name": "hubuum:cargo_lock_sha256", "value": lock_digest},
        ],
    }

    components = unique_by_ref(base_components + cargo_components)
    dependencies = unique_by_ref(base_dependencies + cargo_dependencies)
    root_dependencies = sorted(set(base_root_dependencies + workspace_refs))
    dependencies.append({"ref": root_ref, "dependsOn": root_dependencies})
    dependencies = unique_by_ref(dependencies)

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
