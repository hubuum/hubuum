#!/usr/bin/env python3
"""Prepare tagged documentation and retain immutable release sites for Pages."""

from __future__ import annotations

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import html
import json
from pathlib import Path
import re
import shutil
import tomllib
from urllib.parse import quote, urlsplit


ROOT = Path(__file__).resolve().parent.parent
RELEASE = re.compile(r"v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)\Z")


def validate_version(version: str) -> str:
    if version != "main" and not RELEASE.fullmatch(version):
        raise ValueError("Version must be main or a stable release tag such as v0.0.16")
    return version


def toml_value(value: object) -> str:
    if isinstance(value, dict):
        return "{ " + ", ".join(f"{json.dumps(k)} = {toml_value(v)}" for k, v in value.items()) + " }"
    if isinstance(value, list):
        return "[" + ", ".join(map(toml_value, value)) + "]"
    return json.dumps(value, ensure_ascii=False)


def filter_nav(value: object, available: set[str], seen: set[str], source: Path, source_sha: str) -> object:
    if isinstance(value, str):
        if match := re.match(r"https://github.com/hubuum/hubuum/(blob|tree)/main/(.*)", value):
            if not (source / match[2]).exists():
                return None
            return value.replace("/main/", f"/{source_sha}/", 1)
        if urlsplit(value).scheme or value in available:
            seen.add(value)
            return value
        return None
    if isinstance(value, list):
        return [item for child in value if (item := filter_nav(child, available, seen, source, source_sha))]
    return {key: item for key, child in value.items() if (item := filter_nav(child, available, seen, source, source_sha))}


def release_home(source: Path, version: str, source_sha: str) -> str:
    sections = [
        ("Install & operate", [("Deployment", "deployment.md"), ("Configuration", "quick_start.md"), ("Backup & restore", "backup-restore.md")]),
        ("Work with data", [("Collections", "collection_hierarchy.md"), ("Permissions", "permissions.md"), ("Querying", "querying.md")]),
        ("Build integrations", [("HTTP specification", "openapi.json"), ("Authentication", "auth_model.md"), ("Imports", "import_api.md")]),
        ("Contribute", [("Development", "development.md"), ("Architecture", "storage_boundary.md"), ("Release process", "releasing.md")]),
    ]
    cards = []
    for title, links in sections:
        available = [f"[{label}]({path})" for label, path in links if (source / "docs" / path).is_file()]
        if available:
            cards.append(f"- **{title}**\n\n    " + " · ".join(available))
    return (
        f"# Hubuum {version} documentation\n\n"
        "Guides and reference for this released version of Hubuum. "
        "Choose a starting point below, or search for a workflow, endpoint, or setting.\n\n"
        '<div class="grid cards" markdown>\n\n' + "\n\n".join(cards) + "\n\n</div>\n\n"
        "## Clients and interfaces\n\n"
        "Connect through the [web frontend](https://github.com/hubuum/hubuum-frontend), "
        "[CLI](https://github.com/hubuum/hubuum-cli), "
        "[Rust client](https://github.com/hubuum/hubuum-client-rust), or "
        "[Python client](https://github.com/hubuum/hubuum-client-python). "
        "Check the companion project's declared server compatibility before choosing its version.\n\n"
        "## About this edition\n\n"
        f"This edition preserves the documentation shipped with **{version}** and uses the current website navigation and theme. "
        "Choose **main (development)** in the version menu only when you want unreleased documentation.\n\n"
        f"[Release notes](https://github.com/hubuum/hubuum/releases/tag/{version}) · "
        f"[Source snapshot](https://github.com/hubuum/hubuum/tree/{source_sha}/docs)\n"
    )


def prepare(source: Path, destination: Path, version: str, source_sha: str) -> None:
    validate_version(version)
    if source.resolve().is_relative_to(destination.resolve()):
        raise ValueError("The staging destination must not contain the source checkout")
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("Source revision must be a full Git commit SHA")
    if not (source / "docs").is_dir():
        raise ValueError("The selected release has no docs directory")
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True)
    shutil.copytree(source / "docs", destination / "docs")
    shutil.copytree(ROOT / "docs-site", destination / "docs-site")
    shutil.copytree(ROOT / "docs/assets", destination / "docs/assets", dirs_exist_ok=True)
    project = tomllib.loads((ROOT / "zensical.toml").read_text())["project"]
    base_url = project["site_url"].rstrip("/") + "/"
    project["site_url"] = base_url + version + "/"
    # The builder mounts target/docs-site here inside its container.
    project["site_dir"] = "site"
    project.setdefault("extra", {})["docs_version"] = version
    project["extra"]["docs_source_sha"] = source_sha
    if version != "main":
        project["edit_uri"] = ""
        project["theme"]["features"] = [feature for feature in project["theme"]["features"] if feature != "content.action.edit"]
        index = destination / "docs/index.md"
        if not index.exists():
            index.write_text(release_home(source, version, source_sha))
        available = {p.relative_to(destination / "docs").as_posix() for p in (destination / "docs").rglob("*.md")}
        seen: set[str] = set()
        project["nav"] = filter_nav(project["nav"], available, seen, source, source_sha)
        if extra := sorted(available - seen):
            project["nav"].append({"Release reference": extra})

    # Keep repository links pinned to the same source as the documentation.
    # Older releases predate the test-suite relocation fixes in the website PR.
    relocated = {
        "src/tests/api/v1/imports.rs": "tests/api_jobs_suite/imports.rs",
        "src/tests/api/meta.rs": "tests/api_platform_suite/meta.rs",
    }
    for page in (destination / "docs").rglob("*.md"):
        content = page.read_text()
        source_page = source / "docs" / page.relative_to(destination / "docs")

        def repository_link(match: re.Match) -> str:
            href = match[1]
            path, separator, anchor = href.partition("#")
            if not path.startswith("../"):
                return match[0]
            target = (source_page.parent / path).resolve()
            if target.is_relative_to((source / "docs").resolve()):
                return match[0]
            relative = target.relative_to(source.resolve()).as_posix()
            if not target.exists() and relative in relocated:
                relative = relocated[relative]
                target = source / relative
            kind = "tree" if target.is_dir() else "blob"
            ref = "main" if version == "main" else source_sha
            return f"](https://github.com/hubuum/hubuum/{kind}/{ref}/{quote(relative)}{separator}{anchor})"

        content = re.sub(r"\]\(([^)]+)\)", repository_link, content)
        if version != "main":
            content = re.sub(r"(https://github.com/hubuum/hubuum/(?:blob|tree)/)main/", rf"\g<1>{source_sha}/", content)
        page.write_text(content)
    (destination / "zensical.toml").write_text("[project]\n" + "\n".join(f"{json.dumps(k)} = {toml_value(v)}" for k, v in project.items()) + "\n")
    (destination / "build.json").write_text(json.dumps({"version": version, "source_sha": source_sha}, indent=2) + "\n")


def assemble(site: Path, archive: Path, version: str, source_sha: str) -> None:
    validate_version(version)
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("Source revision must be a full Git commit SHA")
    if not (site / "index.html").is_file():
        raise ValueError("Refusing to publish a site without index.html")
    manifest = archive / "versions.json"
    versions = json.loads(manifest.read_text()) if manifest.exists() else []
    existing = next((entry for entry in versions if entry["version"] == version), None)
    target = archive / version
    if existing and version != "main":
        if existing["source_sha"] != source_sha:
            raise ValueError(f"Refusing to replace immutable {version}: source commit changed")
        if not (target / "index.html").is_file():
            raise ValueError(f"Archive for {version} is incomplete")
        # Even a new renderer must not rewrite a previously published release.
        return
    if target.exists():
        if version != "main":
            raise ValueError(f"Refusing to overwrite untracked archive {version}")
        shutil.rmtree(target)
    archive.mkdir(parents=True, exist_ok=True)
    shutil.copytree(site, target)
    versions = [entry for entry in versions if entry["version"] != version]
    versions.append({"version": version, "source_sha": source_sha})
    releases = sorted((entry for entry in versions if entry["version"] != "main"), key=lambda entry: tuple(map(int, RELEASE.fullmatch(entry["version"]).groups())), reverse=True)
    versions = releases + [entry for entry in versions if entry["version"] == "main"]
    manifest.write_text(json.dumps(versions, indent=2) + "\n")
    (archive / ".nojekyll").touch()
    if releases:
        latest = releases[0]["version"]
        (archive / "index.html").write_text(
            '<!doctype html><html lang="en"><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>Hubuum documentation</title>'
            f'<meta http-equiv="refresh" content="0; url={latest}/">'
            f'<p><a href="{latest}/">Open the latest released documentation ({html.escape(latest)})</a>.</p></html>\n'
        )
        # GitHub Pages serves only the root 404 page for unknown paths.
        root_url = tomllib.loads((ROOT / "zensical.toml").read_text())["project"]["site_url"]
        (archive / "404.html").write_text(
            '<!doctype html><html lang="en"><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '<title>Page not found · Hubuum</title><h1>Page not found</h1>'
            f'<p>Open <a href="{html.escape(root_url, quote=True)}">Hubuum documentation</a> and select a version.</p></html>\n'
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("--source", type=Path, required=True)
    prep.add_argument("--destination", type=Path, required=True)
    publish = commands.add_parser("assemble")
    publish.add_argument("--site", type=Path, required=True)
    publish.add_argument("--archive", type=Path, required=True)
    for subparser in (prep, publish):
        subparser.add_argument("--version", required=True)
        subparser.add_argument("--source-sha", required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            prepare(args.source, args.destination, args.version, args.source_sha)
        else:
            assemble(args.site, args.archive, args.version, args.source_sha)
    except (ValueError, OSError) as error:
        parser.exit(1, f"Documentation version error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
