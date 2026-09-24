#!/usr/bin/env python3
"""Check documentation navigation coverage and the built site's local links."""

from __future__ import annotations

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
import tomllib
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parent.parent


def navigation_paths(value: object) -> list[str]:
    if isinstance(value, str):
        return [] if urlsplit(value).scheme else [value]
    if isinstance(value, list):
        return [path for item in value for path in navigation_paths(item)]
    if isinstance(value, dict):
        return [path for item in value.values() for path in navigation_paths(item)]
    raise ValueError(f"Unsupported navigation value: {value!r}")


def check_navigation(root: Path, project: dict) -> list[str]:
    docs = root / project["docs_dir"]
    actual = {path.relative_to(docs).as_posix() for path in docs.rglob("*.md")}
    paths = navigation_paths(project["nav"])
    errors = [f"Page missing from navigation: {path}" for path in sorted(actual - set(paths))]
    errors.extend(f"Navigation target does not exist: {path}" for path in sorted(set(paths) - actual))
    errors.extend(f"Page occurs more than once in navigation: {path}" for path, count in Counter(paths).items() if count > 1)
    return errors


class Page(HTMLParser):
    def __init__(self, text: str) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: set[str] = set()
        self.links: list[str] = []
        self.feed(text)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if anchor := attributes.get("id"):
            self.anchors.add(anchor)
        for name in ("href", "src"):
            if value := attributes.get(name):
                self.links.append(value)


def check_site(site: Path, site_url: str) -> list[str]:
    site = site.resolve()
    pages = {path: Page(path.read_text(encoding="utf-8")) for path in site.rglob("*.html")}
    errors = []
    if site / "index.html" not in pages:
        errors.append("Built site is missing index.html")
    prefix = urlsplit(site_url).path.rstrip("/") + "/"
    for source, page in pages.items():
        for link in page.links:
            url = urlsplit(link)
            if url.scheme or url.netloc:
                continue
            path = unquote(url.path)
            if path.startswith("/"):
                if not path.startswith(prefix):
                    errors.append(f"{source.relative_to(site)}: link escapes site base path: {link}")
                    continue
                target = site / path.removeprefix(prefix)
            else:
                target = source.parent / path if path else source
            target = target.resolve()
            if not target.is_relative_to(site):
                errors.append(f"{source.relative_to(site)}: link escapes site directory: {link}")
                continue
            if target.is_dir():
                target /= "index.html"
            if not target.is_file():
                errors.append(f"{source.relative_to(site)}: missing target: {link}")
            elif url.fragment and target in pages and unquote(url.fragment) not in pages[target].anchors:
                errors.append(f"{source.relative_to(site)}: missing anchor: {link}")
    return sorted(set(errors))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT, help="Prepared source and configuration root")
    parser.add_argument("--site-dir", type=Path, help="Also check rendered links and assets")
    args = parser.parse_args()
    project = tomllib.loads((args.root / "zensical.toml").read_text(encoding="utf-8"))["project"]
    errors = check_navigation(args.root, project)
    if args.site_dir:
        errors.extend(check_site(args.site_dir, project["site_url"]))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("Documentation navigation" + (" and rendered links" if args.site_dir else "") + " verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
