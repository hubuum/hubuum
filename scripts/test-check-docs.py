#!/usr/bin/env python3
"""Regression tests for documentation coverage and GitHub Pages path checking."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import importlib.util
import json
from pathlib import Path
import tempfile
import tomllib
import unittest


spec = importlib.util.spec_from_file_location("check_docs", Path(__file__).with_name("check-docs.py"))
assert spec and spec.loader
docs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(docs)
version_spec = importlib.util.spec_from_file_location("docs_versions", Path(__file__).with_name("docs-versions.py"))
assert version_spec and version_spec.loader
versions = importlib.util.module_from_spec(version_spec)
version_spec.loader.exec_module(versions)


class DocumentationChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")

    def test_navigation_requires_every_document_once(self):
        self.write("docs/index.md", "# Home")
        self.write("docs/nested/guide.md", "# Guide")
        cases = [
            (["index.md"], "missing from navigation"),
            (["index.md", "nested/guide.md", "gone.md"], "does not exist"),
            (["index.md", "nested/guide.md", "index.md"], "more than once"),
        ]
        for nav, expected in cases:
            with self.subTest(nav=nav):
                errors = docs.check_navigation(self.root, {"docs_dir": "docs", "nav": nav})
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_navigation_supports_nested_sections_and_external_projects(self):
        self.write("docs/index.md", "# Home")
        self.write("docs/nested/guide.md", "# Guide")
        nav = [{"Home": "index.md"}, {"Guides": [{"Guide": "nested/guide.md"}]}, {"CLI": "https://example.org/cli"}]
        self.assertEqual(docs.check_navigation(self.root, {"docs_dir": "docs", "nav": nav}), [])

    def test_rendered_links_support_project_prefix_and_assets(self):
        self.write("index.html", '<a href="/hubuum/guide/#topic">Guide</a><a href="openapi.json">API</a>')
        self.write("openapi.json", "{}")
        self.write("guide/index.html", '<h1 id="topic">Guide</h1><a href="../">Home</a><img src="../logo.svg">')
        self.write("logo.svg", "<svg/>")
        self.assertEqual(docs.check_site(self.root, "https://hubuum.github.io/hubuum/"), [])

    def test_rendered_links_reject_broken_targets_and_base_paths(self):
        self.write("guide/index.html", '<h1 id="topic">Guide</h1>')
        cases = [
            ("gone/", "missing target"),
            ("guide/#gone", "missing anchor"),
            ("/guide/", "escapes site base path"),
            ("../outside.html", "escapes site directory"),
            ("missing.svg", "missing target"),
        ]
        for link, expected in cases:
            with self.subTest(link=link):
                self.write("index.html", f'<a href="{link}">Target</a>')
                errors = docs.check_site(self.root, "https://hubuum.github.io/hubuum/")
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_empty_build_fails(self):
        self.assertEqual(docs.check_site(self.root, "https://hubuum.github.io/hubuum/"), ["Built site is missing index.html"])

    def test_latest_release_is_default_even_when_main_is_updated(self):
        self.write("build/index.html", "release")
        archive = self.root / "archive"
        for version in ("v0.0.9", "v0.0.16", "main", "v0.0.10"):
            versions.assemble(self.root / "build", archive, version, "a" * 40)
        self.assertIn('url=v0.0.16/', (archive / "index.html").read_text())
        manifest = json.loads((archive / "versions.json").read_text())
        self.assertEqual([entry["version"] for entry in manifest], ["v0.0.16", "v0.0.10", "v0.0.9", "main"])

    def test_development_alone_does_not_become_public_default(self):
        self.write("build/index.html", "development")
        versions.assemble(self.root / "build", self.root / "archive", "main", "a" * 40)
        self.assertFalse((self.root / "archive/index.html").exists())

    def test_release_snapshot_is_immutable_across_renderer_changes(self):
        self.write("build/index.html", "original")
        versions.assemble(self.root / "build", self.root / "archive", "v0.0.16", "a" * 40)
        self.write("build/index.html", "different renderer")
        versions.assemble(self.root / "build", self.root / "archive", "v0.0.16", "a" * 40)
        self.assertEqual((self.root / "archive/v0.0.16/index.html").read_text(), "original")

    def test_retargeted_release_tag_is_rejected(self):
        self.write("build/index.html", "original")
        versions.assemble(self.root / "build", self.root / "archive", "v0.0.16", "a" * 40)
        with self.assertRaisesRegex(ValueError, "source commit changed"):
            versions.assemble(self.root / "build", self.root / "archive", "v0.0.16", "b" * 40)

    def test_main_can_be_replaced_without_changing_release_snapshots(self):
        self.write("build/index.html", "original")
        archive = self.root / "archive"
        versions.assemble(self.root / "build", archive, "v0.0.16", "a" * 40)
        versions.assemble(self.root / "build", archive, "main", "a" * 40)
        self.write("build/index.html", "updated")
        versions.assemble(self.root / "build", archive, "main", "b" * 40)
        self.assertEqual((archive / "main/index.html").read_text(), "updated")
        self.assertEqual((archive / "v0.0.16/index.html").read_text(), "original")

    def test_invalid_versions_cannot_escape_archive(self):
        for version in ("../escape", "v1.2", "v1.2.3-beta", "v01.2.3", "/tmp/escape"):
            with self.subTest(version=version), self.assertRaises(ValueError):
                versions.validate_version(version)

    def test_legacy_release_uses_only_its_own_documents(self):
        self.write("source/docs/querying.md", "# Original release query guide\n")
        self.write("source/docs/retired.md", "# Retired guide\n")
        stage = self.root / "stage"
        versions.prepare(self.root / "source", stage, "v0.0.1", "a" * 40)
        project = tomllib.loads((stage / "zensical.toml").read_text())["project"]
        self.assertEqual(docs.check_navigation(stage, project), [])
        self.assertEqual((stage / "docs/querying.md").read_text(), "# Original release query guide\n")
        self.assertFalse((stage / "docs/credential_approvals.md").exists())
        self.assertEqual(project["site_url"], "https://hubuum.github.io/hubuum/v0.0.1/")


if __name__ == "__main__":
    unittest.main()
