#!/usr/bin/env python3
"""Regression tests for the Rust API policy checker."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("check-rust-api-policy.py")


class RustApiPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        (self.root / "crates" / "member").mkdir(parents=True)
        (self.root / "src").mkdir()
        (self.root / "src" / "lib.rs").write_text("", encoding="utf-8")
        (self.root / "crates" / "member" / "src").mkdir()
        (self.root / "crates" / "member" / "src" / "lib.rs").write_text(
            "", encoding="utf-8"
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def write_workspace(
        self,
        *,
        root_status: str = "internal-application",
        root_publish: str = "false",
        member_status: str = "workspace-internal",
        member_publish: str = "false",
        member_policy: str = "",
        member_version: str = "0.0.1",
        member_release_train: str = "",
    ) -> None:
        (self.root / "Cargo.toml").write_text(
            textwrap.dedent(
                f"""
                [package]
                name = "root"
                version = "0.0.1"
                edition = "2024"
                publish = {root_publish}

                [package.metadata.hubuum]
                rust-api = "{root_status}"

                [workspace]
                members = ["crates/member"]
                """
            ),
            encoding="utf-8",
        )
        (self.root / "crates" / "member" / "Cargo.toml").write_text(
            textwrap.dedent(
                f"""
                [package]
                name = "member"
                version = "{member_version}"
                edition = "2024"
                readme = "../../docs/member.md"
                publish = {member_publish}

                [package.metadata.hubuum]
                rust-api = "{member_status}"
                {member_policy}
                {member_release_train}
                """
            ),
            encoding="utf-8",
        )

    def run_checker(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(self.root), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )

    def write_policy_document(self, path: str = "docs/member.md") -> None:
        policy_path = self.root / path
        policy_path.parent.mkdir(parents=True, exist_ok=True)
        policy_path.write_text("# Member policy\n", encoding="utf-8")

    def write_public_member(
        self,
        *,
        status: str = "stable-public",
        publish: str = "true",
        policy_path: str = "docs/member.md",
        create_policy: bool = True,
        version: str = "0.0.1",
        release_train: str | None = None,
    ) -> None:
        self.write_workspace(
            member_status=status,
            member_publish=publish,
            member_policy=f'policy-document = "{policy_path}"',
            member_version=version,
            member_release_train=(
                f'release-train = "{release_train}"' if release_train else ""
            ),
        )
        if create_policy:
            self.write_policy_document(policy_path)

    def write_dependency_package(
        self,
        *,
        status: str,
        publish: str,
        version: str = "0.1.0",
        release_train: str | None = None,
    ) -> None:
        dependency = self.root / "crates" / "dependency"
        (dependency / "src").mkdir(parents=True)
        (dependency / "src" / "lib.rs").write_text("", encoding="utf-8")
        policy = ""
        train = ""
        if status in {"experimental-public", "stable-public"}:
            policy = 'policy-document = "docs/dependency.md"'
            self.write_policy_document("docs/dependency.md")
        if release_train is not None:
            train = f'release-train = "{release_train}"'
        (dependency / "Cargo.toml").write_text(
            textwrap.dedent(
                f"""
                [package]
                name = "dependency"
                version = "{version}"
                edition = "2024"
                readme = "../../docs/dependency.md"
                publish = {publish}

                [package.metadata.hubuum]
                rust-api = "{status}"
                {policy}
                {train}
                """
            ),
            encoding="utf-8",
        )

    def add_member_dependency(self, version: str) -> None:
        with (self.root / "crates" / "member" / "Cargo.toml").open(
            "a", encoding="utf-8"
        ) as manifest:
            manifest.write(
                "\n[dependencies]\n"
                f'dependency = {{ path = "../dependency", version = "{version}" }}\n'
            )

    def test_internal_workspace_is_accepted(self) -> None:
        self.write_workspace()

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("passed for 2 package(s)", result.stdout)

    def test_internal_package_must_disable_publishing(self) -> None:
        self.write_workspace(member_publish="true")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must disable publishing", result.stderr)

    def test_implicit_cargo_workspace_member_is_validated(self) -> None:
        self.write_workspace()
        implicit = self.root / "crates" / "implicit"
        (implicit / "src").mkdir(parents=True)
        (implicit / "src" / "lib.rs").write_text("", encoding="utf-8")
        (implicit / "Cargo.toml").write_text(
            textwrap.dedent(
                """
                [package]
                name = "implicit"
                version = "0.0.1"
                edition = "2024"
                publish = false
                """
            ),
            encoding="utf-8",
        )
        with (self.root / "Cargo.toml").open("a", encoding="utf-8") as manifest:
            manifest.write(
                '\n[dependencies]\nimplicit = { path = "crates/implicit" }\n'
            )

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("implicit must set package.metadata.hubuum.rust-api", result.stderr)

    def test_public_package_is_selected(self) -> None:
        self.write_public_member()

        result = self.run_checker("--supported-packages")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "member\n")

    def test_public_package_cannot_disable_publishing(self) -> None:
        self.write_public_member(status="experimental-public", publish="false")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must enable publishing", result.stderr)

    def test_public_package_cannot_use_an_empty_publish_allowlist(self) -> None:
        self.write_public_member(status="experimental-public", publish="[]")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must enable publishing", result.stderr)

    def test_public_package_accepts_an_allowlist_containing_crates_io(self) -> None:
        self.write_public_member(
            status="experimental-public",
            publish='["company-registry", "crates-io"]',
        )

        result = self.run_checker("--supported-packages")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "member\n")

    def test_public_package_rejects_an_allowlist_without_crates_io(self) -> None:
        self.write_public_member(
            status="experimental-public",
            publish='["company-registry"]',
        )

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must allow crates.io publishing", result.stderr)

    def test_public_package_cannot_depend_on_an_internal_workspace_package(self) -> None:
        self.write_public_member()
        self.write_dependency_package(status="workspace-internal", publish="false")
        self.add_member_dependency("=0.1.0")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must not depend on internal workspace package", result.stderr)

    def test_release_train_requires_exact_in_graph_versions(self) -> None:
        self.write_public_member(version="0.1.0", release_train="storage-sdk")
        self.write_dependency_package(
            status="experimental-public",
            publish="true",
            release_train="storage-sdk",
        )
        self.add_member_dependency("0.1.0")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must use exact requirement =0.1.0", result.stderr)

    def test_release_train_packages_share_one_version(self) -> None:
        self.write_public_member(version="0.1.0", release_train="storage-sdk")
        self.write_dependency_package(
            status="experimental-public",
            publish="true",
            version="0.2.0",
            release_train="storage-sdk",
        )
        self.add_member_dependency("=0.2.0")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("packages must share one version", result.stderr)

    def test_release_train_accepts_one_version_and_exact_requirements(self) -> None:
        self.write_public_member(version="0.1.0", release_train="storage-sdk")
        self.write_dependency_package(
            status="experimental-public",
            publish="true",
            release_train="storage-sdk",
        )
        self.add_member_dependency("=0.1.0")

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_public_package_policy_document_must_exist(self) -> None:
        self.write_public_member(policy_path="docs/missing.md", create_policy=False)

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("policy-document does not exist as a file", result.stderr)

    def test_public_package_readme_must_be_its_policy_document(self) -> None:
        self.write_public_member()
        manifest = self.root / "crates" / "member" / "Cargo.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'readme = "../../docs/member.md"',
                'readme = "README.md"',
            ),
            encoding="utf-8",
        )
        (self.root / "crates" / "member" / "README.md").write_text(
            "# Other readme\n", encoding="utf-8"
        )

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("readme must be its declared policy document", result.stderr)

    def test_declared_policy_documents_include_a_missing_file(self) -> None:
        self.write_public_member(policy_path="docs/missing.md", create_policy=False)

        result = self.run_checker("--declared-policy-documents")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "docs/missing.md\n")

    def test_public_package_policy_document_must_be_a_file(self) -> None:
        self.write_public_member(policy_path="docs/member", create_policy=False)
        (self.root / "docs" / "member").mkdir(parents=True)

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("policy-document does not exist as a file", result.stderr)

    def test_public_package_policy_document_must_stay_in_repository(self) -> None:
        outside_path = self.root.parent / f"{self.root.name}-outside.md"
        self.write_public_member(
            policy_path=f"../{outside_path.name}",
            create_policy=False,
        )
        outside_path.write_text("policy\n", encoding="utf-8")
        self.addCleanup(outside_path.unlink, missing_ok=True)

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must remain inside the repository", result.stderr)

    def test_unknown_classification_is_rejected(self) -> None:
        self.write_workspace(member_status="accidentally-public")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must set package.metadata.hubuum.rust-api", result.stderr)

    def test_root_must_remain_internal_application(self) -> None:
        self.write_workspace(root_status="workspace-internal")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("root hubuum package must remain", result.stderr)


if __name__ == "__main__":
    unittest.main()
